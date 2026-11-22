package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GuardedFileIOTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void rejectsForeignSchemeAuthorityBeforeTouch() {
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.set("fs.defaultFS", "hdfs://namenode:8020");

    FileIO failOnTouch = failOnDelegateCall();
    GuardedFileIO io = new GuardedFileIO(
        failOnTouch,
        new RestFileIoPathGuard(hdfsConf, List.of()),
        new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of()),
        1024L);

    for (String bad : new String[] {
        "hdfs://evil:8020/warehouse/db/t/metadata/v1.metadata.json",
        "webhdfs://evil:50070/etc/passwd",
        "file:///etc/passwd",
        "hdfs://user:secret@namenode:8020/warehouse/t",
        "//evil/share/file",
        "hdfs://namenode:8020/warehouse/db/../etc/passwd",
        "hdfs://namenode:8020/warehouse/db/%2e%2e/etc/passwd",
        "hdfs://namenode:8020/warehouse/db/file\0.json"
    }) {
      try {
        io.newInputFile(bad);
        Assert.fail("Expected ForbiddenException for " + bad);
      } catch (ForbiddenException expected) {
        // Expected before reaching delegate FileIO or Hadoop FileSystem.
      }
      try {
        io.deleteFile(bad);
        Assert.fail("Expected ForbiddenException on deleteFile for " + bad);
      } catch (ForbiddenException expected) {
        // Expected before reaching delegate FileIO or Hadoop FileSystem.
      }
    }
  }

  @Test
  public void allowsConfiguredFileIoAllowedPrefixes() {
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.set("fs.defaultFS", "hdfs://namenode:8020");
    RestFileIoPathGuard guard = new RestFileIoPathGuard(
        hdfsConf, List.of("s3a://approved-bucket/warehouse"));
    java.util.List<String> delegated = new java.util.ArrayList<>();
    FileIO recordingDelegate = new FileIO() {
      @Override
      public InputFile newInputFile(String path) {
        delegated.add(path);
        return org.apache.iceberg.Files.localInput("/nonexistent");
      }

      @Override
      public OutputFile newOutputFile(String path) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void deleteFile(String path) {
        throw new UnsupportedOperationException();
      }
    };
    GuardedFileIO io = new GuardedFileIO(
        recordingDelegate,
        guard,
        new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of()),
        1024L);

    String approvedPath = "s3a://approved-bucket/warehouse/db/tbl/v1.metadata.json";
    Assert.assertNotNull(io.newInputFile(approvedPath));
    Assert.assertEquals(List.of(approvedPath), delegated);
    try {
      io.newInputFile("s3a://other-bucket/warehouse/db/tbl/v1.metadata.json");
      Assert.fail("Expected ForbiddenException for unapproved S3 bucket");
    } catch (ForbiddenException expected) {
      Assert.assertEquals(List.of(approvedPath), delegated);
    }
  }

  @Test
  public void refuseModeBlocksArbitraryDeletesButAllowsRollbackOfCurrentRequestOutput()
      throws Exception {
    File root = tempFolder.getRoot();
    File existing = tempFolder.newFile("existing.metadata.json");
    Files.write(existing.toPath(), new byte[] {1, 2, 3});
    File tempMeta = new File(root, "v2.metadata.json");

    Configuration conf = new Configuration(false);
    GuardedFileIO io = new GuardedFileIO(
        new HadoopFileIO(conf),
        new RestFileIoPathGuard(conf, List.of()),
        new IcebergPurgePolicy(RestCatalogPurgeMode.REFUSE, List.of()),
        1024L);

    try {
      io.deleteFile("file:" + existing.getAbsolutePath());
      Assert.fail("Expected ForbiddenException under REFUSE");
    } catch (ForbiddenException expected) {
      // Expected.
    }
    Assert.assertTrue(existing.exists());

    GuardedFileIO.beginRequest();
    try {
      OutputFile output = io.newOutputFile("file:" + tempMeta.getAbsolutePath());
      try (var stream = output.createOrOverwrite()) {
        stream.write(new byte[] {4, 5});
      }
      Assert.assertTrue(tempMeta.exists());
      // Same-request rollback cleanup must succeed even under REFUSE.
      io.deleteFile(output);
      Assert.assertFalse("same-request rollback file should be deleted", tempMeta.exists());
      // Second delete on the same path must fail because it was already consumed.
      try {
        io.deleteFile("file:" + tempMeta.getAbsolutePath());
        Assert.fail("Expected second delete under REFUSE to fail");
      } catch (ForbiddenException expected) {
        // Expected.
      }
    } finally {
      GuardedFileIO.endRequest();
    }
  }

  @Test
  public void allowlistBlocksRootAndSymlinkEscapes() throws Exception {
    File allowRoot = tempFolder.newFolder("allowed");
    File outsideDir = tempFolder.newFolder("outside");
    File outsideFile = new File(outsideDir, "secret.txt");
    Files.writeString(outsideFile.toPath(), "secret");
    Path escapingLink = allowRoot.toPath().resolve("escape.txt");
    Files.createSymbolicLink(escapingLink, outsideFile.toPath());
    Path escapingDirLink = allowRoot.toPath().resolve("linked_dir");
    Files.createSymbolicLink(escapingDirLink, outsideDir.toPath());
    File throughDirLink = new File(escapingDirLink.toFile(), "secret.txt");

    Configuration conf = new Configuration(false);
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:" + allowRoot.getAbsolutePath()));
    GuardedFileIO io = new GuardedFileIO(
        new HadoopFileIO(conf),
        new RestFileIoPathGuard(conf, List.of()),
        policy,
        1024L);

    try {
      io.deleteFile("file:" + allowRoot.getAbsolutePath());
      Assert.fail("Deleting allowlist root itself must be refused");
    } catch (ForbiddenException expected) {
      // Expected.
    }
    try {
      io.deleteFile("file:" + escapingLink.toAbsolutePath());
      Assert.fail("Deleting a direct symlink must be refused");
    } catch (ForbiddenException expected) {
      // Expected.
    }
    try {
      io.deleteFile("file:" + throughDirLink.getAbsolutePath());
      Assert.fail("Deleting through an escaping directory symlink must be refused");
    } catch (ForbiddenException expected) {
      // Expected.
    }
    Assert.assertTrue(outsideFile.exists());
  }

  @Test
  public void boundedInputFileRejectsOversizedMetadataFile() throws Exception {
    File bigFile = tempFolder.newFile("big.metadata.json");
    Files.write(bigFile.toPath(), new byte[128]);

    Configuration conf = new Configuration(false);
    GuardedFileIO io = new GuardedFileIO(
        new HadoopFileIO(conf),
        new RestFileIoPathGuard(conf, List.of()),
        new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of()),
        64L);

    InputFile bounded = io.newInputFile("file:" + bigFile.getAbsolutePath());
    try (SeekableInputStream ignored = bounded.newStream()) {
      Assert.fail("Expected ValidationException when file length exceeds maxMetadataFileBytes");
    } catch (ValidationException expected) {
      Assert.assertTrue(expected.getMessage().contains("max-metadata-file-bytes"));
    }

    try {
      io.newInputFile("file:" + bigFile.getAbsolutePath(), 128L);
      Assert.fail("Expected ValidationException when declared length exceeds limit");
    } catch (ValidationException expected) {
      Assert.assertTrue(expected.getMessage().contains("max-metadata-file-bytes"));
    }
  }

  @Test
  public void allowsFederatedReadFsUrisForReadsOnlyWhileBlockingWritesAndDeletes() {
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.set("fs.defaultFS", "hdfs://namenode-a:8020");
    RestFileIoPathGuard guard = new RestFileIoPathGuard(
        hdfsConf,
        List.of(),
        List.of(URI.create("hdfs://namenode-b:8020")));

    java.util.List<String> delegated = new java.util.ArrayList<>();
    FileIO recordingDelegate = new FileIO() {
      @Override
      public InputFile newInputFile(String path) {
        delegated.add(path);
        return org.apache.iceberg.Files.localInput("/nonexistent");
      }

      @Override
      public OutputFile newOutputFile(String path) {
        throw new AssertionError("Should not reach delegate OutputFile for: " + path);
      }

      @Override
      public void deleteFile(String path) {
        throw new AssertionError("Should not reach delegate deleteFile for: " + path);
      }
    };
    GuardedFileIO io = new GuardedFileIO(
        recordingDelegate,
        guard,
        new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of()),
        1024L);

    String federatedPath = "hdfs://namenode-b:8020/warehouse/db/t/metadata/v1.metadata.json";
    Assert.assertNotNull(io.newInputFile(federatedPath));
    Assert.assertNotNull(io.newInputFile(federatedPath, 128L));
    Assert.assertEquals(List.of(federatedPath, federatedPath), delegated);

    try {
      io.newOutputFile(federatedPath);
      Assert.fail("Expected ForbiddenException on write to federated FS");
    } catch (ForbiddenException expected) {
      // Expected: federated read trust does not grant write access.
    }
    try {
      io.deleteFile(federatedPath);
      Assert.fail("Expected ForbiddenException on delete from federated FS");
    } catch (ForbiddenException expected) {
      // Expected: federated read trust does not grant delete access.
    }
    for (String unapproved : new String[] {
        "file:///etc/passwd",
        "hdfs://evil:8020/warehouse/db/t/metadata/v1.metadata.json"
    }) {
      try {
        io.newInputFile(unapproved);
        Assert.fail("Expected ForbiddenException for " + unapproved);
      } catch (ForbiddenException expected) {
        // Expected.
      }
    }
  }

  @Test
  public void executesFileOperationsThroughConfiguredPrivilegedRunner() {
    Configuration hdfsConf = new Configuration(false);
    hdfsConf.set("fs.defaultFS", "file:///");
    RestFileIoPathGuard guard = new RestFileIoPathGuard(hdfsConf, List.of("file:///tmp/"), List.of());

    java.util.concurrent.atomic.AtomicInteger runnerInvocations =
        new java.util.concurrent.atomic.AtomicInteger();
    GuardedFileIO.PrivilegedRunner countingRunner = new GuardedFileIO.PrivilegedRunner() {
      @Override
      public <T> T run(java.util.function.Supplier<T> action) {
        runnerInvocations.incrementAndGet();
        return action.get();
      }
    };

    FileIO rawIo = new FileIO() {
      @Override
      public InputFile newInputFile(String path) {
        return org.apache.iceberg.Files.localInput("/tmp/example-nonexistent.json");
      }

      @Override
      public OutputFile newOutputFile(String path) {
        return org.apache.iceberg.Files.localOutput("/tmp/example-out.json");
      }

      @Override
      public void deleteFile(String path) {
      }
    };

    GuardedFileIO io = new GuardedFileIO(
        rawIo,
        guard,
        new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of()),
        0L,
        countingRunner);

    InputFile in = io.newInputFile("file:///tmp/example-nonexistent.json");
    in.exists();
    OutputFile out = io.newOutputFile("file:///tmp/example-out.json");
    out.toInputFile().exists();
    io.deleteFile("file:///tmp/example-out.json");

    Assert.assertEquals(6, runnerInvocations.get());
  }

  private static FileIO failOnDelegateCall() {
    return new FileIO() {
      @Override
      public InputFile newInputFile(String path) {
        throw new AssertionError("Delegate FileIO must not be reached for: " + path);
      }

      @Override
      public OutputFile newOutputFile(String path) {
        throw new AssertionError("Delegate FileIO must not be reached for: " + path);
      }

      @Override
      public void deleteFile(String path) {
        throw new AssertionError("Delegate FileIO must not be reached for: " + path);
      }
    };
  }
}
