package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PrefixGuardedFileIOTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void deletesInsideTheAllowlistAndSkipsOutside() throws Exception {
    File inside = tempFolder.newFolder("inside");
    File outside = tempFolder.newFolder("outside");
    File allowed = new File(inside, "a.parquet");
    File refused = new File(outside, "b.parquet");
    Files.write(allowed.toPath(), new byte[] {1});
    Files.write(refused.toPath(), new byte[] {1});

    PrefixGuardedFileIO io = new PrefixGuardedFileIO(
        new HadoopFileIO(new Configuration()),
        PurgePathQualifier.qualifyPrefixes(
            List.of("file:" + inside.getAbsolutePath()), new Configuration()),
        new Configuration(),
        "default.t");

    io.deleteFile("file:" + allowed.getAbsolutePath());
    io.deleteFile("file:" + refused.getAbsolutePath());

    Assert.assertFalse("a path inside the allowlist must be deleted", allowed.exists());
    Assert.assertTrue("a path outside the allowlist must survive", refused.exists());
    Assert.assertEquals(io.skippedPaths().toString(), 1, io.skippedPaths().size());
    Assert.assertTrue(io.skippedPaths().toString(),
        io.skippedPaths().get(0).contains(refused.getName()));
  }

  @Test
  public void anUnqualifiedPathIsMatchedAfterQualification() throws Exception {
    File inside = tempFolder.newFolder("qualify_me");
    File allowed = new File(inside, "c.parquet");
    Files.write(allowed.toPath(), new byte[] {1});

    PrefixGuardedFileIO io = new PrefixGuardedFileIO(
        new HadoopFileIO(new Configuration()),
        PurgePathQualifier.qualifyPrefixes(
            List.of("file:" + inside.getAbsolutePath()), new Configuration()),
        new Configuration(),
        "default.t");

    // No scheme: a manifest may carry a bare path, and the guard must qualify it the same way it
    // qualified the prefixes rather than refusing it as unmatched.
    io.deleteFile(allowed.getAbsolutePath());

    Assert.assertFalse(allowed.exists());
    Assert.assertEquals(List.of(), io.skippedPaths());
  }

  @Test
  public void readAndWriteMethodsAreNotConstrained() throws Exception {
    File outside = tempFolder.newFolder("write_outside");
    PrefixGuardedFileIO io = new PrefixGuardedFileIO(
        new HadoopFileIO(new Configuration()),
        List.of("file:/nowhere"),
        new Configuration(),
        "default.t");

    // Only deletion is guarded: a purge never writes, but the same FileIO instance must not
    // change how anything else behaves if it is ever reused.
    Assert.assertNotNull(io.newOutputFile("file:" + outside.getAbsolutePath() + "/d.parquet"));
    Assert.assertNotNull(io.newInputFile("file:" + outside.getAbsolutePath() + "/d.parquet"));
  }
}
