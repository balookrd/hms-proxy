package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unified {@link FileIO} installed into {@link RoutingHiveCatalog} so every Iceberg catalog path
 * ({@code loadTable}, {@code registerTable}, {@code updateTable} with
 * {@code write.metadata.delete-after-commit.enabled}, {@code dropTable}, {@code dropView}, and
 * {@code commitTransaction}) shares one pre-filesystem URI guard, bounded read cap, symlink check,
 * and runtime deletion policy.
 *
 * <p>Metadata/manifest files written during the current REST request are recorded in a per-request
 * {@link ThreadLocal} set so rollback cleanup after an optimistic concurrency conflict (e.g.
 * {@code 409 CommitFailedException}) succeeds even when {@code rest-catalog.purge.mode=REFUSE}.
 */
final class GuardedFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(GuardedFileIO.class);
  private static final int MAX_TRACKED_CREATED_PATHS = 64;
  private static final ThreadLocal<Set<String>> CREATED_IN_REQUEST = new ThreadLocal<>();

  @FunctionalInterface
  interface PrivilegedRunner {
    <T> T run(java.util.function.Supplier<T> action);
  }

  static final PrivilegedRunner DIRECT_RUNNER = java.util.function.Supplier::get;

  private final FileIO delegate;
  private final RestFileIoPathGuard guard;
  private final IcebergPurgePolicy purgePolicy;
  private final long maxMetadataFileBytes;
  private final PrivilegedRunner privilegedRunner;

  GuardedFileIO(
      FileIO delegate,
      RestFileIoPathGuard guard,
      IcebergPurgePolicy purgePolicy,
      long maxMetadataFileBytes
  ) {
    this(delegate, guard, purgePolicy, maxMetadataFileBytes, DIRECT_RUNNER);
  }

  GuardedFileIO(
      FileIO delegate,
      RestFileIoPathGuard guard,
      IcebergPurgePolicy purgePolicy,
      long maxMetadataFileBytes,
      PrivilegedRunner privilegedRunner
  ) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.guard = Objects.requireNonNull(guard, "guard");
    this.purgePolicy = Objects.requireNonNull(purgePolicy, "purgePolicy");
    this.maxMetadataFileBytes = maxMetadataFileBytes;
    this.privilegedRunner = privilegedRunner == null ? DIRECT_RUNNER : privilegedRunner;
  }

  static void beginRequest() {
    CREATED_IN_REQUEST.set(new HashSet<>());
  }

  static void endRequest() {
    CREATED_IN_REQUEST.remove();
  }

  FileIO rawDelegate() {
    return delegate;
  }

  @Override
  public InputFile newInputFile(String path) {
    return privilegedRunner.run(() -> {
      guard.validateBeforeReadAccess(path);
      return wrapInputFile(delegate.newInputFile(path));
    });
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return privilegedRunner.run(() -> {
      guard.validateBeforeReadAccess(path);
      if (maxMetadataFileBytes > 0L && length > maxMetadataFileBytes) {
        throw new ValidationException(
            "Metadata file length exceeds rest-catalog.max-metadata-file-bytes (%s)",
            maxMetadataFileBytes);
      }
      return wrapInputFile(delegate.newInputFile(path, length));
    });
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return privilegedRunner.run(() -> {
      String qualified = guard.qualifySafely(path);
      guard.verifyNoSymlinkEscape(qualified, null);
      trackCreated(qualified);
      return new GuardedOutputFile(delegate.newOutputFile(path));
    });
  }

  @Override
  public void deleteFile(String path) {
    privilegedRunner.run(() -> {
      String qualified = guard.qualifySafely(path);
      if (consumeCreated(qualified)) {
        guard.verifyNoSymlinkEscape(qualified, null);
        delegate.deleteFile(path);
        return null;
      }
      String refusal = purgePolicy.authorizeRuntimeDelete(qualified, guard);
      if (refusal != null) {
        LOG.warn("Refused FileIO deletion of '{}': {}", qualified, refusal);
        throw new ForbiddenException("%s", refusal);
      }
      delegate.deleteFile(path);
      return null;
    });
  }

  @Override
  public void deleteFile(InputFile file) {
    deleteFile(file.location());
  }

  @Override
  public void deleteFile(OutputFile file) {
    deleteFile(file.location());
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private InputFile wrapInputFile(InputFile raw) {
    return (maxMetadataFileBytes > 0L || privilegedRunner != DIRECT_RUNNER)
        ? new GuardedInputFile(raw, maxMetadataFileBytes, privilegedRunner)
        : raw;
  }

  private static void trackCreated(String qualifiedPath) {
    Set<String> created = CREATED_IN_REQUEST.get();
    if (created != null && created.size() < MAX_TRACKED_CREATED_PATHS) {
      created.add(qualifiedPath);
    }
  }

  private static boolean consumeCreated(String qualifiedPath) {
    Set<String> created = CREATED_IN_REQUEST.get();
    return created != null && created.remove(qualifiedPath);
  }

  private final class GuardedOutputFile implements OutputFile {
    private final OutputFile raw;

    GuardedOutputFile(OutputFile raw) {
      this.raw = raw;
    }

    @Override
    public PositionOutputStream create() {
      return privilegedRunner.run(raw::create);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return privilegedRunner.run(raw::createOrOverwrite);
    }

    @Override
    public String location() {
      return raw.location();
    }

    @Override
    public InputFile toInputFile() {
      return privilegedRunner.run(() -> wrapInputFile(raw.toInputFile()));
    }
  }

  private static final class GuardedInputFile implements InputFile {
    private final InputFile raw;
    private final long maxBytes;
    private final PrivilegedRunner privilegedRunner;

    GuardedInputFile(InputFile raw, long maxBytes, PrivilegedRunner privilegedRunner) {
      this.raw = raw;
      this.maxBytes = maxBytes;
      this.privilegedRunner = privilegedRunner;
    }

    @Override
    public long getLength() {
      return privilegedRunner.run(raw::getLength);
    }

    @Override
    public SeekableInputStream newStream() {
      return privilegedRunner.run(() -> {
        if (maxBytes > 0L) {
          long len = raw.getLength();
          if (len > maxBytes) {
            throw new ValidationException(
                "File size exceeds rest-catalog.max-metadata-file-bytes (%s)", maxBytes);
          }
        }
        SeekableInputStream stream = raw.newStream();
        return maxBytes > 0L ? new BoundedSeekableInputStream(stream, maxBytes) : stream;
      });
    }

    @Override
    public String location() {
      return raw.location();
    }

    @Override
    public boolean exists() {
      return privilegedRunner.run(raw::exists);
    }
  }

  private static final class BoundedSeekableInputStream extends SeekableInputStream {
    private final SeekableInputStream delegate;
    private final long maxPosBytes;
    private final long maxTotalReadBytes;
    private long totalBytesRead;

    BoundedSeekableInputStream(SeekableInputStream delegate, long maxBytes) {
      this.delegate = delegate;
      this.maxPosBytes = maxBytes;
      this.maxTotalReadBytes = maxBytes > (Long.MAX_VALUE / 4L) ? Long.MAX_VALUE : maxBytes * 4L;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos > maxPosBytes) {
        throw new IOException("Seek offset exceeds rest-catalog.max-metadata-file-bytes");
      }
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int b = delegate.read();
      if (b >= 0) {
        recordRead(1);
      }
      return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = delegate.read(b, off, len);
      if (n > 0) {
        recordRead(n);
      }
      return n;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }

    private void recordRead(int n) throws IOException {
      totalBytesRead += n;
      if (totalBytesRead > maxTotalReadBytes || delegate.getPos() > maxPosBytes) {
        throw new IOException("File read exceeds rest-catalog.max-metadata-file-bytes");
      }
    }
  }
}
