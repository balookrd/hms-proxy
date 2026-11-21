package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO decorator that refuses to delete a path outside the configured allowlist during
 * {@code dropTable(..., purge=true)}.
 *
 * <p>A refused path is skipped and logged, never thrown on: by the time deletion runs the table
 * is already dropped in the metastore, so failing here would leave a half-purged table and a 500.
 */
final class PrefixGuardedFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixGuardedFileIO.class);
  static final int MAX_RECORDED_SKIPPED_PATHS = 64;

  private final FileIO delegate;
  private final List<String> qualifiedPrefixes;
  private final transient RestFileIoPathGuard pathGuard;
  private final String tableName;
  private final AtomicInteger skippedCount = new AtomicInteger();
  private final List<String> skippedPaths = Collections.synchronizedList(new ArrayList<>());

  PrefixGuardedFileIO(
      FileIO delegate, List<String> qualifiedPrefixes, Configuration conf, String tableName) {
    this(
        delegate,
        qualifiedPrefixes,
        new RestFileIoPathGuard(Objects.requireNonNull(conf, "conf"), List.of()),
        tableName);
  }

  PrefixGuardedFileIO(
      FileIO delegate,
      List<String> qualifiedPrefixes,
      RestFileIoPathGuard pathGuard,
      String tableName) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.qualifiedPrefixes = List.copyOf(qualifiedPrefixes);
    this.pathGuard = Objects.requireNonNull(pathGuard, "pathGuard");
    this.tableName = Objects.requireNonNull(tableName, "tableName");
  }

  List<String> skippedPaths() {
    synchronized (skippedPaths) {
      return List.copyOf(skippedPaths);
    }
  }

  int skippedCount() {
    return skippedCount.get();
  }

  @Override
  public InputFile newInputFile(String path) {
    pathGuard.validateBeforeFsAccess(path);
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    pathGuard.validateBeforeFsAccess(path);
    return delegate.newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    pathGuard.validateBeforeFsAccess(path);
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    if (!allowed(path)) {
      return;
    }
    delegate.deleteFile(path);
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

  private boolean allowed(String path) {
    String qualified;
    try {
      qualified = pathGuard.qualifySafely(path);
    } catch (RuntimeException e) {
      skip(path, "its location could not be validated or qualified: " + e.getMessage());
      return false;
    }
    if (!PathPrefixAllowlist.matchesStrictDescendant(qualified, qualifiedPrefixes)) {
      skip(qualified, "it is not a strict descendant under rest-catalog.purge.allowed-prefixes");
      return false;
    }
    try {
      pathGuard.verifyNoSymlinkEscape(qualified, qualifiedPrefixes);
      return true;
    } catch (RuntimeException e) {
      skip(qualified, "symbolic-link containment check failed: " + e.getMessage());
      return false;
    }
  }

  private void skip(String path, String reason) {
    int count = skippedCount.incrementAndGet();
    synchronized (skippedPaths) {
      if (skippedPaths.size() < MAX_RECORDED_SKIPPED_PATHS) {
        skippedPaths.add(path);
      }
    }
    if (count <= MAX_RECORDED_SKIPPED_PATHS) {
      LOG.warn("purge of table '{}' skipped deleting '{}' because {}", tableName, path, reason);
    } else if (count == MAX_RECORDED_SKIPPED_PATHS + 1) {
      LOG.warn("purge of table '{}' suppressing further skipped-path logs after {} entries",
          tableName, MAX_RECORDED_SKIPPED_PATHS);
    }
  }
}
