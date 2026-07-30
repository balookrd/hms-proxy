package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO decorator that refuses to delete a path outside the configured allowlist. It exists
 * because a purge deletes whatever the table's manifests reference, and in the REST protocol the
 * manifests are written by the client: a commit can point a snapshot at arbitrary paths, and the
 * proxy would otherwise delete them under its own credentials. Checking the table location up
 * front cannot catch that - those paths are only known while walking the manifests.
 *
 * <p>A refused path is skipped and logged, never thrown on: by the time deletion runs the table
 * is already dropped in the metastore, so failing here would leave a half-purged table and a 500.
 *
 * <p>Only the delete methods are constrained; reads and writes delegate unchanged. Deliberately
 * not usable after serialization (it holds a Hadoop {@link Configuration}), which is fine because
 * this FileIO never leaves the proxy's JVM - Iceberg only serializes a FileIO for distributed
 * planning, which this front door does not do.
 */
final class PrefixGuardedFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixGuardedFileIO.class);

  private final FileIO delegate;
  private final List<String> qualifiedPrefixes;
  private final transient Configuration conf;
  private final String tableName;
  private final List<String> skippedPaths = new CopyOnWriteArrayList<>();

  PrefixGuardedFileIO(
      FileIO delegate, List<String> qualifiedPrefixes, Configuration conf, String tableName) {
    this.delegate = delegate;
    this.qualifiedPrefixes = List.copyOf(qualifiedPrefixes);
    this.conf = conf;
    this.tableName = tableName;
  }

  List<String> skippedPaths() {
    return List.copyOf(skippedPaths);
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return delegate.newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
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
      qualified = PurgePathQualifier.qualify(path, conf);
    } catch (IOException | IllegalArgumentException e) {
      // Fail closed: a path whose filesystem cannot even be resolved cannot be shown to be inside
      // the allowlist, and this code deletes data.
      skip(path, "its location could not be qualified: " + e.getMessage());
      return false;
    }
    if (PathPrefixAllowlist.matches(qualified, qualifiedPrefixes)) {
      return true;
    }
    skip(qualified, "it is outside rest-catalog.purge.allowed-prefixes");
    return false;
  }

  private void skip(String path, String reason) {
    skippedPaths.add(path);
    LOG.warn("purge of table '{}' skipped deleting '{}' because {}", tableName, path, reason);
  }
}
