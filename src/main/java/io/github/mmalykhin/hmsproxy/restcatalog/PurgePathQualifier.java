package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Turns a configured prefix or a path out of a manifest into the one qualified form both sides of
 * an allowlist comparison must be in: a bare path, "file:/tmp/x" and "file:///tmp/x" all have to
 * compare equal, or the allowlist would refuse paths it means to permit.
 */
final class PurgePathQualifier {
  private PurgePathQualifier() {
  }

  static String qualify(String location, Configuration conf) throws IOException {
    Path path = new Path(location);
    FileSystem fileSystem = path.getFileSystem(conf);
    return path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory()).toString();
  }

  /** Qualifies every prefix, keeping one that cannot be qualified as configured. */
  static List<String> qualifyPrefixes(List<String> prefixes, Configuration conf) {
    List<String> qualified = new ArrayList<>(prefixes.size());
    for (String prefix : prefixes) {
      try {
        qualified.add(qualify(prefix, conf));
      } catch (IOException | IllegalArgumentException e) {
        // A prefix naming an unreachable filesystem then simply matches nothing, which fails
        // closed. Refusing to serve at all would take reads down with it.
        qualified.add(prefix);
      }
    }
    return List.copyOf(qualified);
  }
}
