package io.github.mmalykhin.hmsproxy.util;

import java.util.Arrays;
import java.util.List;

/**
 * Comma-separated path-prefix allowlist shared by the two purge paths (the Thrift listener's
 * external-table purge and the Iceberg REST purge), so a prefix means the same thing in both.
 * Matching is on a path-separator boundary, never a bare string prefix: "hdfs://ns/db" must not
 * cover "hdfs://ns/dbx". Locations are expected already qualified by the caller - this class
 * never touches a FileSystem.
 */
public final class PathPrefixAllowlist {
  private PathPrefixAllowlist() {
  }

  public static List<String> parse(String commaSeparatedOrNull) {
    if (commaSeparatedOrNull == null || commaSeparatedOrNull.isBlank()) {
      return List.of();
    }
    return Arrays.stream(commaSeparatedOrNull.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .toList();
  }

  public static boolean matches(String location, List<String> prefixes) {
    if (location == null) {
      return false;
    }
    for (String prefix : prefixes) {
      String normalizedPrefix = prefix.trim();
      if (normalizedPrefix.isEmpty()) {
        continue;
      }
      if (location.equals(normalizedPrefix)) {
        return true;
      }
      String boundaryPrefix = normalizedPrefix.endsWith("/") ? normalizedPrefix : normalizedPrefix + "/";
      if (location.startsWith(boundaryPrefix)) {
        return true;
      }
    }
    return false;
  }
}
