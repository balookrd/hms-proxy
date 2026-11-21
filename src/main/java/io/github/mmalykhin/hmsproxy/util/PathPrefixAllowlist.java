package io.github.mmalykhin.hmsproxy.util;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

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

  public static boolean containsUnsafePathSyntax(String location) {
    if (location == null || location.isBlank()) {
      return true;
    }
    if (location.indexOf('\\') >= 0
        || location.indexOf('\0') >= 0
        || location.indexOf('\r') >= 0
        || location.indexOf('\n') >= 0) {
      return true;
    }
    if (hasDotOrEncodedTraversalSegment(extractPathPart(location))) {
      return true;
    }
    try {
      String uriPath = URI.create(location).getPath();
      if (uriPath != null && hasDotOrEncodedTraversalSegment(uriPath)) {
        return true;
      }
    } catch (IllegalArgumentException ignored) {
      // Non-URI syntax is already inspected via extractPathPart(location) and rejected on match.
    }
    return false;
  }

  public static boolean matches(String location, List<String> prefixes) {
    if (containsUnsafePathSyntax(location) || prefixes == null || prefixes.isEmpty()) {
      return false;
    }
    for (String prefix : prefixes) {
      String normalizedPrefix = prefix == null ? "" : prefix.trim();
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

  /**
   * Matches only strict descendants under an allowlisted prefix (never the allowlist root itself)
   * and rejects relative traversal segments such as {@code ..}, {@code .}, backslashes, or
   * percent-encoded slashes/dots.
   */
  public static boolean matchesStrictDescendant(String location, List<String> prefixes) {
    if (containsUnsafePathSyntax(location) || prefixes == null || prefixes.isEmpty()) {
      return false;
    }
    String normalizedLocation = canonicalizeForComparison(location);
    for (String prefix : prefixes) {
      if (prefix == null) {
        continue;
      }
      String normalizedPrefix = canonicalizeForComparison(prefix.trim());
      if (normalizedPrefix.isEmpty()) {
        continue;
      }
      String boundaryPrefix = normalizedPrefix.endsWith("/") ? normalizedPrefix : normalizedPrefix + "/";
      if (normalizedLocation.startsWith(boundaryPrefix)
          && normalizedLocation.length() > boundaryPrefix.length()) {
        return true;
      }
    }
    return false;
  }

  private static String canonicalizeForComparison(String value) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    String result = value;
    if (result.startsWith("file://")) {
      int rest = "file://".length();
      while (rest + 1 < result.length() && result.charAt(rest) == '/' && result.charAt(rest + 1) == '/') {
        rest++;
      }
      if (rest < result.length() && result.charAt(rest) == '/') {
        result = "file:" + result.substring(rest);
      } else {
        result = "file:/" + result.substring(rest);
      }
    }
    return trimTrailingSlashes(result);
  }

  private static String extractPathPart(String location) {
    int schemeSep = location.indexOf("://");
    if (schemeSep >= 0) {
      int pathStart = location.indexOf('/', schemeSep + 3);
      return pathStart >= 0 ? location.substring(pathStart) : "";
    }
    int colon = location.indexOf(':');
    if (colon >= 0) {
      return location.substring(colon + 1);
    }
    return location;
  }

  private static boolean hasDotOrEncodedTraversalSegment(String path) {
    if (path == null || path.isEmpty()) {
      return false;
    }
    String[] segments = path.split("/");
    for (String segment : segments) {
      if (segment.isEmpty()) {
        continue;
      }
      if (".".equals(segment) || "..".equals(segment)) {
        return true;
      }
      String lower = segment.toLowerCase(Locale.ROOT);
      if (lower.contains("%2e") || lower.contains("%2f") || lower.contains("%5c")) {
        return true;
      }
    }
    return false;
  }

  private static String trimTrailingSlashes(String value) {
    int end = value.length();
    while (end > 1 && value.charAt(end - 1) == '/') {
      if (end >= 2 && value.charAt(end - 2) == ':') {
        break;
      }
      if (end >= 3 && value.charAt(end - 2) == '/' && value.charAt(end - 3) == ':') {
        break;
      }
      end--;
    }
    return end == value.length() ? value : value.substring(0, end);
  }
}
