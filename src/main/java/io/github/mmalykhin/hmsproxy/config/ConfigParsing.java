package io.github.mmalykhin.hmsproxy.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

public final class ConfigParsing {
  /** Bind hosts that accept connections on every local address, so they clash with any other host. */
  private static final Set<String> WILDCARD_BIND_HOSTS = Set.of("", "*", "0.0.0.0", "::", "[::]", "::0", "0:0:0:0:0:0:0:0");

  private ConfigParsing() {
  }

  /**
   * Single rule for every enum-valued property: trim, upper-case with {@link Locale#ROOT} so a
   * Turkish locale cannot mangle it, and report the accepted constants on failure.
   */
  public static <E extends Enum<E>> E parseEnum(Class<E> type, String value, String propertyName) {
    String normalized = PropertyReader.trimToNull(value);
    if (normalized == null) {
      throw new IllegalArgumentException(
          "Invalid value for " + propertyName + ": " + value + ". Expected one of: " + acceptedValues(type));
    }
    try {
      return Enum.valueOf(type, normalized.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for " + propertyName + ": " + value + ". Expected one of: " + acceptedValues(type),
          e);
    }
  }

  /** Same as {@link #parseEnum}, but an unset (null or blank) value yields {@code defaultValue}. */
  public static <E extends Enum<E>> E parseEnum(
      Class<E> type,
      String value,
      String propertyName,
      E defaultValue
  ) {
    return PropertyReader.trimToNull(value) == null ? defaultValue : parseEnum(type, value, propertyName);
  }

  public static String acceptedValues(Class<? extends Enum<?>> type) {
    return Arrays.stream(type.getEnumConstants()).map(Enum::name).collect(Collectors.joining(", "));
  }

  /**
   * True when two listeners cannot bind side by side. A wildcard host covers every local address,
   * so it conflicts with any host on the same port. Hosts are compared as configured: no DNS
   * resolution, so startup never depends on a resolver ({@code localhost} vs {@code 127.0.0.1}
   * still fails at bind time).
   */
  public static boolean bindingsConflict(String leftHost, int leftPort, String rightHost, int rightPort) {
    if (leftPort != rightPort) {
      return false;
    }
    String left = normalizeBindHost(leftHost);
    String right = normalizeBindHost(rightHost);
    return isWildcardBindHost(left) || isWildcardBindHost(right) || left.equals(right);
  }

  public static String describeBinding(String bindHost, int port) {
    return bindHost + ":" + port;
  }

  private static boolean isWildcardBindHost(String normalizedHost) {
    return WILDCARD_BIND_HOSTS.contains(normalizedHost);
  }

  private static String normalizeBindHost(String bindHost) {
    String normalized = bindHost == null ? "" : bindHost.trim().toLowerCase(Locale.ROOT);
    if (normalized.startsWith("[") && normalized.endsWith("]") && normalized.length() > 2) {
      normalized = normalized.substring(1, normalized.length() - 1);
    }
    return normalized;
  }

  public static void requireNonBlank(String value, String name) {
    if (PropertyReader.trimToNull(value) == null) {
      throw new IllegalArgumentException("Missing required property: " + name);
    }
  }

  public static void requireReadableFile(String path, String propertyName) {
    if (!Files.isReadable(Path.of(path))) {
      throw new IllegalArgumentException(
          "File not found or not readable for " + propertyName + ": " + path);
    }
  }

  public static void validateRegexList(String propertyName, String[] patterns) {
    for (String pattern : patterns) {
      validateRegex(propertyName, pattern);
    }
  }

  public static void validateRegex(String propertyName, String pattern) {
    try {
      Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid regex for " + propertyName + ": " + pattern + " - " + e.getMessage(),
          e);
    }
  }
}
