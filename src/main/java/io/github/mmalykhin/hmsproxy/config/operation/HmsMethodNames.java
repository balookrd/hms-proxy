package io.github.mmalykhin.hmsproxy.config.operation;

/**
 * Canonical spelling of HMS Thrift method names. Operation classification and rate-limit RPC-class
 * classification must agree on it: a divergence would silently move methods between the write/ddl
 * buckets, so both go through this single implementation.
 */
public final class HmsMethodNames {
  private HmsMethodNames() {
  }

  public static String normalize(String methodName) {
    return methodName == null ? "" : methodName.trim();
  }

  /** Lower-cases the method name and splits camelCase spellings into {@code snake_case}. */
  public static String canonicalize(String methodName) {
    String normalized = normalize(methodName);
    StringBuilder builder = new StringBuilder(normalized.length() + 8);
    for (int i = 0; i < normalized.length(); i++) {
      char current = normalized.charAt(i);
      if (Character.isUpperCase(current) && i > 0 && builder.charAt(builder.length() - 1) != '_') {
        builder.append('_');
      }
      builder.append(Character.toLowerCase(current));
    }
    return builder.toString();
  }
}
