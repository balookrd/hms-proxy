package io.github.mmalykhin.hmsproxy.util;

/**
 * Single strict JSON string escaper shared by audit records and management endpoint payloads.
 * Client-controlled values (Kerberos principals, remote addresses, backend error messages) reach
 * both paths, so every character below 0x20 must be escaped to keep the output machine-parsable.
 */
public final class JsonEscapeUtil {
  private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

  private JsonEscapeUtil() {
  }

  public static String escape(String value) {
    int firstSpecial = indexOfFirstSpecial(value);
    if (firstSpecial < 0) {
      return value;
    }
    StringBuilder builder = new StringBuilder(value.length() + 16);
    builder.append(value, 0, firstSpecial);
    appendEscaped(builder, value, firstSpecial);
    return builder.toString();
  }

  public static void appendEscaped(StringBuilder builder, String value) {
    appendEscaped(builder, value, 0);
  }

  private static void appendEscaped(StringBuilder builder, String value, int fromIndex) {
    for (int index = fromIndex; index < value.length(); index++) {
      char current = value.charAt(index);
      switch (current) {
        case '"' -> builder.append("\\\"");
        case '\\' -> builder.append("\\\\");
        case '\n' -> builder.append("\\n");
        case '\r' -> builder.append("\\r");
        case '\t' -> builder.append("\\t");
        case '\b' -> builder.append("\\b");
        case '\f' -> builder.append("\\f");
        default -> {
          if (current < 0x20) {
            builder.append("\\u00")
                .append(HEX_DIGITS[(current >> 4) & 0xf])
                .append(HEX_DIGITS[current & 0xf]);
          } else {
            builder.append(current);
          }
        }
      }
    }
  }

  private static int indexOfFirstSpecial(String value) {
    for (int index = 0; index < value.length(); index++) {
      char current = value.charAt(index);
      if (current == '"' || current == '\\' || current < 0x20) {
        return index;
      }
    }
    return -1;
  }
}
