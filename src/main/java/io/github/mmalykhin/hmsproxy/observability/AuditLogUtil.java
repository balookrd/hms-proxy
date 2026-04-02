package io.github.mmalykhin.hmsproxy.observability;

import java.util.LinkedHashMap;
import java.util.Map;

public final class AuditLogUtil {
  private AuditLogUtil() {
  }

  public static String toJson(Map<String, ?> fields) {
    StringBuilder builder = new StringBuilder(256);
    builder.append('{');
    boolean first = true;
    for (Map.Entry<String, ?> entry : fields.entrySet()) {
      if (!first) {
        builder.append(',');
      }
      first = false;
      builder.append('"').append(escape(entry.getKey())).append('"').append(':');
      appendValue(builder, entry.getValue());
    }
    builder.append('}');
    return builder.toString();
  }

  public static Map<String, Object> orderedFields() {
    return new LinkedHashMap<>();
  }

  private static void appendValue(StringBuilder builder, Object value) {
    if (value == null) {
      builder.append("null");
      return;
    }
    if (value instanceof Number || value instanceof Boolean) {
      builder.append(value);
      return;
    }
    builder.append('"').append(escape(String.valueOf(value))).append('"');
  }

  private static String escape(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t");
  }
}
