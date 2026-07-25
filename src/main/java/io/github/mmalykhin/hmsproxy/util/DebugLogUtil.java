package io.github.mmalykhin.hmsproxy.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public final class DebugLogUtil {
  private static final int MAX_ELEMENTS = 10;
  private static final int MAX_CHARS = 4_000;
  private static final int MAX_DEPTH = 3;
  private static final String TRUNCATED = "...<truncated>";

  private DebugLogUtil() {
  }

  public static String formatArgs(Object[] args) {
    try {
      if (args == null || args.length == 0) {
        return "[]";
      }
      return formatValue(args);
    } catch (Throwable error) {
      return "<debug-format-error args " + error.getClass().getSimpleName() + ": " + error.getMessage() + ">";
    }
  }

  public static String formatValue(Object value) {
    try {
      StringBuilder builder = new StringBuilder(64);
      render(builder, value, 0);
      return builder.toString();
    } catch (Throwable error) {
      return "<debug-format-error value " + error.getClass().getSimpleName() + ": " + error.getMessage() + ">";
    }
  }

  // Renders into a single budget-bounded builder: once MAX_CHARS is reached nothing else is
  // materialized, so a wide collection of large Thrift objects costs one element, not all of them.
  private static void render(StringBuilder out, Object value, int depth) {
    if (isFull(out)) {
      return;
    }
    if (value == null) {
      out.append("null");
      return;
    }
    if (depth >= MAX_DEPTH) {
      out.append("<max-depth>");
      return;
    }
    if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean
        || value instanceof Enum<?>) {
      appendBounded(out, String.valueOf(value));
      return;
    }
    if (value instanceof Throwable throwable) {
      appendBounded(out, throwable.getClass().getName() + "(" + throwable.getMessage() + ")");
      return;
    }
    if (value.getClass().isArray()) {
      renderArray(out, value, depth + 1);
      return;
    }
    if (value instanceof Collection<?> collection) {
      renderCollection(out, collection, depth + 1);
      return;
    }
    if (value instanceof Map<?, ?> map) {
      renderMap(out, map, depth + 1);
      return;
    }
    // Unknown object: toString() is the only rendering available, so this one value is still
    // materialized in full before it is clipped to the remaining budget.
    appendBounded(out, String.valueOf(value));
  }

  private static void renderArray(StringBuilder out, Object array, int depth) {
    int length = Array.getLength(array);
    out.append('[');
    int limit = Math.min(length, MAX_ELEMENTS);
    int index = 0;
    while (index < limit && !isFull(out)) {
      if (index > 0) {
        out.append(", ");
      }
      render(out, Array.get(array, index), depth);
      index++;
    }
    if (length > index) {
      appendOverflow(out, index, length);
    }
    out.append(']');
  }

  private static void renderCollection(StringBuilder out, Collection<?> collection, int depth) {
    out.append('[');
    Iterator<?> iterator = collection.iterator();
    int index = 0;
    while (iterator.hasNext() && index < MAX_ELEMENTS && !isFull(out)) {
      if (index > 0) {
        out.append(", ");
      }
      render(out, iterator.next(), depth);
      index++;
    }
    if (collection.size() > index) {
      appendOverflow(out, index, collection.size());
    }
    out.append(']');
  }

  private static void renderMap(StringBuilder out, Map<?, ?> map, int depth) {
    out.append('{');
    int index = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (index >= MAX_ELEMENTS || isFull(out)) {
        appendOverflow(out, index, map.size());
        break;
      }
      if (index > 0) {
        out.append(", ");
      }
      render(out, entry.getKey(), depth);
      out.append('=');
      render(out, entry.getValue(), depth);
      index++;
    }
    out.append('}');
  }

  private static void appendOverflow(StringBuilder out, int rendered, int size) {
    if (rendered > 0) {
      out.append(", ");
    }
    out.append("... size=").append(size);
  }

  private static boolean isFull(StringBuilder out) {
    return out.length() >= MAX_CHARS;
  }

  private static void appendBounded(StringBuilder out, String value) {
    int remaining = MAX_CHARS - out.length();
    if (remaining <= 0) {
      return;
    }
    if (value.length() <= remaining) {
      out.append(value);
      return;
    }
    out.append(value, 0, remaining).append(TRUNCATED);
  }
}
