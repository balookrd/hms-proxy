package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

final class DebugLogUtil {
  private static final int MAX_ELEMENTS = 10;
  private static final int MAX_CHARS = 4_000;
  private static final int MAX_DEPTH = 3;

  private DebugLogUtil() {
  }

  static String formatArgs(Object[] args) {
    if (args == null || args.length == 0) {
      return "[]";
    }
    return formatValue(args);
  }

  static String formatValue(Object value) {
    return truncate(render(value, 0));
  }

  private static String render(Object value, int depth) {
    if (value == null) {
      return "null";
    }
    if (depth >= MAX_DEPTH) {
      return "<max-depth>";
    }
    if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean
        || value instanceof Enum<?>) {
      return String.valueOf(value);
    }
    if (value instanceof Throwable throwable) {
      return throwable.getClass().getName() + "(" + throwable.getMessage() + ")";
    }
    if (value.getClass().isArray()) {
      return renderArray(value, depth + 1);
    }
    if (value instanceof Collection<?> collection) {
      return renderCollection(collection, depth + 1);
    }
    if (value instanceof Map<?, ?> map) {
      return renderMap(map, depth + 1);
    }
    return truncate(String.valueOf(value));
  }

  private static String renderArray(Object array, int depth) {
    int length = Array.getLength(array);
    StringBuilder builder = new StringBuilder("[");
    int limit = Math.min(length, MAX_ELEMENTS);
    for (int index = 0; index < limit; index++) {
      if (index > 0) {
        builder.append(", ");
      }
      builder.append(render(Array.get(array, index), depth));
    }
    if (length > limit) {
      builder.append(", ... size=").append(length);
    }
    return builder.append(']').toString();
  }

  private static String renderCollection(Collection<?> collection, int depth) {
    StringBuilder builder = new StringBuilder("[");
    Iterator<?> iterator = collection.iterator();
    int index = 0;
    while (iterator.hasNext() && index < MAX_ELEMENTS) {
      if (index > 0) {
        builder.append(", ");
      }
      builder.append(render(iterator.next(), depth));
      index++;
    }
    if (collection.size() > index) {
      builder.append(", ... size=").append(collection.size());
    }
    return builder.append(']').toString();
  }

  private static String renderMap(Map<?, ?> map, int depth) {
    StringBuilder builder = new StringBuilder("{");
    int index = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (index >= MAX_ELEMENTS) {
        builder.append(", ... size=").append(map.size());
        break;
      }
      if (index > 0) {
        builder.append(", ");
      }
      builder.append(render(entry.getKey(), depth))
          .append('=')
          .append(render(entry.getValue(), depth));
      index++;
    }
    return builder.append('}').toString();
  }

  private static String truncate(String value) {
    if (value.length() <= MAX_CHARS) {
      return value;
    }
    return value.substring(0, MAX_CHARS) + "...<truncated>";
  }
}
