package io.github.mmalykhin.hmsproxy.config;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

final class PropertyReader {
  private final Properties properties;

  PropertyReader(Properties properties) {
    this.properties = properties;
  }

  String get(String key, String defaultValue) {
    return Objects.requireNonNullElse(trimToNull(properties.getProperty(key)), defaultValue);
  }

  String getOrNull(String key) {
    return trimToNull(properties.getProperty(key));
  }

  String require(String key) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      throw new IllegalArgumentException("Missing required property: " + key);
    }
    return value;
  }

  boolean has(String key) {
    return properties.containsKey(key);
  }

  boolean hasPrefix(String prefix) {
    return properties.stringPropertyNames().stream().anyMatch(name -> name.startsWith(prefix));
  }

  boolean getBoolean(String key, boolean defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  int getInt(String key, int defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer value for property " + key + ": " + value, e);
    }
  }

  int getNonNegativeInt(String key, int defaultValue) {
    int value = getInt(key, defaultValue);
    if (value < 0) {
      throw new IllegalArgumentException(key + " must be >= 0, got: " + value);
    }
    return value;
  }

  int getPositiveInt(String key, int defaultValue) {
    int value = getInt(key, defaultValue);
    if (value < 1) {
      throw new IllegalArgumentException(key + " must be >= 1, got: " + value);
    }
    return value;
  }

  long getLong(String key, long defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid long value for property " + key + ": " + value, e);
    }
  }

  long getNonNegativeLong(String key, long defaultValue) {
    long value = getLong(key, defaultValue);
    if (value < 0L) {
      throw new IllegalArgumentException(key + " must be >= 0, got: " + value);
    }
    return value;
  }

  long getPositiveLong(String key, long defaultValue) {
    long value = getLong(key, defaultValue);
    if (value < 1L) {
      throw new IllegalArgumentException(key + " must be >= 1, got: " + value);
    }
    return value;
  }

  double getDouble(String key, double defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid floating point value for property " + key + ": " + value, e);
    }
  }

  double getPositiveDouble(String key, double defaultValue) {
    double value = getDouble(key, defaultValue);
    if (value <= 0.0d) {
      throw new IllegalArgumentException(key + " must be > 0, got: " + value);
    }
    return value;
  }

  double getBoundedDouble(String key, double defaultValue, double minExclusive, double maxInclusive) {
    double value = getDouble(key, defaultValue);
    if (value <= minExclusive || value > maxInclusive) {
      throw new IllegalArgumentException(
          key + " must be > " + minExclusive + " and <= " + maxInclusive + ", got: " + value);
    }
    return value;
  }

  /** Returns all property-name suffixes under {@code prefix}, sorted, mapped to their raw values. */
  Map<String, String> collectPrefixed(String prefix) {
    return properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(prefix))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring(prefix.length()),
            properties::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));
  }

  /** Returns sorted, distinct scoped names (first dot-separated token) under {@code prefix}. */
  List<String> scopedNames(String prefix) {
    return properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(prefix))
        .map(name -> extractScopedName(name, prefix))
        .filter(Objects::nonNull)
        .collect(Collectors.toCollection(LinkedHashSet::new))
        .stream()
        .sorted()
        .toList();
  }

  /** Returns sorted property names under {@code prefix} (full names, not stripped). */
  List<String> namesWithPrefix(String prefix) {
    return properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(prefix))
        .sorted()
        .toList();
  }

  String rawValue(String key) {
    return properties.getProperty(key);
  }

  static String[] splitCsv(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(token -> !token.isEmpty())
        .toArray(String[]::new);
  }

  static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static String extractScopedName(String propertyName, String prefix) {
    String suffix = propertyName.substring(prefix.length());
    int separatorIndex = suffix.lastIndexOf('.');
    if (separatorIndex <= 0) {
      return null;
    }
    return suffix.substring(0, separatorIndex);
  }
}
