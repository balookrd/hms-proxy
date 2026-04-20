package io.github.mmalykhin.hmsproxy.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class ConfigParsing {
  private ConfigParsing() {
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
