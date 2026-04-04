package io.github.mmalykhin.hmsproxy.docs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.yaml.snakeyaml.Yaml;

final class CapabilityMatrixData {
  static final Path MATRIX_PATH = Path.of("capabilities.yaml");
  static final Path README_PATH = Path.of("README.md");
  static final Path README_RU_PATH = Path.of("README.ru.md");
  static final String BEGIN_MARKER = "<!-- BEGIN GENERATED: capability-matrix -->";
  static final String END_MARKER = "<!-- END GENERATED: capability-matrix -->";
  private static final Set<String> ALLOWED_STATUS_VALUES = Set.of("supported", "degraded", "rejected", "passthrough");

  private CapabilityMatrixData() {
  }

  static CapabilityMatrix loadDefault() throws IOException {
    return load(MATRIX_PATH);
  }

  static CapabilityMatrix load(Path path) throws IOException {
    Yaml yaml = new Yaml();
    try (InputStream inputStream = Files.newInputStream(path)) {
      Object loaded = yaml.load(inputStream);
      if (!(loaded instanceof Map<?, ?> root)) {
        throw new IllegalArgumentException("Expected top-level YAML mapping in " + path);
      }
      int schemaVersion = intValue(root, "schema_version");
      List<Map<String, Object>> capabilityMaps = listOfMaps(root, "capabilities");
      List<Capability> capabilities = new ArrayList<>();
      for (Map<String, Object> capabilityMap : capabilityMaps) {
        capabilities.add(parseCapability(capabilityMap));
      }
      return new CapabilityMatrix(schemaVersion, List.copyOf(capabilities));
    }
  }

  static String renderMarkdownTable(CapabilityMatrix matrix, Language language) {
    StringBuilder builder = new StringBuilder();
    builder.append("| ")
        .append(language == Language.EN ? "Client version" : "Версия клиента")
        .append(" | ")
        .append(language == Language.EN ? "Front-door profile" : "Профиль front door")
        .append(" | ")
        .append(language == Language.EN ? "Backend profile" : "Профиль backend")
        .append(" | ")
        .append(language == Language.EN ? "Auth mode" : "Режим auth")
        .append(" | ")
        .append(language == Language.EN ? "Method families" : "Семейства методов")
        .append(" | ")
        .append(language == Language.EN ? "Expected result" : "Ожидаемый результат")
        .append(" |\n");
    builder.append("| --- | --- | --- | --- | --- | --- |\n");
    for (Capability capability : matrix.capabilities()) {
      builder.append("| ")
          .append(escapeCell(capability.client().value(language)))
          .append(" | ")
          .append(escapeCell(capability.frontDoor().value(language)))
          .append(" | ")
          .append(escapeCell(capability.backend().value(language)))
          .append(" | ")
          .append(escapeCell(capability.auth().value(language)))
          .append(" | ")
          .append(escapeCell(String.join(", ", capability.methodFamilies().value(language))))
          .append(" | ")
          .append(escapeCell(capability.expectedResult().value(language)))
          .append(" |\n");
    }
    return builder.toString().trim();
  }

  static String syncReadme(String readmeContent, CapabilityMatrix matrix, Language language) {
    String generatedBlock = BEGIN_MARKER + "\n"
        + renderMarkdownTable(matrix, language) + "\n"
        + END_MARKER;
    int begin = readmeContent.indexOf(BEGIN_MARKER);
    int end = readmeContent.indexOf(END_MARKER);
    if (begin < 0 || end < 0 || end < begin) {
      throw new IllegalArgumentException("Missing capability matrix markers in README content");
    }
    int endInclusive = end + END_MARKER.length();
    return readmeContent.substring(0, begin)
        + generatedBlock
        + readmeContent.substring(endInclusive);
  }

  private static Capability parseCapability(Map<String, Object> map) {
    String id = stringValue(map, "id");
    String status = stringValue(map, "status");
    if (!ALLOWED_STATUS_VALUES.contains(status)) {
      throw new IllegalArgumentException("Unsupported capability status '" + status + "' for " + id);
    }
    return new Capability(
        id,
        status,
        parseLocalizedText(map, "client"),
        parseDisplayCell(map, "front_door"),
        parseDisplayCell(map, "backend"),
        parseDisplayCell(map, "auth"),
        parseLocalizedList(map, "method_families"),
        parseLocalizedText(map, "expected_result"),
        stringList(map, "smoke_tests"));
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> listOfMaps(Map<?, ?> map, String key) {
    Object value = map.get(key);
    if (!(value instanceof List<?> rawList)) {
      throw new IllegalArgumentException("Expected list at key '" + key + "'");
    }
    List<Map<String, Object>> result = new ArrayList<>();
    for (Object entry : rawList) {
      if (!(entry instanceof Map<?, ?> rawMap)) {
        throw new IllegalArgumentException("Expected mapping entries under '" + key + "'");
      }
      Map<String, Object> converted = new LinkedHashMap<>();
      for (Map.Entry<?, ?> item : rawMap.entrySet()) {
        converted.put(String.valueOf(item.getKey()), item.getValue());
      }
      result.add(converted);
    }
    return result;
  }

  private static LocalizedText parseDisplayCell(Map<String, Object> map, String key) {
    Map<String, Object> section = section(map, key);
    return new LocalizedText(stringValue(section, "en"), stringValue(section, "ru"));
  }

  private static LocalizedText parseLocalizedText(Map<String, Object> map, String key) {
    Map<String, Object> section = section(map, key);
    return new LocalizedText(stringValue(section, "en"), stringValue(section, "ru"));
  }

  private static LocalizedList parseLocalizedList(Map<String, Object> map, String key) {
    Map<String, Object> section = section(map, key);
    return new LocalizedList(stringList(section, "en"), stringList(section, "ru"));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> section(Map<String, Object> map, String key) {
    Object value = map.get(key);
    if (!(value instanceof Map<?, ?> rawMap)) {
      throw new IllegalArgumentException("Expected mapping at key '" + key + "'");
    }
    Map<String, Object> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
      result.put(String.valueOf(entry.getKey()), entry.getValue());
    }
    return result;
  }

  private static int intValue(Map<?, ?> map, String key) {
    Object value = map.get(key);
    if (value instanceof Number number) {
      return number.intValue();
    }
    throw new IllegalArgumentException("Expected integer at key '" + key + "'");
  }

  private static String stringValue(Map<?, ?> map, String key) {
    Object value = map.get(key);
    if (!(value instanceof String stringValue) || stringValue.isBlank()) {
      throw new IllegalArgumentException("Expected non-blank string at key '" + key + "'");
    }
    return stringValue;
  }

  @SuppressWarnings("unchecked")
  private static List<String> stringList(Map<?, ?> map, String key) {
    Object value = map.get(key);
    if (!(value instanceof List<?> rawList)) {
      throw new IllegalArgumentException("Expected list at key '" + key + "'");
    }
    List<String> result = new ArrayList<>();
    for (Object entry : rawList) {
      if (!(entry instanceof String stringEntry) || stringEntry.isBlank()) {
        throw new IllegalArgumentException("Expected non-blank string entries under '" + key + "'");
      }
      result.add(stringEntry);
    }
    return List.copyOf(result);
  }

  private static String escapeCell(String value) {
    return value.replace("|", "\\|").replace("\n", "<br>");
  }

  record CapabilityMatrix(int schemaVersion, List<Capability> capabilities) {
    CapabilityMatrix {
      capabilities = List.copyOf(capabilities);
    }
  }

  record Capability(
      String id,
      String status,
      LocalizedText client,
      LocalizedText frontDoor,
      LocalizedText backend,
      LocalizedText auth,
      LocalizedList methodFamilies,
      LocalizedText expectedResult,
      List<String> smokeTests
  ) {
    Capability {
      smokeTests = List.copyOf(smokeTests);
    }
  }

  record LocalizedText(String en, String ru) {
    LocalizedText {
      en = requireText("en", en);
      ru = requireText("ru", ru);
    }

    String value(Language language) {
      return language == Language.EN ? en : ru;
    }
  }

  record LocalizedList(List<String> en, List<String> ru) {
    LocalizedList {
      en = validateList("en", en);
      ru = validateList("ru", ru);
    }

    List<String> value(Language language) {
      return language == Language.EN ? en : ru;
    }

    private static List<String> validateList(String language, List<String> values) {
      Objects.requireNonNull(values, language + " list");
      if (values.isEmpty()) {
        throw new IllegalArgumentException("Expected at least one " + language + " list value");
      }
      if (values.stream().anyMatch(value -> value == null || value.isBlank())) {
        throw new IllegalArgumentException("Expected non-blank " + language + " list entries");
      }
      return List.copyOf(values);
    }
  }

  enum Language {
    EN,
    RU;

    static Language fromTag(String tag) {
      return Arrays.stream(values())
          .filter(language -> language.name().equalsIgnoreCase(tag.toUpperCase(Locale.ROOT)))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Unsupported language tag: " + tag));
    }
  }

  private static String requireText(String key, String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Expected non-blank localized text for " + key);
    }
    return value;
  }
}
