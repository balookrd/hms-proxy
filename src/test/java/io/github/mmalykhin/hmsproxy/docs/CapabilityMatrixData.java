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
  static final Path COMPATIBILITY_DOC_PATH = Path.of("COMPATIBILITY.md");
  static final Path COMPATIBILITY_DOC_RU_PATH = Path.of("COMPATIBILITY.ru.md");
  static final String BEGIN_MARKER = "<!-- BEGIN GENERATED: capability-matrix -->";
  static final String END_MARKER = "<!-- END GENERATED: capability-matrix -->";
  static final String METHOD_BEGIN_MARKER = "<!-- BEGIN GENERATED: method-compatibility-matrix -->";
  static final String METHOD_END_MARKER = "<!-- END GENERATED: method-compatibility-matrix -->";
  private static final Set<String> ALLOWED_STATUS_VALUES = Set.of("supported", "degraded", "rejected", "passthrough", "unsafe");

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
      List<Map<String, Object>> methodMatrixMaps = listOfMapsOptional(root, "method_matrix");
      List<MethodMatrixRow> methodMatrixRows = new ArrayList<>();
      for (Map<String, Object> methodMatrixMap : methodMatrixMaps) {
        methodMatrixRows.add(parseMethodMatrixRow(methodMatrixMap));
      }
      return new CapabilityMatrix(schemaVersion, List.copyOf(capabilities), List.copyOf(methodMatrixRows));
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

  static String renderMethodCompatibilityMarkdownTable(CapabilityMatrix matrix, Language language) {
    StringBuilder builder = new StringBuilder();
    builder.append("| ")
        .append(language == Language.EN ? "Method(s)" : "Метод(ы)")
        .append(" | ")
        .append(language == Language.EN ? "Apache backend" : "Apache backend")
        .append(" | ")
        .append(language == Language.EN ? "HDP backend" : "HDP backend")
        .append(" | ")
        .append(language == Language.EN ? "Frontend profile support" : "Поддержка front-door profile")
        .append(" | ")
        .append(language == Language.EN ? "Routing mode" : "Режим маршрутизации")
        .append(" | ")
        .append(language == Language.EN ? "Fallback strategy" : "Стратегия fallback")
        .append(" | ")
        .append(language == Language.EN ? "Semantic risk flag" : "Флаг semantic-risk")
        .append(" |\n");
    builder.append("| --- | --- | --- | --- | --- | --- | --- |\n");
    for (MethodMatrixRow row : matrix.methodMatrixRows()) {
      builder.append("| ")
          .append(escapeCell(row.method().value(language)))
          .append(" | ")
          .append(escapeCell(formatSupportCell(row.apacheBackend(), language)))
          .append(" | ")
          .append(escapeCell(formatSupportCell(row.hdpBackend(), language)))
          .append(" | ")
          .append(escapeCell(row.frontDoorSupport().value(language)))
          .append(" | ")
          .append(escapeCell(row.routingMode().value(language)))
          .append(" | ")
          .append(escapeCell(row.fallbackStrategy().value(language)))
          .append(" | ")
          .append(escapeCell(row.semanticRisk().value(language)))
          .append(" |\n");
    }
    return builder.toString().trim();
  }

  static String syncReadme(String readmeContent, CapabilityMatrix matrix, Language language) {
    return syncBlock(
        readmeContent,
        BEGIN_MARKER,
        END_MARKER,
        renderMarkdownTable(matrix, language),
        "README content");
  }

  static String syncMethodCompatibilityDoc(String docContent, CapabilityMatrix matrix, Language language) {
    return syncBlock(
        docContent,
        METHOD_BEGIN_MARKER,
        METHOD_END_MARKER,
        renderMethodCompatibilityMarkdownTable(matrix, language),
        "method compatibility doc content");
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

  private static MethodMatrixRow parseMethodMatrixRow(Map<String, Object> map) {
    String id = stringValue(map, "id");
    return new MethodMatrixRow(
        id,
        parseLocalizedText(map, "method"),
        parseSupportCell(map, "apache_backend"),
        parseSupportCell(map, "hdp_backend"),
        parseLocalizedText(map, "front_door_support"),
        parseLocalizedText(map, "routing_mode"),
        parseLocalizedText(map, "fallback_strategy"),
        parseLocalizedText(map, "semantic_risk"),
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

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> listOfMapsOptional(Map<?, ?> map, String key) {
    Object value = map.get(key);
    if (value == null) {
      return List.of();
    }
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

  private static SupportCell parseSupportCell(Map<String, Object> map, String key) {
    Map<String, Object> section = section(map, key);
    String status = stringValue(section, "status");
    if (!ALLOWED_STATUS_VALUES.contains(status)) {
      throw new IllegalArgumentException("Unsupported support status '" + status + "' in section '" + key + "'");
    }
    return new SupportCell(
        status,
        new LocalizedText(stringValue(section, "en"), stringValue(section, "ru")));
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

  private static String formatSupportCell(SupportCell supportCell, Language language) {
    return "`" + supportCell.status() + "`: " + supportCell.detail().value(language);
  }

  private static String syncBlock(
      String content,
      String beginMarker,
      String endMarker,
      String renderedTable,
      String contentDescription
  ) {
    String generatedBlock = beginMarker + "\n"
        + renderedTable + "\n"
        + endMarker;
    int begin = content.indexOf(beginMarker);
    int end = content.indexOf(endMarker);
    if (begin < 0 || end < 0 || end < begin) {
      throw new IllegalArgumentException("Missing generated block markers in " + contentDescription);
    }
    int endInclusive = end + endMarker.length();
    return content.substring(0, begin)
        + generatedBlock
        + content.substring(endInclusive);
  }

  record CapabilityMatrix(int schemaVersion, List<Capability> capabilities, List<MethodMatrixRow> methodMatrixRows) {
    CapabilityMatrix {
      capabilities = List.copyOf(capabilities);
      methodMatrixRows = List.copyOf(methodMatrixRows);
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

  record MethodMatrixRow(
      String id,
      LocalizedText method,
      SupportCell apacheBackend,
      SupportCell hdpBackend,
      LocalizedText frontDoorSupport,
      LocalizedText routingMode,
      LocalizedText fallbackStrategy,
      LocalizedText semanticRisk,
      List<String> smokeTests
  ) {
    MethodMatrixRow {
      smokeTests = List.copyOf(smokeTests);
    }
  }

  record SupportCell(String status, LocalizedText detail) {
    SupportCell {
      if (status == null || status.isBlank()) {
        throw new IllegalArgumentException("Expected non-blank support status");
      }
      detail = Objects.requireNonNull(detail, "detail");
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
