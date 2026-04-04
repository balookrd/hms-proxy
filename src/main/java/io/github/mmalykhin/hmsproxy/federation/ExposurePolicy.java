package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

final class ExposurePolicy {
  private final Map<String, CatalogExposurePolicy> catalogs;

  ExposurePolicy(ProxyConfig config) {
    Map<String, CatalogExposurePolicy> byCatalog = new LinkedHashMap<>();
    for (Map.Entry<String, ProxyConfig.CatalogConfig> entry : config.catalogs().entrySet()) {
      byCatalog.put(entry.getKey(), new CatalogExposurePolicy(entry.getValue()));
    }
    this.catalogs = Map.copyOf(byCatalog);
  }

  boolean isDatabaseExposed(String catalogName, String backendDbName) {
    return databaseExposure(catalogName, backendDbName) != DatabaseExposure.HIDDEN;
  }

  boolean isTableExposed(String catalogName, String backendDbName, String tableName) {
    DatabaseExposure databaseExposure = databaseExposure(catalogName, backendDbName);
    if (databaseExposure == DatabaseExposure.HIDDEN) {
      return false;
    }
    String normalizedTableName = normalize(tableName);
    if (normalizedTableName == null) {
      return true;
    }
    CatalogExposurePolicy catalogPolicy = catalogs.get(catalogName);
    if (catalogPolicy == null) {
      return true;
    }
    List<TableExposureRule> matchingTableRules = catalogPolicy.matchingTableRules(normalize(backendDbName));
    if (!matchingTableRules.isEmpty()) {
      for (TableExposureRule rule : matchingTableRules) {
        if (rule.matchesTable(normalizedTableName)) {
          return true;
        }
      }
      return false;
    }
    return true;
  }

  private DatabaseExposure databaseExposure(String catalogName, String backendDbName) {
    String normalizedDbName = normalize(backendDbName);
    if (normalizedDbName == null) {
      return DatabaseExposure.FALLBACK_ALLOW;
    }
    CatalogExposurePolicy catalogPolicy = catalogs.get(catalogName);
    if (catalogPolicy == null) {
      return DatabaseExposure.FALLBACK_ALLOW;
    }
    if (catalogPolicy.matchesDatabase(normalizedDbName)) {
      return DatabaseExposure.EXPLICIT_DB_RULE;
    }
    if (catalogPolicy.hasTableRulesForDatabase(normalizedDbName)) {
      return DatabaseExposure.TABLE_RULE;
    }
    if (catalogPolicy.hasDatabaseRules()) {
      return DatabaseExposure.HIDDEN;
    }
    return catalogPolicy.exposeMode() == ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT
        ? DatabaseExposure.HIDDEN
        : DatabaseExposure.FALLBACK_ALLOW;
  }

  private static String normalize(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }

  private enum DatabaseExposure {
    EXPLICIT_DB_RULE,
    TABLE_RULE,
    FALLBACK_ALLOW,
    HIDDEN
  }

  private record CatalogExposurePolicy(
      ProxyConfig.CatalogExposureMode exposeMode,
      List<Pattern> databasePatterns,
      List<TableExposureRule> tableRules
  ) {
    private CatalogExposurePolicy(ProxyConfig.CatalogConfig catalogConfig) {
      this(
          catalogConfig.exposeMode(),
          compilePatterns(catalogConfig.exposeDbPatterns()),
          compileTableRules(catalogConfig.exposeTablePatterns()));
    }

    private boolean hasDatabaseRules() {
      return !databasePatterns.isEmpty();
    }

    private boolean matchesDatabase(String backendDbName) {
      return matchesAny(databasePatterns, backendDbName);
    }

    private boolean hasTableRulesForDatabase(String backendDbName) {
      return !matchingTableRules(backendDbName).isEmpty();
    }

    private List<TableExposureRule> matchingTableRules(String backendDbName) {
      List<TableExposureRule> matches = new ArrayList<>();
      for (TableExposureRule rule : tableRules) {
        if (rule.matchesDatabase(backendDbName)) {
          matches.add(rule);
        }
      }
      return matches;
    }
  }

  private record TableExposureRule(Pattern databasePattern, List<Pattern> tablePatterns) {
    private boolean matchesDatabase(String backendDbName) {
      return databasePattern.matcher(backendDbName).matches();
    }

    private boolean matchesTable(String tableName) {
      return matchesAny(tablePatterns, tableName);
    }
  }

  private static List<Pattern> compilePatterns(List<String> regexes) {
    List<Pattern> patterns = new ArrayList<>(regexes.size());
    for (String regex : regexes) {
      patterns.add(Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
    }
    return List.copyOf(patterns);
  }

  private static List<TableExposureRule> compileTableRules(Map<String, List<String>> rules) {
    List<TableExposureRule> compiled = new ArrayList<>(rules.size());
    for (Map.Entry<String, List<String>> entry : rules.entrySet()) {
      compiled.add(new TableExposureRule(
          Pattern.compile(entry.getKey(), Pattern.CASE_INSENSITIVE),
          compilePatterns(entry.getValue())));
    }
    return List.copyOf(compiled);
  }

  private static boolean matchesAny(List<Pattern> patterns, String value) {
    for (Pattern pattern : patterns) {
      if (pattern.matcher(value).matches()) {
        return true;
      }
    }
    return false;
  }
}
