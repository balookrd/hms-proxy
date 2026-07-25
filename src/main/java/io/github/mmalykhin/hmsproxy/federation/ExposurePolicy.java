package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;

final class ExposurePolicy {
  private final Map<String, CatalogExposurePolicy> catalogs;

  ExposurePolicy(ProxyConfig config) {
    Map<String, CatalogExposurePolicy> byCatalog = new LinkedHashMap<>();
    for (Map.Entry<String, CatalogConfig> entry : config.catalogs().entrySet()) {
      byCatalog.put(entry.getKey(), new CatalogExposurePolicy(entry.getValue()));
    }
    this.catalogs = Map.copyOf(byCatalog);
  }

  boolean isDatabaseExposed(String catalogName, String backendDbName) {
    String normalizedDbName = normalize(backendDbName);
    CatalogExposurePolicy catalogPolicy = catalogs.get(catalogName);
    if (normalizedDbName == null || catalogPolicy == null) {
      return true;
    }
    return databaseExposure(
        catalogPolicy, normalizedDbName, catalogPolicy.hasTableRulesForDatabase(normalizedDbName))
        != DatabaseExposure.HIDDEN;
  }

  boolean isTableExposed(String catalogName, String backendDbName, String tableName) {
    String normalizedDbName = normalize(backendDbName);
    CatalogExposurePolicy catalogPolicy = catalogs.get(catalogName);
    // A blank database name carries no namespace to match rules against, so it falls back to allow
    // the same way the database-only check does. Returning here also keeps the table rules from
    // ever running their database pattern against null.
    if (normalizedDbName == null || catalogPolicy == null) {
      return true;
    }
    String normalizedTableName = normalize(tableName);
    if (normalizedTableName == null) {
      return isDatabaseExposed(catalogName, backendDbName);
    }
    // get_tables/get_table_meta run this per listed object, so the table rules are walked once:
    // the same pass answers whether the database is exposed through a table rule and whether this
    // table matches one.
    TableRuleMatch tableRuleMatch = catalogPolicy.matchTableRules(normalizedDbName, normalizedTableName);
    if (databaseExposure(catalogPolicy, normalizedDbName, tableRuleMatch != TableRuleMatch.NO_RULES)
        == DatabaseExposure.HIDDEN) {
      return false;
    }
    return tableRuleMatch != TableRuleMatch.NO_MATCH;
  }

  private static DatabaseExposure databaseExposure(
      CatalogExposurePolicy catalogPolicy,
      String normalizedDbName,
      boolean hasTableRulesForDatabase
  ) {
    if (catalogPolicy.matchesDatabase(normalizedDbName)) {
      return DatabaseExposure.EXPLICIT_DB_RULE;
    }
    if (hasTableRulesForDatabase) {
      return DatabaseExposure.TABLE_RULE;
    }
    if (catalogPolicy.hasDatabaseRules()) {
      return DatabaseExposure.HIDDEN;
    }
    return catalogPolicy.exposeMode() == CatalogExposureMode.DENY_BY_DEFAULT
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

  private enum TableRuleMatch {
    NO_RULES,
    MATCHED,
    NO_MATCH
  }

  private record CatalogExposurePolicy(
      CatalogExposureMode exposeMode,
      List<Pattern> databasePatterns,
      List<TableExposureRule> tableRules
  ) {
    private CatalogExposurePolicy(CatalogConfig catalogConfig) {
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
      for (TableExposureRule rule : tableRules) {
        if (rule.matchesDatabase(backendDbName)) {
          return true;
        }
      }
      return false;
    }

    private TableRuleMatch matchTableRules(String backendDbName, String tableName) {
      TableRuleMatch result = TableRuleMatch.NO_RULES;
      for (TableExposureRule rule : tableRules) {
        if (!rule.matchesDatabase(backendDbName)) {
          continue;
        }
        if (rule.matchesTable(tableName)) {
          return TableRuleMatch.MATCHED;
        }
        result = TableRuleMatch.NO_MATCH;
      }
      return result;
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
