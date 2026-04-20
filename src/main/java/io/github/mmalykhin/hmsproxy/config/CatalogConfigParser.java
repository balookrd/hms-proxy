package io.github.mmalykhin.hmsproxy.config;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class CatalogConfigParser {
  private CatalogConfigParser() {
  }

  static Map<String, String> parseBackendConf(PropertyReader reader) {
    return reader.collectPrefixed("backend.conf.");
  }

  static Map<String, CatalogConfig> parse(
      PropertyReader reader,
      Map<String, String> backendConf,
      boolean globalImpersonation
  ) {
    String catalogsValue = reader.require("catalogs");
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    for (String catalogName : PropertyReader.splitCsv(catalogsValue)) {
      catalogs.put(catalogName, parseCatalog(reader, catalogName, backendConf, globalImpersonation));
    }
    return catalogs;
  }

  static String resolveDefaultCatalog(
      PropertyReader reader,
      Map<String, CatalogConfig> catalogs
  ) {
    String defaultCatalog = reader.getOrNull("routing.default-catalog");
    if (defaultCatalog == null) {
      if (catalogs.size() == 1) {
        return catalogs.keySet().iterator().next();
      }
      throw new IllegalArgumentException(
          "routing.default-catalog is required when more than one catalog is configured");
    }
    if (!catalogs.containsKey(defaultCatalog)) {
      throw new IllegalArgumentException("Unknown routing.default-catalog: " + defaultCatalog);
    }
    return defaultCatalog;
  }

  private static CatalogConfig parseCatalog(
      PropertyReader reader,
      String catalogName,
      Map<String, String> backendConf,
      boolean globalImpersonation
  ) {
    String prefix = "catalog." + catalogName + ".";
    boolean impersonationEnabled = reader.getBoolean(prefix + "impersonation-enabled", globalImpersonation);
    CatalogAccessMode accessMode = parseCatalogAccessMode(reader.getOrNull(prefix + "access-mode"));
    String[] writeDbWhitelist = PropertyReader.splitCsv(reader.get(prefix + "write-db-whitelist", ""));
    CatalogExposureMode exposureMode = parseCatalogExposureMode(reader.getOrNull(prefix + "expose-mode"));
    String[] exposeDbPatterns = PropertyReader.splitCsv(reader.get(prefix + "expose-db-patterns", ""));
    ConfigParsing.validateRegexList(prefix + "expose-db-patterns", exposeDbPatterns);
    Map<String, List<String>> exposeTablePatterns = parseExposeTablePatterns(reader, prefix);
    MetastoreRuntimeProfile runtimeProfile = parseRuntimeProfile(reader.getOrNull(prefix + "runtime-profile"));
    String catalogBackendStandaloneMetastoreJar = reader.getOrNull(prefix + "backend-standalone-metastore-jar");
    long latencyBudgetMs = reader.getNonNegativeLong(prefix + "latency-budget-ms", 0L);
    int maxImpersonationClients = reader.getPositiveInt(prefix + "impersonation-max-clients", 128);
    long impersonationClientIdleTtlMs = reader.getNonNegativeLong(prefix + "impersonation-client-idle-ttl-ms", 0L);
    int sharedSessionPoolSize = reader.getPositiveInt(prefix + "shared-session-pool-size", 1);

    Map<String, String> hiveConfOverrides = reader.collectPrefixed(prefix + "conf.");
    Map<String, String> hiveConf = new LinkedHashMap<>(backendConf);
    hiveConf.putAll(hiveConfOverrides);
    if (!hiveConf.containsKey("hive.metastore.uris")) {
      throw new IllegalArgumentException(
          "Missing backend.conf.hive.metastore.uris or " + prefix + "conf.hive.metastore.uris for catalog "
              + catalogName);
    }

    return new CatalogConfig(
        catalogName,
        reader.get(prefix + "description", catalogName),
        reader.get(prefix + "location-uri", "file:///warehouse/" + catalogName),
        impersonationEnabled,
        accessMode,
        Arrays.asList(writeDbWhitelist),
        exposureMode,
        Arrays.asList(exposeDbPatterns),
        exposeTablePatterns,
        runtimeProfile,
        catalogBackendStandaloneMetastoreJar,
        hiveConf,
        latencyBudgetMs,
        maxImpersonationClients,
        impersonationClientIdleTtlMs,
        sharedSessionPoolSize);
  }

  private static Map<String, List<String>> parseExposeTablePatterns(PropertyReader reader, String prefix) {
    String propertyPrefix = prefix + "expose-table-patterns.";
    Map<String, List<String>> patterns = new LinkedHashMap<>();
    for (String propertyName : reader.namesWithPrefix(propertyPrefix)) {
      String dbPattern = PropertyReader.trimToNull(propertyName.substring(propertyPrefix.length()));
      if (dbPattern == null) {
        throw new IllegalArgumentException(propertyName + " must not have a blank database pattern suffix");
      }
      String rawValue = reader.rawValue(propertyName);
      String[] tablePatterns = PropertyReader.splitCsv(reader.get(propertyName, ""));
      if (rawValue != null && tablePatterns.length == 0) {
        throw new IllegalArgumentException(propertyName + " must define at least one table regex");
      }
      ConfigParsing.validateRegex(propertyName + " (db pattern)", dbPattern);
      ConfigParsing.validateRegexList(propertyName, tablePatterns);
      patterns.put(dbPattern, Arrays.asList(tablePatterns));
    }
    return patterns;
  }

  private static MetastoreRuntimeProfile parseRuntimeProfile(String value) {
    if (value == null) {
      return null;
    }
    return MetastoreRuntimeProfile.valueOf(value.trim().toUpperCase(Locale.ROOT));
  }

  private static CatalogAccessMode parseCatalogAccessMode(String value) {
    if (value == null) {
      return CatalogAccessMode.READ_WRITE;
    }
    try {
      return CatalogAccessMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for catalog.<name>.access-mode: " + value
              + ". Expected one of: READ_ONLY, READ_WRITE, READ_WRITE_DB_WHITELIST",
          e);
    }
  }

  private static CatalogExposureMode parseCatalogExposureMode(String value) {
    if (value == null) {
      return CatalogExposureMode.ALLOW_ALL;
    }
    try {
      return CatalogExposureMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for catalog.<name>.expose-mode: " + value
              + ". Expected one of: ALLOW_ALL, DENY_BY_DEFAULT",
          e);
    }
  }
}
