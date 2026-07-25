package io.github.mmalykhin.hmsproxy.config.catalog;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
public final class CatalogConfigParser {
  private CatalogConfigParser() {
  }

  public static Map<String, String> parseBackendConf(PropertyReader reader) {
    return reader.collectPrefixed("backend.conf.");
  }

  public static Map<String, CatalogConfig> parse(
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

  public static String resolveDefaultCatalog(
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
    CatalogAccessMode accessMode = ConfigParsing.parseEnum(
        CatalogAccessMode.class,
        reader.getOrNull(prefix + "access-mode"),
        prefix + "access-mode",
        CatalogAccessMode.READ_WRITE);
    String[] writeDbWhitelist = PropertyReader.splitCsv(reader.get(prefix + "write-db-whitelist", ""));
    validateWriteDbWhitelist(prefix, accessMode, writeDbWhitelist);
    CatalogExposureMode exposureMode = ConfigParsing.parseEnum(
        CatalogExposureMode.class,
        reader.getOrNull(prefix + "expose-mode"),
        prefix + "expose-mode",
        CatalogExposureMode.ALLOW_ALL);
    String[] exposeDbPatterns = PropertyReader.splitCsv(reader.get(prefix + "expose-db-patterns", ""));
    ConfigParsing.validateRegexList(prefix + "expose-db-patterns", exposeDbPatterns);
    Map<String, List<String>> exposeTablePatterns = parseExposeTablePatterns(reader, prefix);
    MetastoreRuntimeProfile runtimeProfile = ConfigParsing.parseEnum(
        MetastoreRuntimeProfile.class,
        reader.getOrNull(prefix + "runtime-profile"),
        prefix + "runtime-profile",
        null);
    String catalogBackendStandaloneMetastoreJar = reader.getOrNull(prefix + "backend-standalone-metastore-jar");
    long latencyBudgetMs = reader.getNonNegativeLong(prefix + "latency-budget-ms", 0L);
    int maxImpersonationClients = reader.getPositiveInt(prefix + "impersonation-max-clients", 128);
    long impersonationClientIdleTtlMs = reader.getNonNegativeLong(prefix + "impersonation-client-idle-ttl-ms", 0L);
    int sharedSessionPoolSize = reader.getPositiveInt(prefix + "shared-session-pool-size", 1);
    int impersonationPoolMaxSize = reader.getPositiveInt(prefix + "impersonation-pool-max-size", 4);
    long impersonationSessionIdleTtlMs = reader.getNonNegativeLong(
        prefix + "impersonation-session-idle-ttl-ms", 0L);

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
        sharedSessionPoolSize,
        impersonationPoolMaxSize,
        impersonationSessionIdleTtlMs);
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

  /**
   * The whitelist is only consulted by {@code CatalogAccessModeGuard} in READ_WRITE_DB_WHITELIST
   * mode, so either half without the other means the catalog behaves differently than configured.
   */
  private static void validateWriteDbWhitelist(
      String prefix,
      CatalogAccessMode accessMode,
      String[] writeDbWhitelist
  ) {
    boolean whitelistConfigured = writeDbWhitelist.length > 0;
    if (whitelistConfigured && accessMode != CatalogAccessMode.READ_WRITE_DB_WHITELIST) {
      throw new IllegalArgumentException(
          prefix + "write-db-whitelist is set but " + prefix + "access-mode is " + accessMode
              + ", so the whitelist would be ignored and writes stay allowed for every database. "
              + "Set " + prefix + "access-mode=READ_WRITE_DB_WHITELIST, or drop the whitelist.");
    }
    if (!whitelistConfigured && accessMode == CatalogAccessMode.READ_WRITE_DB_WHITELIST) {
      throw new IllegalArgumentException(
          prefix + "access-mode=READ_WRITE_DB_WHITELIST requires a non-empty " + prefix
              + "write-db-whitelist. Use " + prefix + "access-mode=READ_ONLY to forbid all writes.");
    }
  }

}
