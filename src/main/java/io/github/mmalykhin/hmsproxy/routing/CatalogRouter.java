package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.api.MetaException;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;

public final class CatalogRouter implements AutoCloseable {
  private volatile ProxyConfig config;
  private final Map<String, CatalogBackend> backends;
  private volatile List<CatalogPrefix> patternPrefixes;
  private volatile Map<String, String> unprefixedDatabases;
  private volatile Set<String> mergeWithDefaultCatalogs;
  private final java.util.concurrent.ConcurrentHashMap<String, String> dynamicMergedDatabases = new java.util.concurrent.ConcurrentHashMap<>();
  private final Set<String> defaultCatalogDatabases = java.util.concurrent.ConcurrentHashMap.newKeySet();
  private final java.util.concurrent.ConcurrentHashMap<String, Long> negativeResolutionCache = new java.util.concurrent.ConcurrentHashMap<>();

  CatalogRouter(ProxyConfig config, Map<String, CatalogBackend> backends) {
    this.config = config;
    this.backends = backends;
    this.patternPrefixes = buildPatternPrefixes(config, backends.keySet());
    this.unprefixedDatabases = buildUnprefixedDatabases(config);
    this.mergeWithDefaultCatalogs = buildMergeWithDefaultCatalogs(config);
  }

  public static CatalogRouter createForTest(ProxyConfig config, Map<String, CatalogBackend> backends) {
    return new CatalogRouter(config, backends);
  }

  public static CatalogRouter open(ProxyConfig config) throws MetaException {
    return open(config, null);
  }

  public static CatalogRouter open(ProxyConfig config, PrometheusMetrics metrics) throws MetaException {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    try {
      for (Map.Entry<String, CatalogConfig> entry : config.catalogs().entrySet()) {
        backends.put(entry.getKey(), CatalogBackend.open(config, entry.getValue(), metrics));
      }
      return new CatalogRouter(config, backends);
    } catch (Throwable t) {
      for (CatalogBackend backend : backends.values()) {
        CatalogBackend.closeQuietly(backend, "backend catalog '" + backend.name() + "'");
      }
      if (t instanceof MetaException me) {
        throw me;
      }
      if (t instanceof RuntimeException re) {
        throw re;
      }
      MetaException metaException = new MetaException("Failed to open catalog backends: " + t.getMessage());
      metaException.initCause(t);
      throw metaException;
    }
  }

  public Collection<CatalogBackend> backends() {
    return backends.values();
  }

  public boolean singleCatalog() {
    return backends.size() == 1;
  }

  public CatalogBackend defaultBackend() {
    return requireBackend(config.defaultCatalog());
  }

  public CatalogBackend requireBackend(String catalog) {
    if (!backends.containsKey(catalog)) {
      throw new IllegalArgumentException("Unknown catalog: " + catalog);
    }
    return backends.get(catalog);
  }

  public Optional<ResolvedNamespace> resolveCatalogIfKnown(String catalog, String backendDbName) {
    if (catalog == null || catalog.isBlank() || !backends.containsKey(catalog)) {
      return Optional.empty();
    }
    return Optional.of(resolveCatalog(catalog, backendDbName));
  }

  public Optional<CatalogBackend> resolveFallbackBackend(String catalogName) {
    if (catalogName == null || !backends.containsKey(catalogName)) {
      return Optional.empty();
    }
    CatalogBackend primary = backends.get(catalogName);
    if (primary != null && primary.fallbackOnOutage() && primary.fallbackCatalog() != null) {
      CatalogBackend fallback = backends.get(primary.fallbackCatalog());
      if (fallback != null) {
        return Optional.of(fallback);
      }
    }
    return Optional.empty();
  }

  public ResolvedNamespace resolveDatabase(String dbName) throws MetaException {
    String normalizedDbName = normalizeExternalDbName(dbName);
    if (normalizedDbName == null || normalizedDbName.isBlank()) {
      return resolveCatalog(config.defaultCatalog(), normalizedDbName);
    }

    String prefixedCatalog = prefixedCatalog(normalizedDbName);
    if (prefixedCatalog != null) {
      String rawBackendDb = normalizedDbName.substring(prefixedCatalog.length() + config.catalogDbSeparator().length());
      String backendDb = rawBackendDb.contains(".") && !rawBackendDb.contains("*") && !rawBackendDb.contains("%")
          ? rawBackendDb.replace('.', '_')
          : rawBackendDb;
      String canonicalExternalDb = isUnprefixedDatabase(prefixedCatalog, backendDb)
          ? backendDb
          : prefixedCatalog + config.catalogDbSeparator() + backendDb;
      return resolveCatalog(prefixedCatalog, backendDb, canonicalExternalDb);
    }

    String lowerDb = normalizedDbName.toLowerCase(java.util.Locale.ROOT);
    String routedCatalog = unprefixedDatabases.get(lowerDb);
    if (routedCatalog != null && backends.containsKey(routedCatalog)) {
      return resolveCatalog(routedCatalog, normalizedDbName, normalizedDbName);
    }

    if (hasMergeWithDefaultCatalogs()) {
      if (defaultCatalogDatabases.contains(lowerDb)) {
        return resolveCatalog(config.defaultCatalog(), normalizedDbName);
      }
      String mergedCat = dynamicMergedDatabases.get(lowerDb);
      if (mergedCat != null && backends.containsKey(mergedCat)) {
        return resolveCatalog(mergedCat, normalizedDbName, normalizedDbName);
      }
      String resolved = lookupDatabaseCatalogOnDemand(normalizedDbName);
      if (resolved != null && backends.containsKey(resolved)) {
        return resolveCatalog(resolved, normalizedDbName, normalizedDbName);
      }
    }

    return resolveCatalog(config.defaultCatalog(), normalizedDbName);
  }

  public Optional<ResolvedNamespace> resolvePattern(String dbPattern) {
    String normalizedDbPattern = normalizeExternalDbName(dbPattern);
    if (normalizedDbPattern == null || normalizedDbPattern.isBlank()) {
      return Optional.empty();
    }
    for (CatalogPrefix candidate : patternPrefixes) {
      if (normalizedDbPattern.startsWith(candidate.prefix())) {
        String remainder = normalizedDbPattern.substring(candidate.prefix().length());
        String backendDb = remainder.contains(".") && !remainder.contains("*") && !remainder.contains("%")
            ? remainder.replace('.', '_')
            : remainder;
        String externalDbName = isUnprefixedDatabase(candidate.catalogName(), backendDb)
            ? backendDb
            : candidate.catalogName() + config.catalogDbSeparator() + backendDb;
        return Optional.of(resolveCatalog(candidate.catalogName(), backendDb, externalDbName));
      }
    }
    if (!normalizedDbPattern.contains("*") && !normalizedDbPattern.contains("%")) {
      String lowerPattern = normalizedDbPattern.toLowerCase(java.util.Locale.ROOT);
      String routedCatalog = unprefixedDatabases.get(lowerPattern);
      if (routedCatalog != null && backends.containsKey(routedCatalog)) {
        return Optional.of(resolveCatalog(routedCatalog, normalizedDbPattern, normalizedDbPattern));
      }
      if (hasMergeWithDefaultCatalogs()) {
        if (!defaultCatalogDatabases.contains(lowerPattern)) {
          String mergedCat = dynamicMergedDatabases.get(lowerPattern);
          if (mergedCat == null) {
            mergedCat = lookupDatabaseCatalogOnDemand(normalizedDbPattern);
          }
          if (mergedCat != null && !mergedCat.equals(config.defaultCatalog()) && backends.containsKey(mergedCat)) {
            return Optional.of(resolveCatalog(mergedCat, normalizedDbPattern, normalizedDbPattern));
          }
        }
      }
    }
    return Optional.empty();
  }

  public ResolvedNamespace resolveCatalog(String catalog, String backendDbName) {
    String effectiveDbName = backendDbName == null || backendDbName.isBlank() ? "" : backendDbName;
    return resolveCatalog(catalog, effectiveDbName, externalDatabaseName(catalog, effectiveDbName));
  }

  private ResolvedNamespace resolveCatalog(String catalog, String backendDbName, String externalDbName) {
    return new ResolvedNamespace(requireBackend(catalog), catalog, externalDbName, backendDbName);
  }

  public String externalDatabaseName(String catalog, String backendDbName) {
    if (backendDbName == null || backendDbName.isBlank()) {
      return catalog.equals(config.defaultCatalog()) ? backendDbName : catalog;
    }
    if (catalog.equals(config.defaultCatalog())) {
      return backendDbName;
    }
    if (isUnprefixedDatabase(catalog, backendDbName)) {
      return backendDbName;
    }
    return catalog + config.catalogDbSeparator() + backendDbName;
  }

  String prefixedCatalog(String dbName) {
    int separator = dbName.indexOf(config.catalogDbSeparator());
    if (separator <= 0) {
      return null;
    }
    String catalog = dbName.substring(0, separator);
    return backends.containsKey(catalog) ? catalog : null;
  }

  private String normalizeExternalDbName(String dbName) {
    if (dbName == null || dbName.isBlank()) {
      return dbName;
    }
    int hash = dbName.indexOf('#');
    if (dbName.startsWith("@") && hash > 1) {
      if (hash + 1 < dbName.length()) {
        String remainder = dbName.substring(hash + 1);
        if (remainder.equals("!")) {
          return "";
        }
        return normalizeExternalDbName(remainder);
      }
      return "";
    }
    for (CatalogPrefix candidate : patternPrefixes) {
      if (dbName.startsWith(candidate.prefix())) {
        String canonicalPrefix = candidate.catalogName() + config.catalogDbSeparator();
        if (!candidate.prefix().equals(canonicalPrefix)) {
          return canonicalPrefix + dbName.substring(candidate.prefix().length());
        }
        return dbName;
      }
    }

    int dot = dbName.indexOf('.');
    if (dot > 0) {
      String remainder = dbName.substring(dot + 1);
      if (looksLikeExternalDbName(remainder)) {
        return normalizeExternalDbName(remainder);
      }
      String prefixedCatalog = dbName.substring(0, dot);
      if (prefixedCatalog.equalsIgnoreCase("hive") || prefixedCatalog.equalsIgnoreCase("spark_catalog")) {
        return normalizeExternalDbName(remainder);
      }
    }
    return dbName;
  }

  public String normalizeDatabasePattern(String dbPattern) {
    if (dbPattern == null || dbPattern.isBlank()) {
      return "*";
    }
    String normalized = normalizeExternalDbName(dbPattern.trim());
    if (normalized == null || normalized.isBlank()) {
      return "*";
    }
    return normalized;
  }

  public boolean canMatchRemoteCatalogs(String dbPattern) {
    if (singleCatalog()) {
      return false;
    }
    String normalized = normalizeDatabasePattern(dbPattern);
    if ("*".equals(normalized) || ".*".equals(normalized) || "%".equals(normalized)) {
      return true;
    }
    for (String unprefixedDb : unprefixedDatabases.keySet()) {
      if (matchesHivePattern(unprefixedDb, normalized)) {
        return true;
      }
    }
    for (String mergedDb : dynamicMergedDatabases.keySet()) {
      if (matchesHivePattern(mergedDb, normalized)) {
        return true;
      }
    }
    if (hasMergeWithDefaultCatalogs()) {
      boolean matchesNonMergedOtherCatalog = false;
      for (CatalogPrefix candidate : patternPrefixes) {
        if (!candidate.catalogName().equals(config.defaultCatalog()) && !isMergeWithDefaultCatalog(candidate.catalogName())) {
          if (normalized.startsWith(candidate.prefix())) {
            matchesNonMergedOtherCatalog = true;
            break;
          }
        }
      }
      if (!matchesNonMergedOtherCatalog) {
        return true;
      }
    }
    for (CatalogPrefix candidate : patternPrefixes) {
      if (candidate.catalogName().equals(config.defaultCatalog())) {
        continue;
      }
      if (normalized.startsWith(candidate.prefix())) {
        return true;
      }
    }
    int star = normalized.indexOf('*');
    int percent = normalized.indexOf('%');
    int wildcardPos;
    if (star >= 0 && percent >= 0) {
      wildcardPos = Math.min(star, percent);
    } else if (star >= 0) {
      wildcardPos = star;
    } else {
      wildcardPos = percent;
    }

    if (wildcardPos >= 0) {
      String prefixBeforeWildcard = normalized.substring(0, wildcardPos);
      if (prefixBeforeWildcard.isEmpty() || prefixBeforeWildcard.equals(".")) {
        return true;
      }
      for (CatalogPrefix candidate : patternPrefixes) {
        if (candidate.catalogName().equals(config.defaultCatalog())) {
          continue;
        }
        if (candidate.prefix().startsWith(prefixBeforeWildcard)
            || prefixBeforeWildcard.startsWith(candidate.prefix())) {
          return true;
        }
      }
    }
    return false;
  }

  public Optional<String> backendDatabasePattern(String catalogName, String dbPattern) {
    String normalized = normalizeDatabasePattern(dbPattern);

    if (catalogName.equals(config.defaultCatalog())) {
      if (!normalized.contains("*") && !normalized.contains("%")) {
        if (unprefixedDatabases.containsKey(normalized.toLowerCase(java.util.Locale.ROOT))) {
          return Optional.empty();
        }
      }
      for (CatalogPrefix candidate : patternPrefixes) {
        if (candidate.catalogName().equals(config.defaultCatalog())) {
          continue;
        }
        if (normalized.startsWith(candidate.prefix())) {
          return Optional.empty();
        }
      }
      return Optional.of(dbPattern == null || dbPattern.isBlank() ? "*" : dbPattern);
    }

    List<CatalogPrefix> thisCatalogPrefixes = new ArrayList<>();
    for (CatalogPrefix candidate : patternPrefixes) {
      if (candidate.catalogName().equals(catalogName)) {
        thisCatalogPrefixes.add(candidate);
      }
    }

    for (CatalogPrefix candidate : thisCatalogPrefixes) {
      if (normalized.startsWith(candidate.prefix())) {
        String remainder = normalized.substring(candidate.prefix().length());
        return Optional.of(remainder.isEmpty() ? "*" : remainder);
      }
    }

    for (CatalogPrefix candidate : patternPrefixes) {
      if (!candidate.catalogName().equals(catalogName) && !candidate.catalogName().equals(config.defaultCatalog())) {
        if (normalized.startsWith(candidate.prefix())) {
          return Optional.empty();
        }
      }
    }

    if (normalized.equals("*") || normalized.equals(".*") || normalized.equals("%")) {
      return Optional.of(dbPattern != null && dbPattern.startsWith("@") ? "*" : dbPattern);
    }

    int star = normalized.indexOf('*');
    int percent = normalized.indexOf('%');
    int wildcardPos;
    if (star >= 0 && percent >= 0) {
      wildcardPos = Math.min(star, percent);
    } else if (star >= 0) {
      wildcardPos = star;
    } else {
      wildcardPos = percent;
    }

    if (wildcardPos >= 0) {
      String prefixBeforeWildcard = normalized.substring(0, wildcardPos);
      if (prefixBeforeWildcard.isEmpty() || prefixBeforeWildcard.equals(".")) {
        return Optional.of(dbPattern != null && dbPattern.startsWith("@") ? normalized : dbPattern);
      }

      boolean matchesCatalogPrefix = false;
      for (CatalogPrefix candidate : thisCatalogPrefixes) {
        if (candidate.prefix().startsWith(prefixBeforeWildcard)) {
          matchesCatalogPrefix = true;
          break;
        }
      }

      if (matchesCatalogPrefix) {
        String remainderAfterWildcard = normalized.substring(wildcardPos + 1);
        if (remainderAfterWildcard.isEmpty() || remainderAfterWildcard.equals("*") || remainderAfterWildcard.equals("%")) {
          return Optional.of("*");
        }
        return Optional.of("*" + remainderAfterWildcard);
      }

      for (CatalogPrefix candidate : patternPrefixes) {
        if (!candidate.catalogName().equals(catalogName) && !candidate.catalogName().equals(config.defaultCatalog())) {
          if (prefixBeforeWildcard.startsWith(candidate.prefix()) || candidate.prefix().startsWith(prefixBeforeWildcard)) {
            return Optional.empty();
          }
        }
      }
    }

    for (Map.Entry<String, String> entry : unprefixedDatabases.entrySet()) {
      if (entry.getValue().equals(catalogName) && matchesHivePattern(entry.getKey(), normalized)) {
        return Optional.of(dbPattern != null && dbPattern.startsWith("@") ? normalized : (dbPattern == null || dbPattern.isBlank() ? "*" : dbPattern));
      }
    }

    for (Map.Entry<String, String> entry : dynamicMergedDatabases.entrySet()) {
      if (entry.getValue().equals(catalogName) && matchesHivePattern(entry.getKey(), normalized)) {
        return Optional.of(dbPattern != null && dbPattern.startsWith("@") ? normalized : (dbPattern == null || dbPattern.isBlank() ? "*" : dbPattern));
      }
    }

    if (isMergeWithDefaultCatalog(catalogName)) {
      return Optional.of(dbPattern != null && dbPattern.startsWith("@") ? normalized : (dbPattern == null || dbPattern.isBlank() ? "*" : dbPattern));
    }

    return Optional.empty();
  }

  public static boolean matchesHivePattern(String value, String hivePattern) {
    if (hivePattern == null || hivePattern.isBlank() || "*".equals(hivePattern) || ".*".equals(hivePattern)) {
      return true;
    }
    if (value == null) {
      return false;
    }
    Pattern compiled = compileHivePattern(hivePattern);
    return compiled.matcher(value).matches();
  }

  static Pattern compileHivePattern(String hivePattern) {
    String[] branches = hivePattern.trim().split("\\|");
    StringBuilder regex = new StringBuilder("(?i)^(");
    for (int b = 0; b < branches.length; b++) {
      if (b > 0) {
        regex.append("|");
      }
      String branch = branches[b];
      for (int i = 0; i < branch.length(); i++) {
        char c = branch.charAt(i);
        if (c == '*' || c == '%') {
          regex.append(".*");
        } else if (c == '?') {
          regex.append(".");
        } else if (c == '.' && i + 1 < branch.length() && branch.charAt(i + 1) == '*') {
          regex.append(".*");
          i++;
        } else if ("()[]{}+^$\\|".indexOf(c) >= 0) {
          regex.append("\\").append(c);
        } else {
          regex.append(c);
        }
      }
    }
    regex.append(")$");
    return Pattern.compile(regex.toString());
  }

  private boolean looksLikeExternalDbName(String dbName) {
    for (CatalogPrefix candidate : patternPrefixes) {
      if (dbName.startsWith(candidate.prefix())) {
        return true;
      }
    }
    return false;
  }

  private static List<CatalogPrefix> buildPatternPrefixes(
      ProxyConfig config,
      Collection<String> catalogNames
  ) {
    List<CatalogPrefix> prefixes = new ArrayList<>();
    String literalSeparator = config.catalogDbSeparator();
    String patternSeparator = literalSeparator.replace('_', '.');
    for (String catalog : catalogNames) {
      String literalPrefix = catalog + literalSeparator;
      prefixes.add(new CatalogPrefix(literalPrefix, catalog));
      String patternPrefix = catalog.replace('_', '.') + patternSeparator;
      if (!patternPrefix.equals(literalPrefix)) {
        prefixes.add(new CatalogPrefix(patternPrefix, catalog));
      }
    }
    prefixes.sort((a, b) -> Integer.compare(b.prefix().length(), a.prefix().length()));
    return Collections.unmodifiableList(prefixes);
  }

  public boolean isUnprefixedDatabase(String catalogName, String dbName) {
    if (catalogName == null || dbName == null || catalogName.equals(config.defaultCatalog())) {
      return false;
    }
    String lowerDb = dbName.toLowerCase(java.util.Locale.ROOT);
    String targetCatalog = unprefixedDatabases.get(lowerDb);
    if (catalogName.equals(targetCatalog)) {
      return true;
    }
    if (isMergeWithDefaultCatalog(catalogName)) {
      if (defaultCatalogDatabases.contains(lowerDb)) {
        return false;
      }
      String mergedCat = dynamicMergedDatabases.get(lowerDb);
      if (catalogName.equals(mergedCat)) {
        return true;
      }
      String resolved = lookupDatabaseCatalogOnDemand(dbName);
      return catalogName.equals(resolved) && !defaultCatalogDatabases.contains(lowerDb);
    }
    return false;
  }

  public boolean isDatabaseShadowed(String catalogName, String dbName) {
    if (catalogName == null || dbName == null || !catalogName.equals(config.defaultCatalog())) {
      return false;
    }
    return unprefixedDatabases.containsKey(dbName.toLowerCase(java.util.Locale.ROOT));
  }

  public boolean hasMergeWithDefaultCatalogs() {
    return mergeWithDefaultCatalogs != null && !mergeWithDefaultCatalogs.isEmpty();
  }

  public boolean isMergeWithDefaultCatalog(String catalogName) {
    return catalogName != null && mergeWithDefaultCatalogs != null && mergeWithDefaultCatalogs.contains(catalogName);
  }

  public void updateDatabases(String catalogName, Collection<String> databases) {
    if (catalogName == null || databases == null) {
      return;
    }
    if (catalogName.equals(config.defaultCatalog())) {
      Set<String> newDefaultDbs = new java.util.HashSet<>();
      for (String db : databases) {
        if (db != null && !db.isBlank()) {
          newDefaultDbs.add(db.toLowerCase(java.util.Locale.ROOT));
        }
      }
      defaultCatalogDatabases.clear();
      defaultCatalogDatabases.addAll(newDefaultDbs);
      for (String defDb : newDefaultDbs) {
        dynamicMergedDatabases.remove(defDb);
        negativeResolutionCache.remove(defDb);
      }
    } else if (isMergeWithDefaultCatalog(catalogName)) {
      for (String db : databases) {
        if (db == null || db.isBlank()) {
          continue;
        }
        String lower = db.toLowerCase(java.util.Locale.ROOT);
        negativeResolutionCache.remove(lower);
        if (!defaultCatalogDatabases.contains(lower)) {
          dynamicMergedDatabases.put(lower, catalogName);
        } else {
          dynamicMergedDatabases.remove(lower);
        }
      }
    }
  }

  public void registerDatabase(String catalogName, String dbName) {
    if (catalogName == null || dbName == null || dbName.isBlank()) {
      return;
    }
    String lower = dbName.toLowerCase(java.util.Locale.ROOT);
    negativeResolutionCache.remove(lower);
    if (catalogName.equals(config.defaultCatalog())) {
      defaultCatalogDatabases.add(lower);
      dynamicMergedDatabases.remove(lower);
    } else if (isMergeWithDefaultCatalog(catalogName)) {
      if (!defaultCatalogDatabases.contains(lower)) {
        dynamicMergedDatabases.put(lower, catalogName);
      }
    }
  }

  public void unregisterDatabase(String catalogName, String dbName) {
    if (catalogName == null || dbName == null || dbName.isBlank()) {
      return;
    }
    String lower = dbName.toLowerCase(java.util.Locale.ROOT);
    negativeResolutionCache.remove(lower);
    if (catalogName.equals(config.defaultCatalog())) {
      defaultCatalogDatabases.remove(lower);
    } else if (isMergeWithDefaultCatalog(catalogName)) {
      dynamicMergedDatabases.remove(lower);
    }
  }

  private String lookupDatabaseCatalogOnDemand(String dbName) {
    if (dbName == null || dbName.isBlank() || !hasMergeWithDefaultCatalogs()) {
      return null;
    }
    String lower = dbName.toLowerCase(java.util.Locale.ROOT);
    Long negativeExpiry = negativeResolutionCache.get(lower);
    long now = System.currentTimeMillis();
    if (negativeExpiry != null && now < negativeExpiry) {
      return null;
    }

    if (backendDatabaseExists(defaultBackend(), dbName)) {
      defaultCatalogDatabases.add(lower);
      dynamicMergedDatabases.remove(lower);
      negativeResolutionCache.remove(lower);
      return config.defaultCatalog();
    }

    for (String remoteCatName : mergeWithDefaultCatalogs) {
      CatalogBackend remoteBackend = backends.get(remoteCatName);
      if (remoteBackend != null && backendDatabaseExists(remoteBackend, dbName)) {
        dynamicMergedDatabases.put(lower, remoteCatName);
        negativeResolutionCache.remove(lower);
        return remoteCatName;
      }
    }

    negativeResolutionCache.put(lower, now + 5000L);
    return null;
  }

  private boolean backendDatabaseExists(CatalogBackend backend, String dbName) {
    if (backend == null) {
      return false;
    }
    try {
      Method getDbMethod = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
          .getMethod("get_database", String.class);
      Object result = backend.invoke(getDbMethod, new Object[]{dbName}, null);
      return result != null;
    } catch (Throwable t) {
      return false;
    }
  }

  private static Set<String> buildMergeWithDefaultCatalogs(ProxyConfig config) {
    Set<String> set = new java.util.LinkedHashSet<>();
    for (Map.Entry<String, CatalogConfig> entry : config.catalogs().entrySet()) {
      if (!entry.getKey().equals(config.defaultCatalog()) && entry.getValue().mergeWithDefault()) {
        set.add(entry.getKey());
      }
    }
    return Collections.unmodifiableSet(set);
  }

  public synchronized void reconfigure(ProxyConfig newConfig) {
    this.config = newConfig;
    this.patternPrefixes = buildPatternPrefixes(newConfig, backends.keySet());
    this.unprefixedDatabases = buildUnprefixedDatabases(newConfig);
    this.mergeWithDefaultCatalogs = buildMergeWithDefaultCatalogs(newConfig);
    this.dynamicMergedDatabases.clear();
    this.defaultCatalogDatabases.clear();
    this.negativeResolutionCache.clear();
  }

  public Map<String, String> unprefixedDatabases() {
    return unprefixedDatabases;
  }

  private static Map<String, String> buildUnprefixedDatabases(ProxyConfig config) {
    Map<String, String> mapping = new LinkedHashMap<>();
    for (Map.Entry<String, CatalogConfig> entry : config.catalogs().entrySet()) {
      String catalog = entry.getKey();
      if (catalog.equals(config.defaultCatalog())) {
        continue;
      }
      for (String db : entry.getValue().unprefixedDatabases()) {
        mapping.put(db.toLowerCase(java.util.Locale.ROOT), catalog);
      }
    }
    return Collections.unmodifiableMap(mapping);
  }

  private record CatalogPrefix(String prefix, String catalogName) {}

  MetaException metaException(String message) {
    return new MetaException(message);
  }

  @Override
  public void close() {
    for (CatalogBackend backend : backends.values()) {
      backend.close();
    }
  }

  public record ResolvedNamespace(
      CatalogBackend backend,
      String catalogName,
      String externalDbName,
      String backendDbName
  ) {
  }
}
