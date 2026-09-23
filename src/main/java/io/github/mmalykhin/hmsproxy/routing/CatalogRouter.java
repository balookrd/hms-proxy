package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.api.MetaException;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;

public final class CatalogRouter implements AutoCloseable {
  private final ProxyConfig config;
  private final Map<String, CatalogBackend> backends;
  private final List<CatalogPrefix> patternPrefixes;

  CatalogRouter(ProxyConfig config, Map<String, CatalogBackend> backends) {
    this.config = config;
    this.backends = backends;
    this.patternPrefixes = buildPatternPrefixes(config, backends.keySet());
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
      return resolveCatalog(
          prefixedCatalog,
          normalizedDbName.substring(prefixedCatalog.length() + config.catalogDbSeparator().length()),
          normalizedDbName);
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
        String externalDbName = candidate.catalogName() + config.catalogDbSeparator() + remainder;
        return Optional.of(resolveCatalog(candidate.catalogName(), remainder, externalDbName));
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
    return catalog + config.catalogDbSeparator() + backendDbName;
  }

  private String prefixedCatalog(String dbName) {
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
    if (looksLikeExternalDbName(dbName)) {
      return dbName;
    }

    int dot = dbName.indexOf('.');
    if (dot > 0) {
      String remainder = dbName.substring(dot + 1);
      if (looksLikeExternalDbName(remainder)) {
        return remainder;
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
        return Optional.of(dbPattern);
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
        } else if ("()[]{}+^$\\.|".indexOf(c) >= 0) {
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
