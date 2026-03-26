package io.github.mmalykhin.hmsproxy;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class CatalogRouter implements AutoCloseable {
  private final ProxyConfig config;
  private final Map<String, CatalogBackend> backends;

  CatalogRouter(ProxyConfig config, Map<String, CatalogBackend> backends) {
    this.config = config;
    this.backends = backends;
  }

  static CatalogRouter open(ProxyConfig config) throws MetaException {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (Map.Entry<String, ProxyConfig.CatalogConfig> entry : config.catalogs().entrySet()) {
      backends.put(entry.getKey(), CatalogBackend.open(config, entry.getValue()));
    }
    return new CatalogRouter(config, backends);
  }

  Collection<CatalogBackend> backends() {
    return backends.values();
  }

  boolean singleCatalog() {
    return backends.size() == 1;
  }

  CatalogBackend defaultBackend() {
    return requireBackend(config.defaultCatalog());
  }

  CatalogBackend requireBackend(String catalog) {
    if (!backends.containsKey(catalog)) {
      throw new IllegalArgumentException("Unknown catalog: " + catalog);
    }
    return backends.get(catalog);
  }

  Optional<ResolvedNamespace> resolveCatalogIfKnown(String catalog, String backendDbName) {
    if (catalog == null || catalog.isBlank() || !backends.containsKey(catalog)) {
      return Optional.empty();
    }
    return Optional.of(resolveCatalog(catalog, backendDbName));
  }

  ResolvedNamespace resolveDatabase(String dbName) throws MetaException {
    String normalizedDbName = normalizeExternalDbName(dbName);
    if (normalizedDbName == null || normalizedDbName.isBlank()) {
      return resolveCatalog(config.defaultCatalog(), normalizedDbName);
    }

    int separator = normalizedDbName.indexOf(config.catalogDbSeparator());
    if (separator > 0) {
      String catalog = normalizedDbName.substring(0, separator);
      if (backends.containsKey(catalog)) {
        return resolveCatalog(
            catalog,
            normalizedDbName.substring(separator + config.catalogDbSeparator().length()),
            normalizedDbName);
      }
    }

    return resolveCatalog(config.defaultCatalog(), normalizedDbName);
  }

  Optional<ResolvedNamespace> resolvePattern(String dbPattern) {
    String normalizedDbPattern = normalizeExternalDbName(dbPattern);
    if (normalizedDbPattern == null || normalizedDbPattern.isBlank()) {
      return Optional.empty();
    }
    int separator = normalizedDbPattern.indexOf(config.catalogDbSeparator());
    if (separator > 0) {
      String catalog = normalizedDbPattern.substring(0, separator);
      if (backends.containsKey(catalog)) {
        return Optional.of(
            resolveCatalog(
                catalog,
                normalizedDbPattern.substring(separator + config.catalogDbSeparator().length()),
                normalizedDbPattern));
      }
    }
    return Optional.empty();
  }

  ResolvedNamespace resolveCatalog(String catalog, String backendDbName) {
    String effectiveDbName = backendDbName == null || backendDbName.isBlank() ? "" : backendDbName;
    return resolveCatalog(catalog, effectiveDbName, externalDatabaseName(catalog, effectiveDbName));
  }

  private ResolvedNamespace resolveCatalog(String catalog, String backendDbName, String externalDbName) {
    return new ResolvedNamespace(requireBackend(catalog), catalog, externalDbName, backendDbName);
  }

  String externalDatabaseName(String catalog, String backendDbName) {
    if (backendDbName == null || backendDbName.isBlank()) {
      return catalog;
    }
    return catalog + config.catalogDbSeparator() + backendDbName;
  }

  private String normalizeExternalDbName(String dbName) {
    if (dbName == null || dbName.isBlank()) {
      return dbName;
    }
    int separator = dbName.indexOf(config.catalogDbSeparator());
    if (separator > 0 && backends.containsKey(dbName.substring(0, separator))) {
      return dbName;
    }

    int dot = dbName.indexOf('.');
    if (dot > 0) {
      String remainder = dbName.substring(dot + 1);
      if (looksLikeExternalDbName(remainder)) {
        return remainder;
      }
      String prefixedCatalog = dbName.substring(0, dot);
      if (!remainder.isBlank() && !backends.containsKey(prefixedCatalog)) {
        return remainder;
      }
    }
    return dbName;
  }

  private boolean looksLikeExternalDbName(String dbName) {
    int separator = dbName.indexOf(config.catalogDbSeparator());
    return separator > 0 && backends.containsKey(dbName.substring(0, separator));
  }

  MetaException metaException(String message) {
    return new MetaException(message);
  }

  @Override
  public void close() {
    for (CatalogBackend backend : backends.values()) {
      backend.close();
    }
  }

  record ResolvedNamespace(
      CatalogBackend backend,
      String catalogName,
      String externalDbName,
      String backendDbName
  ) {
  }
}
