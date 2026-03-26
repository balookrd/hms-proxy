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

  ResolvedNamespace resolveDatabase(String dbName) throws MetaException {
    if (dbName == null || dbName.isBlank()) {
      return resolveCatalog(config.defaultCatalog(), dbName);
    }

    int separator = dbName.indexOf(config.catalogDbSeparator());
    if (separator > 0) {
      String catalog = dbName.substring(0, separator);
      if (backends.containsKey(catalog)) {
        return resolveCatalog(catalog, dbName.substring(separator + config.catalogDbSeparator().length()), dbName);
      }
    }

    return resolveCatalog(config.defaultCatalog(), dbName);
  }

  Optional<ResolvedNamespace> resolvePattern(String dbPattern) {
    if (dbPattern == null || dbPattern.isBlank()) {
      return Optional.empty();
    }
    int separator = dbPattern.indexOf(config.catalogDbSeparator());
    if (separator > 0) {
      String catalog = dbPattern.substring(0, separator);
      if (backends.containsKey(catalog)) {
        return Optional.of(
            resolveCatalog(catalog, dbPattern.substring(separator + config.catalogDbSeparator().length()), dbPattern));
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
