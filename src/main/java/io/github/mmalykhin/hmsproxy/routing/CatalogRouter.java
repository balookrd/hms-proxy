package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.MetaException;

public final class CatalogRouter implements AutoCloseable {
  private final ProxyConfig config;
  private final Map<String, CatalogBackend> backends;

  CatalogRouter(ProxyConfig config, Map<String, CatalogBackend> backends) {
    this.config = config;
    this.backends = backends;
  }

  public static CatalogRouter open(ProxyConfig config) throws MetaException {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    try {
      for (Map.Entry<String, ProxyConfig.CatalogConfig> entry : config.catalogs().entrySet()) {
        backends.put(entry.getKey(), CatalogBackend.open(config, entry.getValue()));
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
    String prefixedCatalog = prefixedCatalog(normalizedDbPattern);
    if (prefixedCatalog != null) {
      return Optional.of(
          resolveCatalog(
              prefixedCatalog,
              normalizedDbPattern.substring(prefixedCatalog.length() + config.catalogDbSeparator().length()),
              normalizedDbPattern));
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
    if (dbName.startsWith("@") && hash > 1 && hash + 1 < dbName.length()) {
      return normalizeExternalDbName(dbName.substring(hash + 1));
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

  public record ResolvedNamespace(
      CatalogBackend backend,
      String catalogName,
      String externalDbName,
      String backendDbName
  ) {
  }
}
