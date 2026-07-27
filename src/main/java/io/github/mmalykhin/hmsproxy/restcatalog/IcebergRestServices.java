package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

/**
 * Prefix -> per-catalog REST service registry. The default catalog keeps the
 * phase-1 federated view (no name translation); every other catalog gets a
 * clean, name-translated view. Built eagerly so a broken configuration fails
 * the proxy start, not the first REST request.
 */
public final class IcebergRestServices implements AutoCloseable {
  private final Map<String, IcebergRestService> byPrefix;
  private final String defaultPrefix;

  private IcebergRestServices(Map<String, IcebergRestService> byPrefix, String defaultPrefix) {
    this.byPrefix = byPrefix;
    this.defaultPrefix = defaultPrefix;
  }

  public static IcebergRestServices open(ProxyConfig config, ThriftHiveMetastore.Iface delegate) {
    Map<String, IcebergRestService> services = new LinkedHashMap<>();
    for (String catalog : config.catalogNames()) {
      CatalogNameTranslation translation = catalog.equals(config.defaultCatalog())
          ? null
          : new CatalogNameTranslation(catalog, config.catalogDbSeparator());
      services.put(catalog, new IcebergRestService(catalog, delegate, translation));
    }
    return new IcebergRestServices(services, config.defaultCatalog());
  }

  public IcebergRestService serviceFor(String prefix) {
    return byPrefix.get(prefix);
  }

  public IcebergRestService byWarehouse(String warehouseOrNull) {
    if (warehouseOrNull == null || warehouseOrNull.isEmpty()) {
      return byPrefix.get(defaultPrefix);
    }
    return byPrefix.get(warehouseOrNull);
  }

  public String defaultPrefix() {
    return defaultPrefix;
  }

  @Override
  public void close() throws IOException {
    for (IcebergRestService service : byPrefix.values()) {
      service.close();
    }
  }
}
