package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.iceberg.hadoop.ConfigProperties;

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

  /**
   * @param catalogForExternalDb resolves an external database name (federated form, e.g.
   *     "apache__default") to the name of the catalog that actually owns it - the same
   *     resolution {@code CatalogRouter.resolveDatabase(...).catalogName()} performs. Consulted
   *     by each service's {@link WriteRouteGate} so a write is refused whenever the namespace it
   *     targets resolves to any catalog other than {@code config.defaultCatalog()}, regardless of
   *     which prefix the request arrived under.
   */
  public static IcebergRestServices open(
      ProxyConfig config, ThriftHiveMetastore.Iface delegate, Function<String, String> catalogForExternalDb) {
    return open(config, delegate, catalogForExternalDb, catalog -> new Configuration());
  }

  /**
   * @param hadoopConfForCatalog supplies the Hadoop {@link Configuration} each catalog's REST
   *     service uses to reach its warehouse filesystem. Production wiring passes {@code
   *     catalog -> router.requireBackend(catalog).hiveConf()} - the same Configuration the
   *     Thrift path already built for that catalog - so REST writes see the same
   *     fs.defaultFS/Kerberos settings instead of a second, independently-built (and
   *     Kerberos-blind) Configuration.
   */
  public static IcebergRestServices open(
      ProxyConfig config, ThriftHiveMetastore.Iface delegate, Function<String, String> catalogForExternalDb,
      Function<String, Configuration> hadoopConfForCatalog) {
    Map<String, Configuration> confByCatalog = new LinkedHashMap<>();
    for (String catalog : config.catalogNames()) {
      // A copy, never the backend's own Configuration: in production that object is
      // router.requireBackend(catalog).hiveConf(), shared with the Thrift path, and writing to it
      // would change behaviour far outside the REST front door.
      Configuration catalogConf = new Configuration(hadoopConfForCatalog.apply(catalog));
      // Only fill in the default: an explicit catalog.<name>.conf.iceberg.engine.hive.enabled
      // override is an operator choice for that one catalog and must survive the proxy-wide
      // default, not get silently replaced by it.
      if (config.restCatalog().hiveEngineDescriptor()
          && catalogConf.get(ConfigProperties.ENGINE_HIVE_ENABLED) == null) {
        catalogConf.setBoolean(ConfigProperties.ENGINE_HIVE_ENABLED, true);
      }
      confByCatalog.put(catalog, catalogConf);
    }
    List<URI> defaultFederatedReadFsUris =
        collectFederatedReadFsUris(config.defaultCatalog(), confByCatalog);
    Map<String, IcebergRestService> services = new LinkedHashMap<>();
    IcebergPurgePolicy purgePolicy = new IcebergPurgePolicy(
        config.restCatalog().purgeMode(), config.restCatalog().purgeAllowedPrefixes());
    for (String catalog : config.catalogNames()) {
      CatalogNameTranslation translation = catalog.equals(config.defaultCatalog())
          ? null
          : new CatalogNameTranslation(catalog, config.catalogDbSeparator());
      List<URI> federatedReadFsUris = catalog.equals(config.defaultCatalog())
          ? defaultFederatedReadFsUris
          : List.of();
      services.put(catalog,
          new IcebergRestService(
              catalog,
              delegate,
              translation,
              config.defaultCatalog(),
              catalogForExternalDb,
              confByCatalog.get(catalog),
              purgePolicy,
              config.restCatalog().fileIoAllowedPrefixes(),
              config.restCatalog().maxMetadataFileBytes(),
              federatedReadFsUris));
    }
    return new IcebergRestServices(services, config.defaultCatalog());
  }

  private static List<URI> collectFederatedReadFsUris(
      String defaultCatalog, Map<String, Configuration> confByCatalog) {
    Configuration defaultConf = confByCatalog.get(defaultCatalog);
    if (defaultConf == null) {
      return List.of();
    }
    URI defaultFsUri = FileSystem.getDefaultUri(defaultConf);
    boolean defaultIsLocalFile = "file".equalsIgnoreCase(defaultFsUri.getScheme());
    Set<URI> result = new LinkedHashSet<>();
    for (Map.Entry<String, Configuration> entry : confByCatalog.entrySet()) {
      if (entry.getKey().equals(defaultCatalog)) {
        continue;
      }
      URI otherUri = FileSystem.getDefaultUri(entry.getValue());
      if (otherUri == null || otherUri.getScheme() == null) {
        continue;
      }
      // Never grant local file: read access on the default catalog just because another catalog
      // had no fs.defaultFS configured when the default catalog itself runs on a remote filesystem.
      if ("file".equalsIgnoreCase(otherUri.getScheme()) && !defaultIsLocalFile) {
        continue;
      }
      result.add(otherUri);
    }
    return List.copyOf(result);
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
    IOException first = null;
    for (IcebergRestService service : byPrefix.values()) {
      try {
        service.close();
      } catch (IOException e) {
        if (first == null) {
          first = e;
        } else {
          first.addSuppressed(e);
        }
      }
    }
    if (first != null) {
      throw first;
    }
  }
}
