package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Field;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class CatalogBackend implements AutoCloseable {
  private final ProxyConfig.CatalogConfig config;
  private final HiveMetaStoreClient client;
  private final ThriftHiveMetastore.Iface thriftClient;
  private final Catalog catalog;

  private CatalogBackend(
      ProxyConfig.CatalogConfig config,
      HiveMetaStoreClient client,
      ThriftHiveMetastore.Iface thriftClient,
      Catalog catalog
  ) {
    this.config = config;
    this.client = client;
    this.thriftClient = thriftClient;
    this.catalog = catalog;
  }

  static CatalogBackend open(ProxyConfig proxyConfig, ProxyConfig.CatalogConfig catalogConfig)
      throws MetaException {
    HiveConf conf = new HiveConf();
    conf.set("hadoop.security.authentication", proxyConfig.security().mode().hadoopAuthValue());
    for (Map.Entry<String, String> entry : catalogConfig.hiveConf().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    ThriftHiveMetastore.Iface thriftClient = extractThriftClient(client);
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    return new CatalogBackend(catalogConfig, client, thriftClient, catalog);
  }

  String name() {
    return config.name();
  }

  Catalog catalog() {
    return new Catalog(catalog);
  }

  ThriftHiveMetastore.Iface thriftClient() {
    return thriftClient;
  }

  @Override
  public void close() {
    client.close();
  }

  // HiveMetaStoreClient implements IMetaStoreClient (high-level API), not ThriftHiveMetastore.Iface
  // (raw Thrift API). The proxy intercepts ThriftHiveMetastore.Iface calls and must forward them to
  // the same interface on each backend. There is no public API to obtain the raw Thrift client from
  // HiveMetaStoreClient, so we use reflection to access the private "client" field.
  // If this breaks after a Hive upgrade, the alternative is to build the Thrift transport directly.
  private static ThriftHiveMetastore.Iface extractThriftClient(HiveMetaStoreClient client)
      throws MetaException {
    try {
      Field field = HiveMetaStoreClient.class.getDeclaredField("client");
      field.setAccessible(true);
      Object value = field.get(client);
      if (!(value instanceof ThriftHiveMetastore.Iface)) {
        throw new MetaException(
            "Unexpected type for HiveMetaStoreClient.client field: "
                + (value == null ? "null" : value.getClass().getName()));
      }
      return (ThriftHiveMetastore.Iface) value;
    } catch (ReflectiveOperationException e) {
      MetaException metaException = new MetaException(
          "Unable to access underlying thrift client via reflection. "
              + "This may happen after a Hive library upgrade that changes HiveMetaStoreClient internals.");
      metaException.initCause(e);
      throw metaException;
    }
  }
}
