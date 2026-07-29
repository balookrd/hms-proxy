package io.github.mmalykhin.hmsproxy.restcatalog;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.hive.HiveCatalog;

/**
 * HiveCatalog that bypasses the standalone HiveMetaStoreClient and dispatches all
 * IMetaStoreClient calls through the proxy's ThriftHiveMetastore.Iface (and therefore
 * through RoutingMetaStoreProxy, federation, observability, etc.).
 *
 * Iceberg's HiveCatalog has no extension point for the client pool, so the private
 * {@code clients} field is replaced via reflection right after {@link #initialize}.
 * This is pinned to a specific Iceberg version (see pom.xml); upgrades must rerun
 * RoutingHiveCatalogTest, which exercises the inject.
 */
public final class RoutingHiveCatalog extends HiveCatalog {
  private static final String CLIENTS_FIELD_NAME = "clients";

  private final IMetaStoreClient client;

  public RoutingHiveCatalog(IMetaStoreClient client, Configuration conf) {
    this.client = Objects.requireNonNull(client, "client");
    setConf(Objects.requireNonNull(conf, "conf"));
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    replaceClientPool();
  }

  private void replaceClientPool() {
    try {
      Field clientsField = HiveCatalog.class.getDeclaredField(CLIENTS_FIELD_NAME);
      clientsField.setAccessible(true);
      Object previous = clientsField.get(this);
      clientsField.set(this, new RoutingClientPool(client));
      closeQuietly(previous);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(
          "Failed to inject RoutingClientPool into HiveCatalog; Iceberg version may be incompatible",
          e);
    }
  }

  private static void closeQuietly(Object resource) {
    if (resource instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception ignored) {
        // Swallow: replaced pool may already be closed or never opened a connection.
      }
    }
  }

  ClientPool<IMetaStoreClient, ?> activeClientPool() {
    try {
      Field clientsField = HiveCatalog.class.getDeclaredField(CLIENTS_FIELD_NAME);
      clientsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      ClientPool<IMetaStoreClient, ?> pool = (ClientPool<IMetaStoreClient, ?>) clientsField.get(this);
      return pool;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException("Cannot read HiveCatalog.clients", e);
    }
  }
}
