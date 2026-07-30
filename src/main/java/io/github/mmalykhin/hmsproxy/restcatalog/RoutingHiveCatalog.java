package io.github.mmalykhin.hmsproxy.restcatalog;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(RoutingHiveCatalog.class);
  private static final String CLIENTS_FIELD_NAME = "clients";

  private final IMetaStoreClient client;
  private final IcebergPurgePolicy purgePolicy;

  public RoutingHiveCatalog(
      IMetaStoreClient client, Configuration conf, IcebergPurgePolicy purgePolicy) {
    this.client = Objects.requireNonNull(client, "client");
    this.purgePolicy = Objects.requireNonNull(purgePolicy, "purgePolicy");
    setConf(Objects.requireNonNull(conf, "conf"));
  }

  /**
   * A purge deletes real files under the proxy's own credentials, so it goes through
   * {@link IcebergPurgePolicy} instead of straight to HiveCatalog. In the default ALLOW mode this
   * delegates untouched. Otherwise the same two steps HiveCatalog performs happen here in the same
   * order - metastore drop first, then the files - with the policy consulted before the drop and
   * the FileIO guarded during the delete.
   */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!purge || purgePolicy.isDefaultBehaviour()) {
      return super.dropTable(identifier, purge);
    }
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    try {
      lastMetadata = ops.current();
    } catch (NotFoundException e) {
      // The metastore entry outlived its metadata file: nothing to walk and nothing to delete.
      lastMetadata = null;
    }
    String refusal = purgePolicy.refusalFor(identifier.toString(), lastMetadata, getConf());
    if (refusal != null) {
      // The client gets the message with its 403, but a refused purge is worth an operator-visible
      // line too: it is the only request the proxy answers that would have destroyed data.
      LOG.warn("refused purge of table '{}': {}", identifier, refusal);
      throw new ForbiddenException("%s", refusal);
    }
    boolean dropped = super.dropTable(identifier, false);
    if (dropped && lastMetadata != null) {
      CatalogUtil.dropTableData(
          purgePolicy.guard(ops.io(), getConf(), identifier.toString()), lastMetadata);
    }
    return dropped;
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
