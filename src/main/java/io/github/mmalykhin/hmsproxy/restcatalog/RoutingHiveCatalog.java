package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogConfig;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.List;
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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HiveCatalog that bypasses the standalone HiveMetaStoreClient and dispatches all
 * IMetaStoreClient calls through the proxy's ThriftHiveMetastore.Iface (and therefore
 * through RoutingMetaStoreProxy, federation, observability, etc.).
 *
 * Iceberg's HiveCatalog has no extension point for the client pool or FileIO, so the private
 * {@code clients} and {@code fileIO} fields are replaced via reflection right after
 * {@link #initialize}. This is pinned to a specific Iceberg version (see pom.xml); upgrades must
 * rerun RoutingHiveCatalogTest, which exercises both injects.
 */
public final class RoutingHiveCatalog extends HiveCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingHiveCatalog.class);
  private static final String CLIENTS_FIELD_NAME = "clients";
  private static final String FILE_IO_FIELD_NAME = "fileIO";

  private final IMetaStoreClient client;
  private final IcebergPurgePolicy purgePolicy;
  private final RestFileIoPathGuard pathGuard;
  private final long maxMetadataFileBytes;
  private FileIO rawFileIO;

  public RoutingHiveCatalog(
      IMetaStoreClient client, Configuration conf, IcebergPurgePolicy purgePolicy) {
    this(
        client,
        conf,
        purgePolicy,
        List.of(),
        RestCatalogConfig.DEFAULT_MAX_METADATA_FILE_BYTES);
  }

  RoutingHiveCatalog(
      IMetaStoreClient client,
      Configuration conf,
      IcebergPurgePolicy purgePolicy,
      List<String> fileIoAllowedPrefixes,
      long maxMetadataFileBytes
  ) {
    this(client, conf, purgePolicy, fileIoAllowedPrefixes, maxMetadataFileBytes, List.of());
  }

  RoutingHiveCatalog(
      IMetaStoreClient client,
      Configuration conf,
      IcebergPurgePolicy purgePolicy,
      List<String> fileIoAllowedPrefixes,
      long maxMetadataFileBytes,
      List<URI> federatedReadFsUris
  ) {
    this.client = Objects.requireNonNull(client, "client");
    this.purgePolicy = Objects.requireNonNull(purgePolicy, "purgePolicy");
    Configuration nonNullConf = Objects.requireNonNull(conf, "conf");
    setConf(nonNullConf);
    this.pathGuard = new RestFileIoPathGuard(
        nonNullConf, fileIoAllowedPrefixes, federatedReadFsUris);
    this.maxMetadataFileBytes = maxMetadataFileBytes;
  }

  RestFileIoPathGuard pathGuard() {
    return pathGuard;
  }

  /**
   * A purge deletes real files under the proxy's own credentials, so it goes through
   * {@link IcebergPurgePolicy} instead of straight to HiveCatalog. In the default ALLOW mode this
   * delegates untouched (backed by the injected {@link GuardedFileIO}). Otherwise the same two steps
   * HiveCatalog performs happen here in the same order - metastore drop first, then the files -
   * with the policy consulted before the drop and {@link PrefixGuardedFileIO} filtering deletes.
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
    String refusal = purgePolicy.refusalFor(identifier.toString(), lastMetadata, pathGuard);
    if (refusal != null) {
      // The client gets the message with its 403, but a refused purge is worth an operator-visible
      // line too: it is the only request the proxy answers that would have destroyed data.
      LOG.warn("refused purge of table '{}': {}", identifier, refusal);
      throw new ForbiddenException("%s", refusal);
    }
    boolean dropped = super.dropTable(identifier, false);
    if (dropped && lastMetadata != null) {
      FileIO baseIo = rawFileIO != null ? rawFileIO : ops.io();
      CatalogUtil.dropTableData(
          purgePolicy.guard(baseIo, pathGuard, identifier.toString()), lastMetadata);
    }
    return dropped;
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    if (!purgePolicy.isDefaultBehaviour()) {
      ViewOperations ops = newViewOps(identifier);
      ViewMetadata lastMetadata;
      try {
        lastMetadata = ops.current();
      } catch (NotFoundException e) {
        lastMetadata = null;
      }
      String refusal = purgePolicy.refusalForView(identifier.toString(), lastMetadata, pathGuard);
      if (refusal != null) {
        LOG.warn("refused drop of view '{}': {}", identifier, refusal);
        throw new ForbiddenException("%s", refusal);
      }
    }
    return super.dropView(identifier);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    replaceClientPool();
    replaceFileIO();
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

  private void replaceFileIO() {
    try {
      Field fileIoField = HiveCatalog.class.getDeclaredField(FILE_IO_FIELD_NAME);
      fileIoField.setAccessible(true);
      Object existing = fileIoField.get(this);
      if (!(existing instanceof FileIO delegate)) {
        throw new IllegalStateException(
            "Expected HiveCatalog.fileIO to be a FileIO, got: "
                + (existing == null ? "null" : existing.getClass().getName()));
      }
      if (delegate instanceof GuardedFileIO) {
        return;
      }
      this.rawFileIO = delegate;
      fileIoField.set(this, new GuardedFileIO(delegate, pathGuard, purgePolicy, maxMetadataFileBytes));
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(
          "Failed to inject GuardedFileIO into HiveCatalog; Iceberg version may be incompatible",
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

  FileIO activeFileIO() {
    try {
      Field fileIoField = HiveCatalog.class.getDeclaredField(FILE_IO_FIELD_NAME);
      fileIoField.setAccessible(true);
      return (FileIO) fileIoField.get(this);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException("Cannot read HiveCatalog.fileIO", e);
    }
  }
}
