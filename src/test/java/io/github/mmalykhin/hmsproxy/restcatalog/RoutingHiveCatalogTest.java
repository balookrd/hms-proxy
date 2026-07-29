package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.catalog.Namespace;
import org.junit.Assert;
import org.junit.Test;

public class RoutingHiveCatalogTest {
  @Test
  public void reflectionInjectReplacesClientPool() {
    RecordingThriftIface delegate = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);
    RoutingHiveCatalog catalog = new RoutingHiveCatalog(client, new Configuration());

    catalog.initialize("test", Map.of());

    ClientPool<IMetaStoreClient, ?> activePool = catalog.activeClientPool();
    Assert.assertTrue(
        "Expected RoutingClientPool after initialize, got: " + activePool.getClass().getName(),
        activePool instanceof RoutingClientPool);
  }

  @Test
  public void namespaceExistsRoutesThroughDelegate() {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.databases.put("sales", RecordingThriftIface.database("sales"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);
    RoutingHiveCatalog catalog = new RoutingHiveCatalog(client, new Configuration());
    catalog.initialize("test", Map.of());

    boolean exists = catalog.namespaceExists(Namespace.of("sales"));

    Assert.assertTrue(exists);
    Assert.assertTrue("expected get_database call, recorded: " + delegate.calls,
        delegate.calls.stream().anyMatch(c -> c.startsWith("get_database:sales")));
  }

  @Test
  public void loadNamespaceMetadataRoutesThroughDelegate() {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.databases.put("sales", RecordingThriftIface.database("sales"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);
    RoutingHiveCatalog catalog = new RoutingHiveCatalog(client, new Configuration());
    catalog.initialize("test", Map.of());

    Map<String, String> metadata = catalog.loadNamespaceMetadata(Namespace.of("sales"));

    Assert.assertNotNull(metadata);
    Assert.assertTrue("expected get_database call, recorded: " + delegate.calls,
        delegate.calls.stream().anyMatch(c -> c.startsWith("get_database:sales")));
  }
}
