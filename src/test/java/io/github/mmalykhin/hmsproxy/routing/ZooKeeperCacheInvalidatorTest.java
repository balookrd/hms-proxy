package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.TableMetadataCacheConfig;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

public class ZooKeeperCacheInvalidatorTest {

  private static CuratorFramework createClient(TestingServer server) throws Exception {
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(server.getConnectString())
        .connectionTimeoutMs(15_000)
        .sessionTimeoutMs(60_000)
        .retryPolicy(new ExponentialBackoffRetry(100, 3))
        .build();
    client.start();
    client.blockUntilConnected(10, TimeUnit.SECONDS);
    return client;
  }

  @Test
  public void testMultiInstanceTableInvalidation() throws Throwable {
    TestingServer server = RoutingMetaStoreProxyTestSupport.startTestingServerOrSkip();
    try {
      try (CuratorFramework client1 = createClient(server);
           CuratorFramework client2 = createClient(server)) {

        // Instance 1 caches
        TableMetadataCache tableCache1 = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
        PartitionMetadataCache partCache1 = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
        DatabaseListCache dbListCache1 = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, true));
        DatabaseMetadataCache dbMetaCache1 = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100, true), dbListCache1);
        LocalCacheInvalidator local1 = new LocalCacheInvalidator(dbListCache1, dbMetaCache1, tableCache1, partCache1);

        // Instance 2 caches
        TableMetadataCache tableCache2 = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
        PartitionMetadataCache partCache2 = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
        DatabaseListCache dbListCache2 = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, true));
        DatabaseMetadataCache dbMetaCache2 = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100, true), dbListCache2);
        LocalCacheInvalidator local2 = new LocalCacheInvalidator(dbListCache2, dbMetaCache2, tableCache2, partCache2);

        String znode = "/test-cache-inv-table";
        AtomicLong clock = new AtomicLong(1000L);

        try (ZooKeeperCacheInvalidator inv1 = new ZooKeeperCacheInvalidator(client1, znode, 300_000L, "inst-1", local1, null, clock::get);
             ZooKeeperCacheInvalidator inv2 = new ZooKeeperCacheInvalidator(client2, znode, 300_000L, "inst-2", local2, null, clock::get)) {

          // Populate cache on instance 2
          ImpersonationContext user = new ImpersonationContext("user1", List.of("group1"));
          tableCache2.get("default", "sales", "orders", user, () -> {
            Table t = new Table();
            t.setDbName("sales");
            t.setTableName("orders");
            return t;
          });
          Assert.assertEquals(1, tableCache2.size());

          // Invalidate on instance 1
          inv1.invalidateTable("default", "sales", "orders");

          // Verify that instance 2 received the invalidation via ZooKeeper
          boolean invalidatedOn2 = false;
          for (int i = 0; i < 50; i++) {
            if (tableCache2.size() == 0) {
              invalidatedOn2 = true;
              break;
            }
            Thread.sleep(100);
          }
          Assert.assertTrue("Instance 2 should have invalidated table cache within timeout", invalidatedOn2);
        }
      }
    } finally {
      server.close();
    }
  }

  @Test
  public void testMultiInstanceDatabaseInvalidation() throws Throwable {
    TestingServer server = RoutingMetaStoreProxyTestSupport.startTestingServerOrSkip();
    try {
      try (CuratorFramework client1 = createClient(server);
           CuratorFramework client2 = createClient(server)) {

        // Instance 1
        LocalCacheInvalidator local1 = new LocalCacheInvalidator(
            new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, true)),
            new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100, true)),
            new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true)),
            new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true)));

        // Instance 2
        DatabaseListCache dbListCache2 = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, true));
        DatabaseMetadataCache dbMetaCache2 = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100, true), dbListCache2);
        TableMetadataCache tableCache2 = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
        PartitionMetadataCache partCache2 = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
        LocalCacheInvalidator local2 = new LocalCacheInvalidator(dbListCache2, dbMetaCache2, tableCache2, partCache2);

        String znode = "/test-cache-inv-db";
        AtomicLong clock = new AtomicLong(1000L);

        try (ZooKeeperCacheInvalidator inv1 = new ZooKeeperCacheInvalidator(client1, znode, 300_000L, "inst-1", local1, null, clock::get);
             ZooKeeperCacheInvalidator inv2 = new ZooKeeperCacheInvalidator(client2, znode, 300_000L, "inst-2", local2, null, clock::get)) {

          ImpersonationContext user = new ImpersonationContext("user1", List.of("group1"));
          dbMetaCache2.get("default", "sales", user, () -> {
            Database db = new Database();
            db.setName("sales");
            return db;
          });
          tableCache2.get("default", "sales", "orders", user, () -> {
            Table t = new Table();
            t.setDbName("sales");
            t.setTableName("orders");
            return t;
          });

          Assert.assertEquals(1, dbMetaCache2.size());
          Assert.assertEquals(1, tableCache2.size());

          // Invalidate database on instance 1
          inv1.invalidateDatabase("default", "sales");

          // Verify that instance 2 received the invalidation
          boolean invalidated = false;
          for (int i = 0; i < 50; i++) {
            if (dbMetaCache2.size() == 0 && tableCache2.size() == 0) {
              invalidated = true;
              break;
            }
            Thread.sleep(100);
          }
          Assert.assertTrue("Instance 2 should have invalidated database and tables within timeout", invalidated);
        }
      }
    } finally {
      server.close();
    }
  }

  @Test
  public void testPurgeExpiredEvents() throws Exception {
    TestingServer server = RoutingMetaStoreProxyTestSupport.startTestingServerOrSkip();
    try {
      try (CuratorFramework client = createClient(server)) {
        LocalCacheInvalidator local = new LocalCacheInvalidator(null, null, null, null);
        String znode = "/test-cache-inv-purge";
        long now = System.currentTimeMillis();
        AtomicLong clock = new AtomicLong(now);
        long retentionMs = 100L;

        try (ZooKeeperCacheInvalidator inv = new ZooKeeperCacheInvalidator(client, znode, retentionMs, "inst-1", local, null, clock::get)) {
          // Publish an event
          inv.invalidateTable("default", "db1", "tbl1");

          String eventsPath = znode + "/events";
          List<String> children = client.getChildren().forPath(eventsPath);
          Assert.assertEquals(1, children.size());

          // Advance clock past retention relative to ZK ctime
          clock.set(System.currentTimeMillis() + 50_000L);

          inv.purgeExpiredEvents();

          // Check that expired event node was deleted
          List<String> remaining = client.getChildren().forPath(eventsPath);
          Assert.assertEquals(0, remaining.size());
        }
      }
    } finally {
      server.close();
    }
  }
}
