package io.github.mmalykhin.hmsproxy.config.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

public class DistributedCacheInvalidationConfigParserTest {

  private static ProxyConfig loadConfig(String content) throws Exception {
    Path file = Files.createTempFile("hms-proxy-dist-invalidation-test", ".properties");
    try {
      Files.writeString(file, content);
      return ProxyConfigLoader.load(file);
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void parsesDefaultDisabledState() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        catalogs=main
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        """;
    ProxyConfig config = loadConfig(props);
    DistributedCacheInvalidationConfig dci = config.latencyRouting().distributedCacheInvalidation();
    Assert.assertNotNull(dci);
    Assert.assertEquals(DistributedCacheInvalidationMode.NONE, dci.mode());
    Assert.assertFalse(dci.isZooKeeper());
  }

  @Test
  public void parsesExplicitZooKeeperConfiguration() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        catalogs=main
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        routing.cache.distributed-invalidation.mode=ZOOKEEPER
        routing.cache.distributed-invalidation.zookeeper.connect-string=zk1:2181,zk2:2181
        routing.cache.distributed-invalidation.zookeeper.znode=/custom-invalidation
        routing.cache.distributed-invalidation.zookeeper.connection-timeout-ms=20000
        routing.cache.distributed-invalidation.zookeeper.session-timeout-ms=45000
        routing.cache.distributed-invalidation.zookeeper.base-sleep-ms=500
        routing.cache.distributed-invalidation.zookeeper.max-retries=5
        routing.cache.distributed-invalidation.zookeeper.event-retention-ms=600000
        """;
    ProxyConfig config = loadConfig(props);
    DistributedCacheInvalidationConfig dci = config.latencyRouting().distributedCacheInvalidation();
    Assert.assertTrue(dci.isZooKeeper());
    Assert.assertEquals(DistributedCacheInvalidationMode.ZOOKEEPER, dci.mode());
    Assert.assertNotNull(dci.zooKeeper());
    Assert.assertEquals("zk1:2181,zk2:2181", dci.zooKeeper().connectString());
    Assert.assertEquals("/custom-invalidation", dci.zooKeeper().znode());
    Assert.assertEquals(20000, dci.zooKeeper().connectionTimeoutMs());
    Assert.assertEquals(45000, dci.zooKeeper().sessionTimeoutMs());
    Assert.assertEquals(500, dci.zooKeeper().baseSleepMs());
    Assert.assertEquals(5, dci.zooKeeper().maxRetries());
    Assert.assertEquals(600000L, dci.zooKeeper().eventRetentionMs());
  }

  @Test
  public void fallsBackToSyntheticReadLockZkConnectString() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=ZOOKEEPER
        synthetic-read-lock.store.zookeeper.connect-string=shared-zk:2181
        catalogs=main
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        routing.cache.distributed-invalidation.mode=ZOOKEEPER
        """;
    ProxyConfig config = loadConfig(props);
    DistributedCacheInvalidationConfig dci = config.latencyRouting().distributedCacheInvalidation();
    Assert.assertTrue(dci.isZooKeeper());
    Assert.assertEquals("shared-zk:2181", dci.zooKeeper().connectString());
    Assert.assertEquals(DistributedCacheInvalidationZooKeeperConfig.DEFAULT_ZNODE, dci.zooKeeper().znode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsWhenConnectStringMissingForZooKeeperMode() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        catalogs=main
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        routing.cache.distributed-invalidation.mode=ZOOKEEPER
        """;
    loadConfig(props);
  }
}
