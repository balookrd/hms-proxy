package io.github.mmalykhin.hmsproxy.config.catalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

public class CatalogCrossDcConfigParserTest {

  private static ProxyConfig loadConfig(String content) throws Exception {
    Path file = Files.createTempFile("hms-proxy-cross-dc-test", ".properties");
    try {
      Files.writeString(file, content);
      return ProxyConfigLoader.load(file);
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void parsesCrossDcDefaults() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        catalogs=main
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        """;
    ProxyConfig config = loadConfig(props);
    CatalogConfig main = config.catalogs().get("main");
    Assert.assertEquals(CatalogStartupMode.STRICT, main.startupMode());
    Assert.assertTrue(main.requiredForReadiness());
    Assert.assertEquals(0, main.maxConcurrentCalls());
    Assert.assertEquals(10000L, main.concurrencyTimeoutMs());
    Assert.assertNull(main.fallbackCatalog());
    Assert.assertFalse(main.fallbackOnOutage());

    Assert.assertEquals(0L, config.latencyRouting().tableMetadataCache().ttlMs());
    Assert.assertEquals(0L, config.latencyRouting().partitionMetadataCache().ttlMs());
    Assert.assertFalse(config.latencyRouting().cacheServeStaleOnError());
    Assert.assertEquals(86400000L, config.latencyRouting().cacheStaleGracePeriodMs());
    Assert.assertTrue(config.management().requireAllCatalogs());
  }

  @Test
  public void parsesCrossDcExplicitSettings() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        routing.default-catalog=main
        catalogs=main,remote,shadow
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        catalog.remote.conf.hive.metastore.uris=thrift://remote-hms:9083
        catalog.remote.startup-mode=LENIENT
        catalog.remote.required-for-readiness=false
        catalog.remote.max-concurrent-calls=15
        catalog.remote.concurrency-timeout-ms=2500
        catalog.remote.fallback-catalog=shadow
        catalog.remote.fallback-on-outage=true
        catalog.shadow.conf.hive.metastore.uris=thrift://shadow-hms:9083
        catalog.shadow.access-mode=READ_ONLY
        routing.table-metadata-cache.enabled=true
        routing.table-metadata-cache.ttl-ms=30000
        routing.table-metadata-cache.max-entries=5000
        routing.partition-metadata-cache.enabled=true
        routing.partition-metadata-cache.ttl-ms=15000
        routing.cache.serve-stale-on-error=true
        routing.cache.stale-grace-period-ms=120000
        management.readyz.require-all-catalogs=false
        """;
    ProxyConfig config = loadConfig(props);
    CatalogConfig remote = config.catalogs().get("remote");
    Assert.assertEquals(CatalogStartupMode.LENIENT, remote.startupMode());
    Assert.assertFalse(remote.requiredForReadiness());
    Assert.assertEquals(15, remote.maxConcurrentCalls());
    Assert.assertEquals(2500L, remote.concurrencyTimeoutMs());
    Assert.assertEquals("shadow", remote.fallbackCatalog());
    Assert.assertTrue(remote.fallbackOnOutage());

    Assert.assertEquals(30000L, config.latencyRouting().tableMetadataCache().ttlMs());
    Assert.assertEquals(5000, config.latencyRouting().tableMetadataCache().maxEntries());

    Assert.assertEquals(15000L, config.latencyRouting().partitionMetadataCache().ttlMs());

    Assert.assertTrue(config.latencyRouting().cacheServeStaleOnError());
    Assert.assertEquals(120000L, config.latencyRouting().cacheStaleGracePeriodMs());
    Assert.assertFalse(config.management().requireAllCatalogs());
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLenientStartupModeOnDefaultCatalog() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        catalogs=main
        catalog.main.startup-mode=LENIENT
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        """;
    loadConfig(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsNotRequiredForReadinessOnDefaultCatalog() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        catalogs=main
        catalog.main.required-for-readiness=false
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        """;
    loadConfig(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsSelfReferencingFallbackCatalog() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        routing.default-catalog=main
        catalogs=main,remote
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        catalog.remote.conf.hive.metastore.uris=thrift://remote:9083
        catalog.remote.fallback-catalog=remote
        """;
    loadConfig(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsUnknownFallbackCatalog() throws Exception {
    String props = """
        synthetic-read-lock.store.mode=IN_MEMORY
        routing.default-catalog=main
        catalogs=main,remote
        catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
        catalog.remote.conf.hive.metastore.uris=thrift://remote:9083
        catalog.remote.fallback-catalog=non_existent_shadow
        """;
    loadConfig(props);
  }
}
