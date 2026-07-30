package io.github.mmalykhin.hmsproxy.config.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class RestCatalogConfigParserTest {
  private static final String COMMON_PROPS = """
      synthetic-read-lock.store.mode=IN_MEMORY
      catalogs=catalog1
      catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
      """;

  @Test
  public void defaultsToDisabled() throws Exception {
    RestCatalogConfig config = loadRestConfig("server.port=9083\n" + COMMON_PROPS);
    Assert.assertFalse(config.enabled());
    Assert.assertEquals(9183, config.port());
    Assert.assertEquals("0.0.0.0", config.bindHost());
    Assert.assertEquals(8, config.minWorkerThreads());
    Assert.assertEquals(64, config.maxWorkerThreads());
  }

  @Test
  public void enabledImplicitlyWhenPortIsSet() throws Exception {
    RestCatalogConfig config = loadRestConfig(
        "server.port=9083\nrest-catalog.port=8181\n" + COMMON_PROPS);
    Assert.assertTrue(config.enabled());
    Assert.assertEquals(8181, config.port());
  }

  @Test
  public void respectsExplicitlyDisabledEvenWithPortSet() throws Exception {
    RestCatalogConfig config = loadRestConfig(
        "server.port=9083\nrest-catalog.port=8181\nrest-catalog.enabled=false\n" + COMMON_PROPS);
    Assert.assertFalse(config.enabled());
  }

  @Test
  public void inheritsBindHostFromServerByDefault() throws Exception {
    RestCatalogConfig config = loadRestConfig(
        "server.port=9083\nserver.bind-host=127.0.0.1\n" + COMMON_PROPS);
    Assert.assertEquals("127.0.0.1", config.bindHost());
  }

  @Test
  public void allowsBindHostOverride() throws Exception {
    RestCatalogConfig config = loadRestConfig(
        "server.port=9083\nrest-catalog.bind-host=10.0.0.1\nrest-catalog.port=8181\n" + COMMON_PROPS);
    Assert.assertEquals("10.0.0.1", config.bindHost());
  }

  @Test
  public void rejectsInvalidPort() throws Exception {
    try {
      loadRestConfig("server.port=9083\nrest-catalog.port=70000\n" + COMMON_PROPS);
      Assert.fail("expected IllegalArgumentException for port out of range");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("rest-catalog.port"));
    }
  }

  @Test
  public void rejectsMinThreadsBelowOne() throws Exception {
    try {
      loadRestConfig(
          "server.port=9083\nrest-catalog.port=8181\nrest-catalog.min-worker-threads=0\n" + COMMON_PROPS);
      Assert.fail("expected IllegalArgumentException for non-positive min threads");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("rest-catalog.min-worker-threads"));
    }
  }

  @Test
  public void rejectsMaxThreadsLessThanMin() throws Exception {
    try {
      loadRestConfig(
          "server.port=9083\nrest-catalog.port=8181\n"
              + "rest-catalog.min-worker-threads=16\nrest-catalog.max-worker-threads=4\n"
              + COMMON_PROPS);
      Assert.fail("expected IllegalArgumentException for max < min");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("rest-catalog.max-worker-threads"));
      Assert.assertTrue(e.getMessage().contains("rest-catalog.min-worker-threads"));
    }
  }

  @Test
  public void skipsPortValidationWhenDisabled() throws Exception {
    RestCatalogConfig config = loadRestConfig(
        "server.port=9083\nrest-catalog.enabled=false\n" + COMMON_PROPS);
    Assert.assertFalse(config.enabled());
  }

  @Test
  public void purgeModeDefaultsToAllowWithNoPrefixes() throws Exception {
    RestCatalogConfig config = loadRestConfig("server.port=9083\n" + COMMON_PROPS);
    Assert.assertEquals(RestCatalogPurgeMode.ALLOW, config.purgeMode());
    Assert.assertEquals(List.of(), config.purgeAllowedPrefixes());
  }

  @Test
  public void purgeModeIsCaseInsensitiveAndPrefixesAreSplit() throws Exception {
    RestCatalogConfig config = loadRestConfig("server.port=9083\n" + COMMON_PROPS
        + "rest-catalog.purge.mode=allowlist\n"
        + "rest-catalog.purge.allowed-prefixes=hdfs://ns/warehouse/, hdfs://ns/tmp/\n");
    Assert.assertEquals(RestCatalogPurgeMode.ALLOWLIST, config.purgeMode());
    Assert.assertEquals(
        List.of("hdfs://ns/warehouse/", "hdfs://ns/tmp/"), config.purgeAllowedPrefixes());
  }

  @Test
  public void unknownPurgeModeIsRejectedWithTheAcceptedValues() {
    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.mode=BEST_EFFORT\n"));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("rest-catalog.purge.mode"));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("ALLOWLIST"));
  }

  @Test
  public void allowlistModeWithoutPrefixesIsRejected() {
    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.mode=ALLOWLIST\n"));
    Assert.assertTrue(e.getMessage(),
        e.getMessage().contains("rest-catalog.purge.allowed-prefixes"));
  }

  @Test
  public void prefixesWithoutAllowlistModeAreRejected() {
    IllegalArgumentException allow = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.allowed-prefixes=hdfs://ns/warehouse/\n"));
    Assert.assertTrue(allow.getMessage(), allow.getMessage().contains("ALLOWLIST"));
    IllegalArgumentException refuse = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.mode=REFUSE\n"
            + "rest-catalog.purge.allowed-prefixes=hdfs://ns/warehouse/\n"));
    Assert.assertTrue(refuse.getMessage(), refuse.getMessage().contains("ALLOWLIST"));
  }

  private static RestCatalogConfig loadRestConfig(String body) throws Exception {
    Path file = Files.createTempFile("hms-proxy-rest", ".properties");
    try {
      Files.writeString(file, body);
      ProxyConfig config = ProxyConfigLoader.load(file);
      return config.restCatalog();
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
