package io.github.mmalykhin.hmsproxy.config.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.nio.file.Files;
import java.nio.file.Path;
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
