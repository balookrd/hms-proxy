package io.github.mmalykhin.hmsproxy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

public class ProxyConfigLoaderTest {
  @Test
  public void loadsCatalogsAndDefaultRouting() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          server.port=9088
          catalogs=catalog1,catalog2
          routing.default-catalog=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog2.conf.hive.metastore.uris=thrift://hms2:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(9088, config.server().port());
      Assert.assertEquals("catalog1", config.defaultCatalog());
      Assert.assertEquals(2, config.catalogs().size());
      Assert.assertEquals("thrift://hms2:9083",
          config.catalogs().get("catalog2").hiveConf().get("hive.metastore.uris"));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsMissingRequiredProperty() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, "server.port=9083\n");
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for missing 'catalogs'");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("catalogs"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsInvalidPort() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          server.port=99999
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for invalid port");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("server.port"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsInvalidThreadPoolConfiguration() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          server.min-worker-threads=100
          server.max-worker-threads=10
          """);
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for min > max threads");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("max-worker-threads"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsMissingDefaultCatalogWithMultipleCatalogs() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1,catalog2
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog2.conf.hive.metastore.uris=thrift://hms2:9083
          """);
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for missing routing.default-catalog");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("routing.default-catalog"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsMissingMetastoreUris() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.description=My catalog
          """);
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for missing hive.metastore.uris");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("hive.metastore.uris"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void defaultsToSingleCatalogWhenNoDefaultCatalogSet() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=only
          catalog.only.conf.hive.metastore.uris=thrift://hms:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals("only", config.defaultCatalog());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void defaultsClientCredentialsToServerCredentialsForKerberizedBackends() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    Path keytab = Files.createTempFile("hms-proxy", ".keytab");
    try {
      Files.writeString(file, """
          security.mode=KERBEROS
          security.server-principal=hive/_HOST@EXAMPLE.COM
          security.keytab=%s
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog1.conf.hive.metastore.sasl.enabled=true
          """.formatted(keytab));

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals("hive/_HOST@EXAMPLE.COM", config.security().clientPrincipal());
      Assert.assertEquals(keytab.toString(), config.security().clientKeytab());
      Assert.assertEquals("hive/_HOST@EXAMPLE.COM", config.security().outboundPrincipal());
      Assert.assertEquals(keytab.toString(), config.security().outboundKeytab());
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(keytab);
    }
  }

  @Test
  public void allowsDedicatedBackendKeytabWhenFrontDoorIsSimple() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    Path clientKeytab = Files.createTempFile("hms-proxy-client", ".keytab");
    try {
      Files.writeString(file, """
          security.mode=NONE
          security.client-principal=hive-metastore/_HOST@EXAMPLE.COM
          security.client-keytab=%s
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog1.conf.hive.metastore.sasl.enabled=true
          """.formatted(clientKeytab));

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals("hive-metastore/_HOST@EXAMPLE.COM", config.security().outboundPrincipal());
      Assert.assertEquals(clientKeytab.toString(), config.security().outboundKeytab());
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(clientKeytab);
    }
  }

  @Test
  public void rejectsMissingClientKeytabForKerberizedBackend() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          security.mode=NONE
          security.client-principal=hive-metastore/_HOST@EXAMPLE.COM
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog1.conf.hive.metastore.sasl.enabled=true
          """);

      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for missing security.client-keytab");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("security.client-keytab"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
