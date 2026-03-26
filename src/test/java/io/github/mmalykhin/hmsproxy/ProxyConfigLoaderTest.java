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
      Assert.assertEquals(".", config.catalogDbSeparator());
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

  @Test
  public void loadsImpersonationFlag() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    Path keytab = Files.createTempFile("hms-proxy", ".keytab");
    try {
      Files.writeString(file, """
          security.mode=KERBEROS
          security.server-principal=hive/_HOST@EXAMPLE.COM
          security.keytab=%s
          security.impersonation-enabled=true
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """.formatted(keytab));

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.security().impersonationEnabled());
      Assert.assertTrue(config.catalogs().get("catalog1").impersonationEnabled());
      Assert.assertTrue(config.security().frontDoorConf().isEmpty());
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(keytab);
    }
  }

  @Test
  public void loadsFrontDoorHiveConfOverrides() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          security.mode=KERBEROS
          security.server-principal=hive/_HOST@EXAMPLE.COM
          security.keytab=/tmp/ignored-in-this-test.keytab
          security.front-door-conf.hive.cluster.delegation.token.store.class=org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore
          security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString=zk1:2181,zk2:2181
          security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.znode=/hms-proxy-delegation-tokens
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for unreadable keytab");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("security.keytab"));
      }

      Path keytab = Files.createTempFile("hms-proxy", ".keytab");
      try {
        Files.writeString(file, """
            security.mode=KERBEROS
            security.server-principal=hive/_HOST@EXAMPLE.COM
            security.keytab=%s
            security.front-door-conf.hive.cluster.delegation.token.store.class=org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore
            security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString=zk1:2181,zk2:2181
            security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.znode=/hms-proxy-delegation-tokens
            catalogs=catalog1
            catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
            """.formatted(keytab));

        ProxyConfig config = ProxyConfigLoader.load(file);

        Assert.assertEquals(
            "org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore",
            config.security().frontDoorConf().get("hive.cluster.delegation.token.store.class"));
        Assert.assertEquals(
            "zk1:2181,zk2:2181",
            config.security().frontDoorConf().get(
                "hive.cluster.delegation.token.store.zookeeper.connectString"));
        Assert.assertEquals(
            "/hms-proxy-delegation-tokens",
            config.security().frontDoorConf().get(
                "hive.cluster.delegation.token.store.zookeeper.znode"));
      } finally {
        Files.deleteIfExists(keytab);
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsImpersonationWithoutKerberosFrontDoor() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          security.mode=NONE
          security.impersonation-enabled=true
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for impersonation without Kerberos");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("impersonation"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void catalogLevelImpersonationOverrideCanDisableSingleBackend() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    Path keytab = Files.createTempFile("hms-proxy", ".keytab");
    try {
      Files.writeString(file, """
          security.mode=KERBEROS
          security.server-principal=hive/_HOST@EXAMPLE.COM
          security.keytab=%s
          security.impersonation-enabled=true
          catalogs=catalog1,catalog2
          routing.default-catalog=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog2.impersonation-enabled=false
          catalog.catalog2.conf.hive.metastore.uris=thrift://hms2:9083
          """.formatted(keytab));

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.catalogs().get("catalog1").impersonationEnabled());
      Assert.assertFalse(config.catalogs().get("catalog2").impersonationEnabled());
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(keytab);
    }
  }

  @Test
  public void catalogLevelImpersonationOverrideCanEnableSingleBackend() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    Path keytab = Files.createTempFile("hms-proxy", ".keytab");
    try {
      Files.writeString(file, """
          security.mode=KERBEROS
          security.server-principal=hive/_HOST@EXAMPLE.COM
          security.keytab=%s
          security.impersonation-enabled=false
          catalogs=catalog1,catalog2
          routing.default-catalog=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog2.impersonation-enabled=true
          catalog.catalog2.conf.hive.metastore.uris=thrift://hms2:9083
          """.formatted(keytab));

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertFalse(config.catalogs().get("catalog1").impersonationEnabled());
      Assert.assertTrue(config.catalogs().get("catalog2").impersonationEnabled());
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(keytab);
    }
  }

  @Test
  public void rejectsCatalogLevelImpersonationWithoutKerberosFrontDoor() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          security.mode=NONE
          security.impersonation-enabled=false
          catalogs=catalog1
          catalog.catalog1.impersonation-enabled=true
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for catalog-level impersonation without Kerberos");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("impersonation"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsCustomCatalogDbSeparator() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          routing.catalog-db-separator=__
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals("__", config.catalogDbSeparator());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsBlankCatalogDbSeparator() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          routing.catalog-db-separator=
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for blank routing.catalog-db-separator");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("routing.catalog-db-separator"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
