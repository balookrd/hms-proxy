package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
      Assert.assertNull(config.catalogs().get("catalog1").runtimeProfile());
      Assert.assertFalse(config.management().enabled());
      Assert.assertEquals(10088, config.management().port());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void enablesManagementListenerWhenPortIsConfigured() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          server.bind-host=127.0.0.2
          catalogs=catalog1
          management.port=19083
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.management().enabled());
      Assert.assertEquals("127.0.0.2", config.management().bindHost());
      Assert.assertEquals(19083, config.management().port());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsGlobalBackendHiveConfAndMergesCatalogOverrides() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1,catalog2
          routing.default-catalog=catalog1
          backend.conf.hive.metastore.uris=thrift://shared:9083
          backend.conf.hive.metastore.client.socket.timeout=45s
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.catalog2.conf.hive.metastore.connect.retries=7
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals("thrift://shared:9083", config.backend().hiveConf().get("hive.metastore.uris"));
      Assert.assertEquals("45s", config.backend().hiveConf().get("hive.metastore.client.socket.timeout"));
      Assert.assertEquals("thrift://hms1:9083",
          config.catalogs().get("catalog1").hiveConf().get("hive.metastore.uris"));
      Assert.assertEquals("thrift://shared:9083",
          config.catalogs().get("catalog2").hiveConf().get("hive.metastore.uris"));
      Assert.assertEquals("45s",
          config.catalogs().get("catalog2").hiveConf().get("hive.metastore.client.socket.timeout"));
      Assert.assertEquals("7",
          config.catalogs().get("catalog2").hiveConf().get("hive.metastore.connect.retries"));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsTransactionalDdlGuardConfiguration() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          guard.transactional-ddl.mode=reject
          guard.transactional-ddl.client-addresses=10.10.0.0/16,192.168.1.20
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.transactionalDdlGuard().enabled());
      Assert.assertEquals(ProxyConfig.TransactionalDdlGuardMode.REJECT, config.transactionalDdlGuard().mode());
      Assert.assertEquals(List.of("10.10.0.0/16", "192.168.1.20"),
          config.transactionalDdlGuard().clientAddressRules());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void allowsTransactionalDdlGuardWithoutClientAddresses() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          guard.transactional-ddl.mode=reject
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.transactionalDdlGuard().enabled());
      Assert.assertEquals(ProxyConfig.TransactionalDdlGuardMode.REJECT, config.transactionalDdlGuard().mode());
      Assert.assertEquals(List.of(), config.transactionalDdlGuard().clientAddressRules());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsTransactionalDdlRewriteConfiguration() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          guard.transactional-ddl.mode=rewrite
          guard.transactional-ddl.client-addresses=10.10.0.0/16
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(ProxyConfig.TransactionalDdlGuardMode.REWRITE, config.transactionalDdlGuard().mode());
      Assert.assertEquals(List.of("10.10.0.0/16"), config.transactionalDdlGuard().clientAddressRules());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsInvalidTransactionalDdlMode() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          guard.transactional-ddl.mode=unexpected
          """);
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for invalid transactional DDL mode");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("guard.transactional-ddl.mode"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsInvalidTransactionalDdlClientAddressRule() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          guard.transactional-ddl.mode=reject
          guard.transactional-ddl.client-addresses=10.10.0.0/99
          """);
      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for invalid client address rule");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("client address rule"));
      }
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
  public void allowsGlobalBackendMetastoreUrisWithoutPerCatalogUris() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          backend.conf.hive.metastore.uris=thrift://shared:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals("thrift://shared:9083",
          config.catalogs().get("catalog1").hiveConf().get("hive.metastore.uris"));
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

  @Test
  public void loadsCompatibilityFlagForPreservingBackendCatalogName() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          compatibility.preserve-backend-catalog-name=true
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertTrue(config.compatibility().preserveBackendCatalogName());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsFrontendCompatibilityProfile() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
          compatibility.frontend-standalone-metastore-jar=/tmp/hdp-standalone-metastore.jar
          compatibility.backend-standalone-metastore-jar=/tmp/backend-standalone-metastore.jar
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(
          ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
          config.compatibility().frontendProfile());
      Assert.assertEquals(
          "/tmp/hdp-standalone-metastore.jar",
          config.compatibility().frontendStandaloneMetastoreJar());
      Assert.assertEquals(
          "/tmp/backend-standalone-metastore.jar",
          config.compatibility().backendStandaloneMetastoreJar());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsNewerFrontendCompatibilityProfile() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
          compatibility.frontend-standalone-metastore-jar=/tmp/hdp-6150-standalone-metastore.jar
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(
          ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
          config.compatibility().frontendProfile());
      Assert.assertEquals(
          "/tmp/hdp-6150-standalone-metastore.jar",
          config.compatibility().frontendStandaloneMetastoreJar());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsPerCatalogBackendRuntimeOverrides() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=hdp,apache
          routing.default-catalog=hdp
          compatibility.backend-standalone-metastore-jar=/tmp/global-backend.jar
          catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_0_78
          catalog.hdp.backend-standalone-metastore-jar=/tmp/hdp-backend.jar
          catalog.hdp.conf.hive.metastore.uris=thrift://hdp:9083
          catalog.apache.runtime-profile=APACHE_3_1_3
          catalog.apache.conf.hive.metastore.uris=thrift://apache:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(
          MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
          config.catalogs().get("hdp").runtimeProfile());
      Assert.assertEquals(
          "/tmp/hdp-backend.jar",
          config.catalogs().get("hdp").backendStandaloneMetastoreJar());
      Assert.assertEquals(
          MetastoreRuntimeProfile.APACHE_3_1_3,
          config.catalogs().get("apache").runtimeProfile());
      Assert.assertNull(config.catalogs().get("apache").backendStandaloneMetastoreJar());
      Assert.assertEquals(
          "/tmp/global-backend.jar",
          config.compatibility().backendStandaloneMetastoreJar());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void loadsPerCatalogAccessModeAndWriteDbWhitelist() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.access-mode=READ_WRITE_DB_WHITELIST
          catalog.catalog1.write-db-whitelist=sales,analytics
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(
          ProxyConfig.CatalogAccessMode.READ_WRITE_DB_WHITELIST,
          config.catalogs().get("catalog1").accessMode());
      Assert.assertEquals(List.of("sales", "analytics"), config.catalogs().get("catalog1").writeDbWhitelist());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void defaultsCatalogAccessModeToReadWrite() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);

      Assert.assertEquals(ProxyConfig.CatalogAccessMode.READ_WRITE, config.catalogs().get("catalog1").accessMode());
      Assert.assertEquals(List.of(), config.catalogs().get("catalog1").writeDbWhitelist());
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void rejectsInvalidCatalogAccessMode() throws Exception {
    Path file = Files.createTempFile("hms-proxy", ".properties");
    try {
      Files.writeString(file, """
          catalogs=catalog1
          catalog.catalog1.access-mode=unexpected
          catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
          """);

      try {
        ProxyConfigLoader.load(file);
        Assert.fail("Expected IllegalArgumentException for invalid catalog access mode");
      } catch (IllegalArgumentException e) {
        Assert.assertTrue(e.getMessage().contains("catalog.<name>.access-mode"));
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
