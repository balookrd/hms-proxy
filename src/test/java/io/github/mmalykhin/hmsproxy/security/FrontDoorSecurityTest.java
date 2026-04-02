package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Assert;
import org.junit.Test;

public class FrontDoorSecurityTest {
  @Test
  public void zooKeeperTokenStoreUsesFrontDoorKerberosCredentialsByDefault() {
    ProxyConfig config = TestConfigFactory.kerberosConfig();
    HiveConf conf = new HiveConf();
    conf.set("hive.cluster.delegation.token.store.class",
        "org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore");

    FrontDoorSecurity.applyZooKeeperKerberosDefaults(config, conf);

    Assert.assertEquals(
        "hive/proxy-host.example.com@EXAMPLE.COM",
        conf.get("hive.metastore.kerberos.principal"));
    Assert.assertEquals(
        "/etc/security/keytabs/hms-proxy.keytab",
        conf.get("hive.metastore.kerberos.keytab.file"));
  }

  @Test
  public void nonZooKeeperTokenStoreDoesNotInjectMetastoreKerberosSettings() {
    ProxyConfig config = TestConfigFactory.kerberosConfig();
    Configuration conf = new Configuration(false);
    conf.set("hive.cluster.delegation.token.store.class",
        "org.apache.hadoop.hive.metastore.security.DBTokenStore");

    FrontDoorSecurity.applyZooKeeperKerberosDefaults(config, conf);

    Assert.assertNull(conf.get("hive.metastore.kerberos.principal"));
    Assert.assertNull(conf.get("hive.metastore.kerberos.keytab.file"));
  }

  @Test
  public void explicitFrontDoorMetastoreKerberosOverridesWin() {
    ProxyConfig config = TestConfigFactory.kerberosConfig(java.util.Map.of(
        "hive.metastore.kerberos.principal", "hive/custom-host.example.com@EXAMPLE.COM",
        "hive.metastore.kerberos.keytab.file", "/etc/security/keytabs/custom.keytab"));
    HiveConf conf = new HiveConf();
    config.security().frontDoorConf().forEach(conf::set);
    conf.set("hive.cluster.delegation.token.store.class",
        "org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore");

    FrontDoorSecurity.applyZooKeeperKerberosDefaults(config, conf);

    Assert.assertEquals(
        "hive/custom-host.example.com@EXAMPLE.COM",
        conf.get("hive.metastore.kerberos.principal"));
    Assert.assertEquals(
        "/etc/security/keytabs/custom.keytab",
        conf.get("hive.metastore.kerberos.keytab.file"));
  }

  private static final class TestConfigFactory {
    private TestConfigFactory() {
    }

    private static ProxyConfig kerberosConfig() {
      return kerberosConfig(java.util.Map.of());
    }

    private static ProxyConfig kerberosConfig(java.util.Map<String, String> frontDoorConf) {
      return new ProxyConfig(
          new ProxyConfig.ServerConfig("hms-proxy", "0.0.0.0", 9083, 16, 256),
          new ProxyConfig.SecurityConfig(
              ProxyConfig.SecurityMode.KERBEROS,
              "hive/proxy-host.example.com@EXAMPLE.COM",
              "hive/backend-host.example.com@EXAMPLE.COM",
              "/etc/security/keytabs/hms-proxy.keytab",
              "/etc/security/keytabs/hms-proxy-client.keytab",
              false,
              frontDoorConf),
          ".",
          "catalog1",
          java.util.Map.of(
              "catalog1",
              new ProxyConfig.CatalogConfig(
                  "catalog1",
                  "catalog1",
                  "file:///warehouse/catalog1",
                  false,
                  null,
                  null,
                  java.util.Map.of("hive.metastore.uris", "thrift://hms1:9083"))));
    }
  }
}
