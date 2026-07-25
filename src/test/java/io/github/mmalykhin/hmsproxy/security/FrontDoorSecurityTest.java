package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.junit.Assert;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;

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

  @Test
  public void firstRpcOfNewConnectionOnReusedThreadSeesCurrentClientIdentity() throws Exception {
    HiveIdentityFixture hive = new HiveIdentityFixture();
    List<String> observedAddresses = new ArrayList<>();
    List<String> observedUsers = new ArrayList<>();
    TProcessor businessProcessor = (in, out) -> {
      observedAddresses.add(ClientRequestContext.remoteAddress().orElse(null));
      observedUsers.add(ClientRequestContext.remoteUser().orElse(null));
      return true;
    };

    TProcessor wrapped = FrontDoorSecurity.wrapWithClientRequestContext(
        businessProcessor, hive::wrapProcessor, hive::remoteAddress, hive::remoteUser);

    // Connection 1 on this pooled worker thread.
    hive.connect("10.20.1.15", "clientA@EXAMPLE.COM");
    wrapped.process(null, null);
    wrapped.process(null, null);

    // Connection 2 lands on the same worker thread; its very first RPC must not see client A.
    hive.connect("192.168.10.5", "clientB@EXAMPLE.COM");
    wrapped.process(null, null);

    Assert.assertEquals(
        List.of("10.20.1.15", "10.20.1.15", "192.168.10.5"), observedAddresses);
    Assert.assertEquals(
        List.of("clientA@EXAMPLE.COM", "clientA@EXAMPLE.COM", "clientB@EXAMPLE.COM"), observedUsers);
  }

  @Test
  public void clientRequestContextIsClearedAfterEachRequest() throws Exception {
    HiveIdentityFixture hive = new HiveIdentityFixture();
    TProcessor wrapped = FrontDoorSecurity.wrapWithClientRequestContext(
        (in, out) -> true, hive::wrapProcessor, hive::remoteAddress, hive::remoteUser);
    hive.connect("10.20.1.15", "clientA@EXAMPLE.COM");

    wrapped.process(null, null);

    Assert.assertEquals(Optional.empty(), ClientRequestContext.remoteAddress());
    Assert.assertEquals(Optional.empty(), ClientRequestContext.remoteUser());
  }

  @Test
  public void clientRequestContextIsClearedWhenRequestFails() {
    HiveIdentityFixture hive = new HiveIdentityFixture();
    TProcessor wrapped = FrontDoorSecurity.wrapWithClientRequestContext(
        (in, out) -> {
          throw new TException("boom");
        },
        hive::wrapProcessor,
        hive::remoteAddress,
        hive::remoteUser);
    hive.connect("10.20.1.15", "clientA@EXAMPLE.COM");

    Assert.assertThrows(TException.class, () -> wrapped.process(null, null));

    Assert.assertEquals(Optional.empty(), ClientRequestContext.remoteAddress());
    Assert.assertEquals(Optional.empty(), ClientRequestContext.remoteUser());
  }

  @Test
  public void unauthenticatedRequestLeavesClientRequestContextEmpty() throws Exception {
    HiveIdentityFixture hive = new HiveIdentityFixture();
    List<String> observedAddresses = new ArrayList<>();
    List<String> observedUsers = new ArrayList<>();
    TProcessor wrapped = FrontDoorSecurity.wrapWithClientRequestContext(
        (in, out) -> {
          observedAddresses.add(ClientRequestContext.remoteAddress().orElse(null));
          observedUsers.add(ClientRequestContext.remoteUser().orElse(null));
          return true;
        },
        hive::wrapProcessor,
        hive::remoteAddress,
        hive::remoteUser);
    hive.connect(null, null);

    wrapped.process(null, null);

    Assert.assertEquals(java.util.Collections.singletonList(null), observedAddresses);
    Assert.assertEquals(java.util.Collections.singletonList(null), observedUsers);
  }

  /**
   * Mimics Hive's {@code HadoopThriftAuthBridge.Server.TUGIAssumingProcessor}: the per-request
   * identity is published into static ThreadLocals from inside {@code process()}, just before the
   * wrapped processor runs, and is never cleared afterwards. Reading it before the SASL processor
   * runs therefore yields whatever the previous connection on this thread left behind.
   */
  private static final class HiveIdentityFixture {
    private final ThreadLocal<String> hiveRemoteAddress = new ThreadLocal<>();
    private final ThreadLocal<String> hiveRemoteUser = new ThreadLocal<>();
    private String connectionAddress;
    private String connectionUser;

    void connect(String address, String user) {
      this.connectionAddress = address;
      this.connectionUser = user;
    }

    TProcessor wrapProcessor(TProcessor processor) {
      return (in, out) -> {
        hiveRemoteAddress.set(connectionAddress);
        hiveRemoteUser.set(connectionUser);
        return processor.process(in, out);
      };
    }

    String remoteAddress() {
      return hiveRemoteAddress.get();
    }

    String remoteUser() {
      return hiveRemoteUser.get();
    }
  }

  private static final class TestConfigFactory {
    private TestConfigFactory() {
    }

    private static ProxyConfig kerberosConfig() {
      return kerberosConfig(java.util.Map.of());
    }

    private static ProxyConfig kerberosConfig(java.util.Map<String, String> frontDoorConf) {
      return ProxyConfig.builder()
          .server(new ServerConfig("hms-proxy", "0.0.0.0", 9083, 16, 256))
          .security(new SecurityConfig(
              SecurityMode.KERBEROS,
              "hive/proxy-host.example.com@EXAMPLE.COM",
              "hive/backend-host.example.com@EXAMPLE.COM",
              "/etc/security/keytabs/hms-proxy.keytab",
              "/etc/security/keytabs/hms-proxy-client.keytab",
              false,
              frontDoorConf))
          .catalogDbSeparator(".")
          .defaultCatalog("catalog1")
          .catalogs(java.util.Map.of(
              "catalog1",
              new CatalogConfig(
                  "catalog1", "catalog1", "file:///warehouse/catalog1", false,
                  CatalogAccessMode.READ_WRITE, java.util.List.of(),
                  null, null, java.util.Map.of("hive.metastore.uris", "thrift://hms1:9083"))))
          .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
          .build();
    }
  }
}
