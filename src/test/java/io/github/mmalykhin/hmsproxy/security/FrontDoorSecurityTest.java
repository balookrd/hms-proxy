package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.metastore.security.DelegationTokenStore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MemoryTokenStore;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
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

  @Test
  public void delegationTokenOperationsWrapTokenStoreExceptionInMetaException() {
    DelegationTokenStore.TokenStoreException zkError =
        new DelegationTokenStore.TokenStoreException(
            "Error getting children for /hive/hms-proxy-delegation-tokens/METASTORE/tokens",
            new RuntimeException("connection loss"));
    MetastoreDelegationTokenManager failingManager = new MetastoreDelegationTokenManager() {
      @Override
      public String getDelegationToken(String owner, String renewer, String remoteAddr) {
        throw zkError;
      }

      @Override
      public long renewDelegationToken(String tokenStrForm) {
        throw zkError;
      }

      @Override
      public void cancelDelegationToken(String tokenStrForm) {
        throw zkError;
      }
    };
    FrontDoorSecurity security = new FrontDoorSecurity(
        new Configuration(false), null, null, failingManager, null);

    MetaException issueError = Assert.assertThrows(
        MetaException.class,
        () -> security.issueDelegationToken("alice", "hive"));
    Assert.assertTrue(issueError.getMessage().contains("Unable to issue delegation token"));
    Assert.assertTrue(issueError.getMessage().contains("Error getting children"));
    Assert.assertSame(zkError, issueError.getCause());

    MetaException renewError = Assert.assertThrows(
        MetaException.class,
        () -> security.renewDelegationToken("tok"));
    Assert.assertTrue(renewError.getMessage().contains("Unable to renew delegation token"));
    Assert.assertSame(zkError, renewError.getCause());

    MetaException cancelError = Assert.assertThrows(
        MetaException.class,
        () -> security.cancelDelegationToken("tok"));
    Assert.assertTrue(cancelError.getMessage().contains("Unable to cancel delegation token"));
    Assert.assertSame(zkError, cancelError.getCause());
  }

  @Test
  public void delegationTokenOperationsWrapIoExceptionInMetaException() {
    IOException ioError = new IOException("Token can't be found");
    MetastoreDelegationTokenManager failingManager = new MetastoreDelegationTokenManager() {
      @Override
      public String getDelegationToken(String owner, String renewer, String remoteAddr) throws IOException {
        throw ioError;
      }

      @Override
      public long renewDelegationToken(String tokenStrForm) throws IOException {
        throw ioError;
      }

      @Override
      public void cancelDelegationToken(String tokenStrForm) throws IOException {
        throw ioError;
      }
    };
    FrontDoorSecurity security = new FrontDoorSecurity(
        new Configuration(false), null, null, failingManager, null);

    MetaException issueError = Assert.assertThrows(
        MetaException.class,
        () -> security.issueDelegationToken("alice", "hive"));
    Assert.assertFalse(issueError.getMessage().contains("IOException"));
    Assert.assertSame(ioError, issueError.getCause());

    MetaException renewError = Assert.assertThrows(
        MetaException.class,
        () -> security.renewDelegationToken("tok"));
    Assert.assertFalse(renewError.getMessage().contains("IOException"));
    Assert.assertSame(ioError, renewError.getCause());

    MetaException cancelError = Assert.assertThrows(
        MetaException.class,
        () -> security.cancelDelegationToken("tok"));
    Assert.assertFalse(cancelError.getMessage().contains("IOException"));
    Assert.assertSame(ioError, cancelError.getCause());
  }

  @Test
  public void delegationTokenIssuanceAndRenewalRejectDigestMd5AndAllowGssapi() throws Exception {
    MetastoreDelegationTokenManager manager = new MetastoreDelegationTokenManager() {
      @Override
      public String getDelegationToken(String owner, String renewer, String remoteAddr) {
        return "issued-token";
      }

      @Override
      public long renewDelegationToken(String tokenStrForm) {
        return 12345L;
      }

      @Override
      public void cancelDelegationToken(String tokenStrForm) {
      }
    };
    FrontDoorSecurity security = new FrontDoorSecurity(
        new Configuration(false),
        null,
        null,
        manager,
        null,
        java.util.Set.of(),
        SecurityConfig.DEFAULT_FRONT_DOOR_MAX_MEMORY_TOKENS,
        "hive/proxy@EXAMPLE.COM");

    String prevMechanism = ClientRequestContext.setAuthMechanism("DIGEST-MD5");
    try {
      MetaException issueErr = Assert.assertThrows(
          MetaException.class, () -> security.issueDelegationToken("alice", "hive"));
      Assert.assertTrue(issueErr.getMessage().contains("Kerberos (GSSAPI)"));

      MetaException renewErr = Assert.assertThrows(
          MetaException.class, () -> security.renewDelegationToken("issued-token"));
      Assert.assertTrue(renewErr.getMessage().contains("Kerberos (GSSAPI)"));

      // Cancel remains allowed even when authenticated with DIGEST-MD5.
      security.cancelDelegationToken("issued-token");

      ClientRequestContext.setAuthMechanism("GSSAPI");
      Assert.assertEquals("issued-token", security.issueDelegationToken("alice", "hive"));
      Assert.assertEquals(12345L, security.renewDelegationToken("issued-token"));
    } finally {
      ClientRequestContext.restoreAuthMechanism(prevMechanism);
    }
  }

  @Test
  public void issueDelegationTokenEnforcesMemoryTokenLimit() throws Exception {
    MemoryTokenStore memoryStore = new MemoryTokenStore();
    memoryStore.setConf(new Configuration(false));
    memoryStore.init(null, HadoopThriftAuthBridge.Server.ServerMode.METASTORE);
    LocalDelegationTokenStore localStore = new LocalDelegationTokenStore(memoryStore);
    MetastoreDelegationTokenManager manager = new MetastoreDelegationTokenManager() {
      @Override
      public String getDelegationToken(String owner, String renewer, String remoteAddr) {
        return "token-value";
      }
    };
    FrontDoorSecurity security = new FrontDoorSecurity(
        new Configuration(false),
        null,
        null,
        manager,
        localStore,
        java.util.Set.of(),
        2,
        "hive/proxy@EXAMPLE.COM");

    String prevMechanism = ClientRequestContext.setAuthMechanism("GSSAPI");
    try {
      Assert.assertEquals("token-value", security.issueDelegationToken("alice", "hive"));
      memoryStore.addToken(
          new DelegationTokenIdentifier(
              new org.apache.hadoop.io.Text("alice"),
              new org.apache.hadoop.io.Text("hive"),
              new org.apache.hadoop.io.Text("alice")),
          new DelegationTokenInformation(1000L, new byte[] {1}));
      Assert.assertEquals("token-value", security.issueDelegationToken("bob", "hive"));
      memoryStore.addToken(
          new DelegationTokenIdentifier(
              new org.apache.hadoop.io.Text("bob"),
              new org.apache.hadoop.io.Text("hive"),
              new org.apache.hadoop.io.Text("bob")),
          new DelegationTokenInformation(1000L, new byte[] {2}));

      MetaException limitErr = Assert.assertThrows(
          MetaException.class, () -> security.issueDelegationToken("charlie", "hive"));
      Assert.assertTrue(limitErr.getMessage().contains("limit (2) reached"));
    } finally {
      ClientRequestContext.restoreAuthMechanism(prevMechanism);
      localStore.close();
    }
  }

  @Test
  public void tokenStoreAdminRpcsAreDeniedByDefaultAndRestrictedToConfiguredAdmins() throws Exception {
    MemoryTokenStore memoryStore = new MemoryTokenStore();
    memoryStore.setConf(new Configuration(false));
    memoryStore.init(null, HadoopThriftAuthBridge.Server.ServerMode.METASTORE);
    LocalDelegationTokenStore localStore = new LocalDelegationTokenStore(memoryStore);
    MetastoreDelegationTokenManager dummyManager = new MetastoreDelegationTokenManager();

    FrontDoorSecurity closedByDefault = new FrontDoorSecurity(
        new Configuration(false), null, null, dummyManager, localStore,
        java.util.Set.of(), 100, "hive/proxy@EXAMPLE.COM");
    FrontDoorSecurity restricted = new FrontDoorSecurity(
        new Configuration(false), null, null, dummyManager, localStore,
        java.util.Set.of("admin/host1@EXAMPLE.COM", "ops"), 100, "hive/proxy@EXAMPLE.COM");

    DelegationTokenIdentifier id = new DelegationTokenIdentifier(
        new org.apache.hadoop.io.Text("alice"),
        new org.apache.hadoop.io.Text("hive"),
        new org.apache.hadoop.io.Text("alice"));
    String encodedId = LocalDelegationTokenStore.encodeTokenIdentifier(id);
    String encodedInfo = LocalDelegationTokenStore.encodeDelegationTokenInformation(
        new DelegationTokenInformation(5000L, new byte[] {1, 2}));

    String prevAuthUser = ClientRequestContext.authenticatedUser().orElse(null);
    String prevRemoteUser = ClientRequestContext.remoteUser().orElse(null);
    String prevMechanism = ClientRequestContext.authMechanism().orElse(null);
    try {
      ClientRequestContext.setAuthMechanism("GSSAPI");
      ClientRequestContext.setAuthenticatedUser("admin/host1@EXAMPLE.COM");
      assertAllTokenAdminRpcsDenied(closedByDefault, encodedId, encodedInfo);

      // Non-admin user is denied.
      ClientRequestContext.setAuthenticatedUser("eve@EXAMPLE.COM");
      assertAllTokenAdminRpcsDenied(restricted, encodedId, encodedInfo);

      // Cross-realm user matching short name 'ops' is denied.
      ClientRequestContext.setAuthenticatedUser("ops@OTHER.REALM");
      assertAllTokenAdminRpcsDenied(restricted, encodedId, encodedInfo);

      // DIGEST-MD5 even with matching name is denied.
      ClientRequestContext.setAuthMechanism("DIGEST-MD5");
      ClientRequestContext.setAuthenticatedUser("admin/host1@EXAMPLE.COM");
      assertAllTokenAdminRpcsDenied(restricted, encodedId, encodedInfo);

      // Authorized full principal and same-realm short name with GSSAPI succeed on all 8 RPCs.
      ClientRequestContext.setAuthMechanism("GSSAPI");
      ClientRequestContext.setAuthenticatedUser("admin/host1@EXAMPLE.COM");
      int seq = restricted.addMasterKey("secret-key");
      restricted.updateMasterKey(seq, "secret-key-2");
      Assert.assertFalse(restricted.getMasterKeys().isEmpty());
      Assert.assertTrue(restricted.addToken(encodedId, encodedInfo));
      Assert.assertEquals(encodedInfo, restricted.getToken(encodedId));
      Assert.assertEquals(List.of(encodedId), restricted.getAllTokenIdentifiers());
      Assert.assertTrue(restricted.removeToken(encodedId));
      Assert.assertTrue(restricted.removeMasterKey(seq));

      ClientRequestContext.setAuthenticatedUser("ops@EXAMPLE.COM");
      Assert.assertEquals(List.of(), restricted.getAllTokenIdentifiers());
    } finally {
      ClientRequestContext.restoreAuthenticatedUser(prevAuthUser);
      ClientRequestContext.restoreRemoteUser(prevRemoteUser);
      ClientRequestContext.restoreAuthMechanism(prevMechanism);
      restricted.close();
    }
  }

  @Test
  public void closeClosesLocalDelegationTokenStore() {
    java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
    DelegationTokenStore store = new DelegationTokenStore() {
      @Override public int addMasterKey(String s) { return 0; }
      @Override public void updateMasterKey(int keySeq, String s) {}
      @Override public boolean removeMasterKey(int keySeq) { return false; }
      @Override public String[] getMasterKeys() { return new String[0]; }
      @Override public boolean addToken(DelegationTokenIdentifier id, DelegationTokenInformation info) { return false; }
      @Override public DelegationTokenInformation getToken(DelegationTokenIdentifier id) { return null; }
      @Override public boolean removeToken(DelegationTokenIdentifier id) { return false; }
      @Override public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() { return List.of(); }
      @Override public void init(Object handler, HadoopThriftAuthBridge.Server.ServerMode serverMode) {}
      @Override public void close() { closed.set(true); }
      @Override public void setConf(Configuration conf) {}
      @Override public Configuration getConf() { return new Configuration(false); }
    };
    FrontDoorSecurity security = new FrontDoorSecurity(
        new Configuration(false), null, null, new MetastoreDelegationTokenManager(), new LocalDelegationTokenStore(store));
    security.close();
    Assert.assertTrue(closed.get());
  }

  private static void assertAllTokenAdminRpcsDenied(
      FrontDoorSecurity security,
      String encodedId,
      String encodedInfo
  ) {
    Assert.assertThrows(MetaException.class, () -> security.addToken(encodedId, encodedInfo));
    Assert.assertThrows(MetaException.class, () -> security.removeToken(encodedId));
    Assert.assertThrows(MetaException.class, () -> security.getToken(encodedId));
    Assert.assertThrows(MetaException.class, security::getAllTokenIdentifiers);
    Assert.assertThrows(MetaException.class, () -> security.addMasterKey("k"));
    Assert.assertThrows(MetaException.class, () -> security.updateMasterKey(0, "k"));
    Assert.assertThrows(MetaException.class, () -> security.removeMasterKey(0));
    Assert.assertThrows(MetaException.class, security::getMasterKeys);
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
