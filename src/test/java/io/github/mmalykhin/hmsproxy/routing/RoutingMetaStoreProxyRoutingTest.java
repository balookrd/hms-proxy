package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.AbstractBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendInvocationSession;
import io.github.mmalykhin.hmsproxy.backend.BackendRuntime;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.IsolatedInvocationBridge;
import io.github.mmalykhin.hmsproxy.backend.IsolatedMetastoreClient;
import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableDropPurgeMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitPolicyConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreMode;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreZooKeeperConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class RoutingMetaStoreProxyRoutingTest {
  @Test
  public void dropFunctionRoutesByExplicitDbFirstMethodAllowlist() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("drop_function".equals(method.getName())) {
        backendCalls.incrementAndGet();
        Assert.assertEquals("sales", args[0]);
        Assert.assertEquals("f_events", args[1]);
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), session));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", backend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("drop_function", String.class, String.class);

    Object result = handler.invoke(null, method, new Object[] {"catalog2__sales", "f_events"});

    Assert.assertNull(result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void setMetaConfWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("setMetaConf", String.class, String.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"metastore.thrift.uris", "thrift://override"}));

    Assert.assertTrue(error.getMessage().contains("requires explicit namespace ownership"));
  }

  @Test
  public void grantRoleWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "grant_role",
        String.class,
        String.class,
        PrincipalType.class,
        String.class,
        PrincipalType.class,
        boolean.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(
            null,
            method,
            new Object[] {"admin_role", "alice", PrincipalType.USER, "hive", PrincipalType.USER, false}));

    Assert.assertTrue(error.getMessage().contains("requires explicit namespace ownership"));
  }

  @Test
  public void revokeRoleWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "revoke_role",
        String.class,
        String.class,
        PrincipalType.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"admin_role", "alice", PrincipalType.USER}));

    Assert.assertTrue(error.getMessage().contains("requires explicit namespace ownership"));
  }

  @Test
  public void catalogManagementRpcsAreRejected() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);

    assertCatalogManagementRejected(handler, "create_catalog");
    assertCatalogManagementRejected(handler, "alter_catalog");
    assertCatalogManagementRejected(handler, "drop_catalog");
  }

  @Test
  public void addTokenWithoutKerberosFrontDoorIsRejected() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("add_token", String.class, String.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"token-id", "payload"}));

    Assert.assertTrue(error.getMessage().contains("Delegation tokens require Kerberos/SASL"));
  }

  @Test
  public void tokenIdentifierListingWithoutKerberosFrontDoorIsRejected() throws Throwable {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_token_identifiers");

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[0]));

    Assert.assertTrue(error.getMessage().contains("Delegation tokens require Kerberos/SASL"));
  }

  @Test
  public void servicePrincipalTrafficDoesNotTriggerBackendImpersonation() {
    SecurityConfig security = new SecurityConfig(
        SecurityMode.KERBEROS,
        "hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "proxy-client/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "/etc/security/keytabs/hive.service.keytab",
        "/etc/security/keytabs/hive.client.keytab",
        true,
        java.util.Map.of());

    Assert.assertTrue(RoutingMetaStoreProxy.isServicePrincipalUser("hive", security));
    Assert.assertFalse(RoutingMetaStoreProxy.isServicePrincipalUser("alice", security));
    Assert.assertFalse(RoutingMetaStoreProxy.isServicePrincipalUser("proxy-client", security));
  }

  @Test
  public void delegationTokenAuthorizationMessageMentionsFrontDoorProxyUserKeys() {
    String message = FrontDoorSecurity.delegationTokenAuthorizationMessage(
        "algaraev",
        "algaraev",
        "hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "10.0.0.8");

    Assert.assertTrue(message.contains("owner 'algaraev'"));
    Assert.assertTrue(message.contains("authenticated as 'hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU'"));
    Assert.assertTrue(message.contains("10.0.0.8"));
    Assert.assertTrue(message.contains("security.front-door-conf.hadoop.proxyuser.hive.hosts"));
    Assert.assertTrue(message.contains("security.front-door-conf.hadoop.proxyuser.hive.groups"));
  }

  @Test
  public void catalogBackendCloseQuietlySuppressesSocketClosedFailures() {
    CatalogBackend.closeQuietly(() -> {
      throw new SocketException("Socket closed");
    }, "test metastore client");
  }

  @Test
  public void explicitDefaultCatalogLeavesUnprefixedDatabaseNameUntouched() throws Exception {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    java.lang.reflect.Method method =
        RoutingMetaStoreProxy.class.getDeclaredMethod("resolveRequestNamespace", String.class, String.class);
    method.setAccessible(true);

    CatalogRouter.ResolvedNamespace namespace =
        (CatalogRouter.ResolvedNamespace) method.invoke(handler, "catalog1", "sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("sales", namespace.externalDbName());
  }

  @Test
  public void explicitCatalogPrefixStillRoutesUsingThatPrefix() throws Exception {
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER, new FederationLayer(CUSTOM_SEPARATOR_CONFIG, CUSTOM_SEPARATOR_ROUTER), null);
    java.lang.reflect.Method method =
        RoutingMetaStoreProxy.class.getDeclaredMethod("resolveRequestNamespace", String.class, String.class);
    method.setAccessible(true);

    CatalogRouter.ResolvedNamespace namespace =
        (CatalogRouter.ResolvedNamespace) method.invoke(handler, "catalog1", "catalog1__sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog1__sales", namespace.externalDbName());
  }

  @Test
  public void getVersionUsesConfiguredFrontendProfile() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
    CatalogRouter router = routerFor(config);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    java.lang.reflect.Method method = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
        .getMethod("getVersion");

    Object version = handler.invoke(null, method, null);

    Assert.assertEquals("3.1.0.3.1.0.0-78", version);
  }

  @Test
  public void transactionalDdlGuardBlocksCreateTableForMatchingClientAddress() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

      Assert.assertTrue(error.getMessage().contains("create_table"));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardAllowsCreateTableForNonMatchingClientAddress() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, capturedTable, "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("192.168.10.5");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("sales", capturedTable.get().getDbName());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardBlocksAlterTableWhenTransactionalPropertiesArePresent() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.10.10.10");
    Table table = table("catalog1__sales", "events", Map.of("transactional_properties", "insert_only"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table_with_environment_context",
        String.class,
        String.class,
        Table.class,
        EnvironmentContext.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.10.10.10");
    try {
      MetaException error = Assert.assertThrows(
          MetaException.class,
          () -> handler.invoke(null, method, new Object[] {"catalog1__sales", "events", table, new EnvironmentContext()}));

      Assert.assertTrue(error.getMessage().contains("alter_table_with_environment_context"));
      Assert.assertEquals(0, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlGuardWithoutAddressListAppliesToAllClients() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REJECT_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

    Assert.assertTrue(error.getMessage().contains("create_table"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void transactionalDdlRewriteChangesManagedTransactionalTableToExternal() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of("10.20.0.0/16")))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of(
        "transactional", "true",
        "transactional_properties", "insert_only",
        "owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
      Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
      Assert.assertEquals("true", capturedTable.get().getParameters().get("external.table.purge"));
      Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
      Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
      Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional_properties"));
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteWithoutAddressListAppliesToAllClients() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
  }

  @Test
  public void transactionalDdlRejectDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = guardedHandler(backendCalls, capturedTable, "10.20.0.0/16");
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
      Assert.assertEquals("true", capturedTable.get().getParameters().get("transactional"));
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL, List.of("10.20.0.0/16")))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table currentTable) {
            capturedTable.set(currentTable);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    String previousRemoteAddress = ClientRequestContext.setRemoteAddress("10.20.1.15");
    try {
      handler.invoke(null, method, new Object[] {table});

      Assert.assertEquals(1, backendCalls.get());
      Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
      Assert.assertEquals("true", capturedTable.get().getParameters().get("transactional"));
      Assert.assertFalse(capturedTable.get().getParameters().containsKey("external.table.purge"));
    } finally {
      ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
    }
  }

  @Test
  public void transactionalDdlRewriteToNonTransactionalStripsTransactionalParams() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of(
        "transactional", "true",
        "transactional_properties", "insert_only",
        "owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("MANAGED_TABLE", capturedTable.get().getTableType());
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional_properties"));
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("EXTERNAL"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("external.table.purge"));
  }

  @Test
  public void transactionalDdlRewriteToNonTransactionalDoesNotApplyToNonTransactionalManagedTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("MANAGED_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
  }

  @Test
  public void transactionalDdlRewriteToNonTransactionalDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("transactional", "true"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("true", capturedTable.get().getParameters().get("transactional"));
  }

  @Test
  public void transactionalDdlRewriteManagedToExternalAppliesToAllManagedTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
    Assert.assertEquals("true", capturedTable.get().getParameters().get("external.table.purge"));
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
  }

  @Test
  public void transactionalDdlRewriteManagedToExternalAlsoAppliesToTransactionalManagedTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of(
        "transactional", "true",
        "transactional_properties", "insert_only",
        "owner", "etl"));
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertEquals("TRUE", capturedTable.get().getParameters().get("EXTERNAL"));
    Assert.assertEquals("true", capturedTable.get().getParameters().get("external.table.purge"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional"));
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("transactional_properties"));
    Assert.assertEquals("etl", capturedTable.get().getParameters().get("owner"));
  }

  @Test
  public void transactionalDdlRewriteManagedToExternalDoesNotApplyToExternalTables() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Table table = table("catalog1__sales", "events", Map.of("owner", "etl"));
    table.setTableType("EXTERNAL_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("EXTERNAL_TABLE", capturedTable.get().getTableType());
    Assert.assertFalse(capturedTable.get().getParameters().containsKey("external.table.purge"));
  }

  @Test
  public void externalTableLocationRewriteQualifiesUnqualifiedLocationForNonDefaultCatalog() throws Throwable {
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandler(
        ExternalTableLocationRewriteMode.QUALIFY_UNQUALIFIED,
        null,
        capturedTable);
    Table table = table("catalog2__sales", "events", Map.of("EXTERNAL", "TRUE"));
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor("/tmp/external/events"));
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals("sales", capturedTable.get().getDbName());
    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events", capturedTable.get().getSd().getLocation());
  }

  @Test
  public void externalTableLocationRewriteMovesFrontendDefaultFsToTargetCatalog() throws Throwable {
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandler(
        ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS,
        "hdfs://ns-frontend",
        capturedTable);
    Table table = table("catalog2__sales", "events", Map.of("EXTERNAL", "TRUE"));
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor("hdfs://ns-frontend/tmp/external/events"));
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals("hdfs://ns-catalog2/tmp/external/events", capturedTable.get().getSd().getLocation());
  }

  @Test
  public void externalTableLocationRewriteLeavesExplicitForeignHdfsLocationUntouched() throws Throwable {
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = locationRewriteHandler(
        ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS,
        "hdfs://ns-frontend",
        capturedTable);
    Table table = table("catalog2__sales", "events", Map.of("EXTERNAL", "TRUE"));
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor("hdfs://ns-shared/tmp/external/events"));
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals("hdfs://ns-shared/tmp/external/events", capturedTable.get().getSd().getLocation());
  }

  @Test
  public void readOnlyCatalogBlocksWriteOperations() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_ONLY,
        List.of(),
        backendCalls,
        new AtomicReference<>());
    Table table = table("catalog1__sales", "events", Map.of());
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

    Assert.assertTrue(error.getMessage().contains("READ_ONLY"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void whitelistCatalogAllowsWritesForWhitelistedDatabases() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Table> capturedTable = new AtomicReference<>();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_WRITE_DB_WHITELIST,
        List.of("sales"),
        backendCalls,
        capturedTable);
    Table table = table("catalog1__sales", "events", Map.of());
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    handler.invoke(null, method, new Object[] {table});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals("sales", capturedTable.get().getDbName());
  }

  @Test
  public void whitelistCatalogBlocksWritesForNonWhitelistedDatabases() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = accessModeHandler(
        CatalogAccessMode.READ_WRITE_DB_WHITELIST,
        List.of("sales"),
        backendCalls,
        new AtomicReference<>());
    Table table = table("catalog1__finance", "events", Map.of());
    table.setTableType("MANAGED_TABLE");
    Method method = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {table}));

    Assert.assertTrue(error.getMessage().contains("not allowed"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void getAllDatabasesFiltersHiddenDatabasesByExposurePolicy() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of("sales", "finance"),
                Map.of(),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_all_databases".equals(method.getName())) {
        return List.of("sales", "hidden", "Finance");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[0]);

    Assert.assertEquals(List.of("sales", "Finance"), result);
  }

  @Test
  public void getTableRejectsHiddenTableByExposurePolicyWithoutBackendCall() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      backendCalls.incrementAndGet();
      return table("sales", "secret", Map.of());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);

    NoSuchObjectException error = Assert.assertThrows(
        NoSuchObjectException.class,
        () -> handler.invoke(null, method, new Object[] {"sales", "secret"}));

    Assert.assertTrue(error.getMessage().contains("not exposed"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void getAllTablesFiltersHiddenTablesByExposurePolicyCaseInsensitively() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_all_tables".equals(method.getName())) {
        return List.of("Orders", "secret");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_tables", String.class);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[] {"sales"});

    Assert.assertEquals(List.of("Orders"), result);
  }

  @Test
  public void getTableMetaFiltersHiddenTablesByExposurePolicy() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_table_meta".equals(method.getName())) {
        return List.of(
            new TableMeta("sales", "Orders", "MANAGED_TABLE"),
            new TableMeta("sales", "secret", "MANAGED_TABLE"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method =
        ThriftHiveMetastore.Iface.class.getMethod("get_table_meta", String.class, String.class, List.class);

    @SuppressWarnings("unchecked")
    List<TableMeta> result =
        (List<TableMeta>) handler.invoke(null, method, new Object[] {"sales", "*", List.of()});

    Assert.assertEquals(1, result.size());
    Assert.assertEquals("Orders", result.get(0).getTableName());
  }

  @Test
  public void addWriteNotificationLogRoutesToResolvedCatalogAndRewritesDb() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(), HDP_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(config, config.catalogs().get("catalog2"),
        capturedDb, capturedTable);
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest", true, classLoader);
    Class<?> fileInfoClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.InsertEventRequestData", true, classLoader);
    Object fileInfo = fileInfoClass.getConstructor().newInstance();
    fileInfoClass.getMethod("setFilesAdded", List.class).invoke(fileInfo, List.of());
    Object request = requestClass
        .getConstructor(long.class, long.class, String.class, String.class, fileInfoClass)
        .newInstance(1L, 2L, "catalog2__sales", "events", fileInfo);

    Object response = handler.addWriteNotificationLog(request);

    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("events", capturedTable.get());
    Assert.assertEquals(
        "org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse",
        response.getClass().getName());
  }

  @Test
  public void addWriteNotificationLogRejectsNonHortonworksBackendRuntime() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(), null, false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend apacheBackend = newBackend(config, config.catalogs().get("catalog1"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession()));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", apacheBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest", true, classLoader);
    Class<?> fileInfoClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.InsertEventRequestData", true, classLoader);
    Object fileInfo = fileInfoClass.getConstructor().newInstance();
    fileInfoClass.getMethod("setFilesAdded", List.class).invoke(fileInfo, List.of());
    Object request = requestClass
        .getConstructor(long.class, long.class, String.class, String.class, fileInfoClass)
        .newInstance(1L, 2L, "default", "events", fileInfo);

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.addWriteNotificationLog(request));

    Assert.assertTrue(error.getMessage().contains("requires a Hortonworks backend runtime"));
  }

  @Test
  public void getTablesExtRoutesToResolvedCatalogAndRewritesNamespace() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));
    AtomicReference<String> capturedCatalog = new AtomicReference<>();
    AtomicReference<String> capturedDb = new AtomicReference<>();

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(), HDP_6150_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config, config.catalogs().get("catalog2"), HDP_6150_JAR, MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
        (proxy, method, args) -> {
          if ("get_tables_ext".equals(method.getName())) {
            Object request = args[0];
            capturedCatalog.set((String) request.getClass().getMethod("getCatalog").invoke(request));
            capturedDb.set((String) request.getClass().getMethod("getDatabase").invoke(request));
            Class<?> infoClass = request.getClass().getClassLoader()
                .loadClass("org.apache.hadoop.hive.metastore.api.ExtendedTableInfo");
            Object info = infoClass.getConstructor(String.class).newInstance("events");
            return List.of(info);
          }
          if ("getVersion".equals(method.getName())) {
            return "3.1.0.3.1.5.6150-1";
          }
          throw new UnsupportedOperationException(method.getName());
        });
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_6150_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.GetTablesExtRequest", true, classLoader);
    Object request = requestClass.getConstructor(String.class, String.class, String.class, int.class)
        .newInstance("catalog2", "catalog2__sales", "*", 1);

    Object response = handler.getTablesExt(request);

    Assert.assertEquals("catalog2", capturedCatalog.get());
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals(1, ((List<?>) response).size());
  }

  @Test
  public void getTablesExtFiltersHiddenTablesByExposurePolicy() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog2")
        .catalogs(Map.of(
            "catalog2",
            catalogConfigWithExposure(
                "catalog2",
                "c2",
                MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
                HDP_6150_JAR.toString(),
                CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("events")),
                Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(), HDP_6150_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config,
        config.catalogs().get("catalog2"),
        HDP_6150_JAR,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
        (proxy, method, args) -> {
          if ("get_tables_ext".equals(method.getName())) {
            Object request = args[0];
            Class<?> infoClass = request.getClass().getClassLoader()
                .loadClass("org.apache.hadoop.hive.metastore.api.ExtendedTableInfo");
            return List.of(
                infoClass.getConstructor(String.class).newInstance("events"),
                infoClass.getConstructor(String.class).newInstance("secret"));
          }
          if ("getVersion".equals(method.getName())) {
            return "3.1.0.3.1.5.6150-1";
          }
          throw new UnsupportedOperationException(method.getName());
        });
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog2", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_6150_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.GetTablesExtRequest", true, classLoader);
    Object request = requestClass.getConstructor(String.class, String.class, String.class, int.class)
        .newInstance("catalog2", "sales", "*", 10);

    Object response = handler.getTablesExt(request);

    Assert.assertEquals(1, ((List<?>) response).size());
    Object tableInfo = ((List<?>) response).get(0);
    Assert.assertEquals("events", tableInfo.getClass().getMethod("getTblName").invoke(tableInfo));
  }

  @Test
  public void getAllMaterializedViewObjectsForRewritingUsesDefaultHortonworksBackend() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog2")
        .catalogs(Map.of("catalog2", catalogConfig(
            "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(),
            Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(), HDP_6150_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config, config.catalogs().get("catalog2"), HDP_6150_JAR, MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
        (proxy, method, args) -> {
          if ("get_all_materialized_view_objects_for_rewriting".equals(method.getName())) {
            return List.of(childTable(proxy.getClass().getClassLoader(), "sales", "mv_events"));
          }
          if ("getVersion".equals(method.getName())) {
            return "3.1.0.3.1.5.6150-1";
          }
          throw new UnsupportedOperationException(method.getName());
        });
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog2", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Object response = handler.getAllMaterializedViewObjectsForRewriting();

    Assert.assertEquals(1, ((List<?>) response).size());
    Table table = (Table) ((List<?>) response).get(0);
    Assert.assertEquals("sales", table.getDbName());
  }

}
