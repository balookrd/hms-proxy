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
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class RoutingMetaStoreHandlerTest {
  private static final Path HDP_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();
  private static final Path HDP_6150_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar").toAbsolutePath();
  private static final ProxyConfig CUSTOM_SEPARATOR_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
      "__",
      "catalog1",
      Map.of(
          "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

  private static CatalogRouter routerFor(ProxyConfig config) {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String name : config.catalogs().keySet()) {
      backends.put(name, null);
    }
    return new CatalogRouter(config, backends);
  }

  @Test
  public void onlyExplicitCompatibilityMethodsUseDefaultBackendPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("set_ugi"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("flushCache"));
  }

  @Test
  public void currentNotificationEventIdUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
  }

  @Test
  public void explicitlyListedOperationalMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_resource_plans"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_locks"));
  }

  @Test
  public void notificationMethodsHaveCompatibilityFallbacks() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_next_notification"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_notification_events_count"));
  }

  @Test
  public void refreshPrivilegesUsesContextRoutingButHasCompatibilityFallback() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("refresh_privileges"));
  }

  @Test
  public void compatibilityFallbackAppliesToBackendMetaAndTransportFailures() {
    Assert.assertTrue(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "get_next_notification", new MetaException("not allowed")));
    Assert.assertTrue(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "refresh_privileges", new TTransportException()));
    Assert.assertTrue(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "show_compact", new TApplicationException("unsupported")));
  }

  @Test
  public void nonCompatibilityMethodsDoNotSilentlyFallback() {
    Assert.assertFalse(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "create_role", new MetaException("boom")));
    Assert.assertFalse(RoutingMetaStoreHandler.shouldUseCompatibilityFallback(
        "get_delegation_token", new MetaException("boom")));
  }

  @Test
  public void serviceReadMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("getMetaConf"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_functions"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_metastore_db_uuid"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns_info"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_locks"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_active_resource_plan"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_runtime_stats"));
  }

  @Test
  public void backendLocalTxnAndLockMethodsUseDefaultBackendPathWhenCatalogContextIsMissing() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("open_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("commit_txn"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("abort_txn"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("abort_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("check_lock"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("unlock"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("heartbeat"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("heartbeat_txn_range"));
  }

  @Test
  public void partitionValidationWithoutNamespaceUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("partition_name_has_valid_characters"));
  }

  @Test
  public void removedPrefixBasedMethodsNoLongerUseDefaultBackendPath() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_role_names"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_token_identifiers"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_master_keys"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("list_roles"));
  }

  @Test
  public void getAllFunctionsWithoutCatalogContextUsesDefaultBackend() throws Throwable {
    GetAllFunctionsResponse backendResponse = new GetAllFunctionsResponse();
    backendResponse.setFunctions(List.of());

    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))));

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_all_functions".equals(method.getName())) {
        backendCalls.incrementAndGet();
        return backendResponse;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_functions");

    Object result = handler.invoke(null, method, new Object[0]);

    Assert.assertSame(backendResponse, result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void getMetaConfWithoutCatalogContextUsesDefaultBackend() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))));

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("getMetaConf".equals(method.getName())) {
        backendCalls.incrementAndGet();
        Assert.assertEquals("metastore.thrift.uris", args[0]);
        return "thrift://backend";
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("getMetaConf", String.class);

    Object result = handler.invoke(null, method, new Object[] {"metastore.thrift.uris"});

    Assert.assertEquals("thrift://backend", result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void getMetastoreDbUuidWithoutCatalogContextUsesDefaultBackend() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))));

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_metastore_db_uuid".equals(method.getName())) {
        backendCalls.incrementAndGet();
        return "uuid-1";
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_metastore_db_uuid");

    Object result = handler.invoke(null, method, new Object[0]);

    Assert.assertEquals("uuid-1", result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void dropFunctionRoutesByExplicitDbFirstMethodAllowlist() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("drop_function", String.class, String.class);

    Object result = handler.invoke(null, method, new Object[] {"catalog2__sales", "f_events"});

    Assert.assertNull(result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void setMetaConfWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("setMetaConf", String.class, String.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"metastore.thrift.uris", "thrift://override"}));

    Assert.assertTrue(error.getMessage().contains("has no catalog context"));
  }

  @Test
  public void grantRoleWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
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

    Assert.assertTrue(error.getMessage().contains("has no catalog context"));
  }

  @Test
  public void revokeRoleWithoutCatalogContextIsRejectedInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "revoke_role",
        String.class,
        String.class,
        PrincipalType.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"admin_role", "alice", PrincipalType.USER}));

    Assert.assertTrue(error.getMessage().contains("has no catalog context"));
  }

  @Test
  public void addTokenWithoutKerberosFrontDoorIsRejected() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("add_token", String.class, String.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"token-id", "payload"}));

    Assert.assertTrue(error.getMessage().contains("Delegation tokens require Kerberos/SASL"));
  }

  @Test
  public void tokenIdentifierListingWithoutKerberosFrontDoorIsRejected() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_token_identifiers");

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[0]));

    Assert.assertTrue(error.getMessage().contains("Delegation tokens require Kerberos/SASL"));
  }

  @Test
  public void unrelatedGlobalMethodStillRequiresExplicitHandling() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("create_role"));
  }

  @Test
  public void servicePrincipalTrafficDoesNotTriggerBackendImpersonation() {
    ProxyConfig.SecurityConfig security = new ProxyConfig.SecurityConfig(
        ProxyConfig.SecurityMode.KERBEROS,
        "hive/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "proxy-client/hd-hdp-31-08.dmp.vimpelcom.ru@BEE.VIMPELCOM.RU",
        "/etc/security/keytabs/hive.service.keytab",
        "/etc/security/keytabs/hive.client.keytab",
        true,
        java.util.Map.of());

    Assert.assertTrue(RoutingMetaStoreHandler.isServicePrincipalUser("hive", security));
    Assert.assertFalse(RoutingMetaStoreHandler.isServicePrincipalUser("alice", security));
    Assert.assertFalse(RoutingMetaStoreHandler.isServicePrincipalUser("proxy-client", security));
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
  public void getConfigValueCompatibilityMapsShortMetastoreAlias() {
    Assert.assertEquals("300", MetastoreCompatibility.compatibleConfigValue(
        "batch.retrieve.max",
        "50",
        java.util.Map.of("metastore.batch.retrieve.max", "300")).orElse(null));
  }

  @Test
  public void getConfigValueCompatibilityUsesClientDefaultWhenProxyConfigMissing() {
    Assert.assertEquals("50", MetastoreCompatibility.compatibleConfigValue(
        "batch.retrieve.max",
        "50",
        java.util.Map.of()).orElse(null));
  }

  @Test
  public void getConfigValueCompatibilityDoesNotInterceptCanonicalMetastoreKeys() {
    Assert.assertFalse(MetastoreCompatibility.compatibleConfigValue(
        "metastore.batch.retrieve.max",
        "50",
        java.util.Map.of()).isPresent());
  }

  @Test
  public void explicitDefaultCatalogLeavesUnprefixedDatabaseNameUntouched() throws Exception {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    java.lang.reflect.Method method =
        RoutingMetaStoreHandler.class.getDeclaredMethod("resolveRequestNamespace", String.class, String.class);
    method.setAccessible(true);

    CatalogRouter.ResolvedNamespace namespace =
        (CatalogRouter.ResolvedNamespace) method.invoke(handler, "catalog1", "sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("sales", namespace.externalDbName());
  }

  @Test
  public void explicitCatalogPrefixStillRoutesUsingThatPrefix() throws Exception {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    java.lang.reflect.Method method =
        RoutingMetaStoreHandler.class.getDeclaredMethod("resolveRequestNamespace", String.class, String.class);
    method.setAccessible(true);

    CatalogRouter.ResolvedNamespace namespace =
        (CatalogRouter.ResolvedNamespace) method.invoke(handler, "catalog1", "catalog1__sales");

    Assert.assertEquals("catalog1", namespace.catalogName());
    Assert.assertEquals("sales", namespace.backendDbName());
    Assert.assertEquals("catalog1__sales", namespace.externalDbName());
  }

  @Test
  public void getVersionUsesConfiguredFrontendProfile() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
            false));
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, routerFor(config), null);
    java.lang.reflect.Method method = org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class
        .getMethod("getVersion");

    Object version = handler.invoke(null, method, null);

    Assert.assertEquals("3.1.0.3.1.0.0-78", version);
  }

  @Test
  public void transactionalDdlGuardBlocksCreateTableForMatchingClientAddress() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreHandler handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.20.0.0/16");
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
    RoutingMetaStoreHandler handler = guardedHandler(backendCalls, capturedTable, "10.20.0.0/16");
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
    RoutingMetaStoreHandler handler = guardedHandler(backendCalls, new AtomicReference<>(), "10.10.10.10");
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
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(false),
        new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.REJECT, List.of()));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
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
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(false),
        new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.REWRITE, List.of("10.20.0.0/16")));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
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
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(false),
        new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.REWRITE, List.of()));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
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
    RoutingMetaStoreHandler handler = guardedHandler(backendCalls, capturedTable, "10.20.0.0/16");
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
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(false),
        new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.REWRITE, List.of("10.20.0.0/16")));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
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
  public void readOnlyCatalogBlocksWriteOperations() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreHandler handler = accessModeHandler(
        ProxyConfig.CatalogAccessMode.READ_ONLY,
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
    RoutingMetaStoreHandler handler = accessModeHandler(
        ProxyConfig.CatalogAccessMode.READ_WRITE_DB_WHITELIST,
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
    RoutingMetaStoreHandler handler = accessModeHandler(
        ProxyConfig.CatalogAccessMode.READ_WRITE_DB_WHITELIST,
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
  public void addWriteNotificationLogRoutesToResolvedCatalogAndRewritesDb() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();

    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
            HDP_JAR.toString(),
            HDP_JAR.toString(),
            false));

    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(config, config.catalogs().get("catalog2"),
        capturedDb, capturedTable);
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreHandlerTest.class.getClassLoader());
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

    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
            HDP_JAR.toString(),
            null,
            false));

    CatalogBackend apacheBackend = newBackend(config, config.catalogs().get("catalog1"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession()));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", apacheBackend)));
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreHandlerTest.class.getClassLoader());
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

    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
            HDP_6150_JAR.toString(),
            HDP_6150_JAR.toString(),
            false));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_6150_JAR.toUri().toURL()},
        RoutingMetaStoreHandlerTest.class.getClassLoader());
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
  public void getAllMaterializedViewObjectsForRewritingUsesDefaultHortonworksBackend() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog2",
        Map.of("catalog2", catalogConfig(
            "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(),
            Map.of("hive.metastore.uris", "thrift://two"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
            HDP_6150_JAR.toString(),
            HDP_6150_JAR.toString(),
            false));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);

    Object response = handler.getAllMaterializedViewObjectsForRewriting();

    Assert.assertEquals(1, ((List<?>) response).size());
    Table table = (Table) ((List<?>) response).get(0);
    Assert.assertEquals("sales", table.getDbName());
  }

  private static CatalogBackend newIsolatedHortonworksBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      AtomicReference<String> capturedDb,
      AtomicReference<String> capturedTable
  ) throws Exception {
    return newIsolatedHortonworksBackend(
        proxyConfig,
        catalogConfig,
        HDP_JAR,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        (proxy, method, args) -> {
          if ("add_write_notification_log".equals(method.getName())) {
            Object request = args[0];
            capturedDb.set((String) request.getClass().getMethod("getDb").invoke(request));
            capturedTable.set((String) request.getClass().getMethod("getTable").invoke(request));
            return request.getClass()
                .getClassLoader()
                .loadClass("org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse")
                .getConstructor()
                .newInstance();
          }
          if ("getVersion".equals(method.getName())) {
            return "3.1.0.3.1.0.0-78";
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

  private static CatalogBackend newIsolatedHortonworksBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      Path jar,
      MetastoreRuntimeProfile runtimeProfile,
      java.lang.reflect.InvocationHandler delegateHandler
  ) throws Exception {
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {jar.toUri().toURL()},
        RoutingMetaStoreHandlerTest.class.getClassLoader());
    Class<?> ifaceClass = Class.forName("org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Iface", true, classLoader);
    Object delegate = java.lang.reflect.Proxy.newProxyInstance(
        classLoader,
        new Class<?>[] {ifaceClass},
        delegateHandler);

    IsolatedInvocationBridge bridge = new IsolatedInvocationBridge(classLoader, delegate, ifaceClass);
    Constructor<IsolatedMetastoreClient> isolatedCtor =
        IsolatedMetastoreClient.class.getDeclaredConstructor(Object.class, IsolatedInvocationBridge.class);
    isolatedCtor.setAccessible(true);
    Object closableClient = new Object() {
      @SuppressWarnings("unused")
      public void close() {
      }
    };
    IsolatedMetastoreClient isolatedClient =
        isolatedCtor.newInstance(closableClient, bridge);

    Constructor<BackendInvocationSession> sessionCtor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    sessionCtor.setAccessible(true);
    BackendInvocationSession session = sessionCtor.newInstance(null, null, isolatedClient);

    BackendAdapter adapter =
        new TestBackendAdapter(runtimeProfile);
    return newBackend(proxyConfig, catalogConfig, adapter, newBackendRuntime(proxyConfig, catalogConfig, session));
  }

  private static CatalogBackend newBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      BackendAdapter adapter,
      BackendRuntime runtime
  ) throws Exception {
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    Constructor<CatalogBackend> ctor = CatalogBackend.class.getDeclaredConstructor(
        ProxyConfig.class,
        ProxyConfig.CatalogConfig.class,
        HiveConf.class,
        BackendAdapter.class,
        BackendRuntime.class,
        Catalog.class);
    ctor.setAccessible(true);
    return ctor.newInstance(proxyConfig, catalogConfig, new HiveConf(), adapter, runtime, catalog);
  }

  private static Object childTable(ClassLoader classLoader, String dbName, String tableName) throws Exception {
    Object table = Class.forName("org.apache.hadoop.hive.metastore.api.Table", true, classLoader)
        .getConstructor()
        .newInstance();
    table.getClass().getMethod("setDbName", String.class).invoke(table, dbName);
    table.getClass().getMethod("setTableName", String.class).invoke(table, tableName);
    return table;
  }

  private static BackendRuntime newBackendRuntime(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      BackendInvocationSession session
  ) throws Exception {
    Constructor<BackendRuntime> ctor = BackendRuntime.class.getDeclaredConstructor(
        ProxyConfig.class,
        ProxyConfig.CatalogConfig.class,
        HiveConf.class,
        boolean.class,
        BackendRuntime.SessionFactory.class,
        BackendInvocationSession.class);
    ctor.setAccessible(true);
    return ctor.newInstance(proxyConfig, catalogConfig, new HiveConf(), false, null, session);
  }

  private static BackendInvocationSession newSession() throws Exception {
    Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    ctor.setAccessible(true);
    org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface thriftClient =
        (org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface) java.lang.reflect.Proxy.newProxyInstance(
            org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class.getClassLoader(),
            new Class<?>[] {org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class},
            (proxy, method, args) -> {
              throw new UnsupportedOperationException(method.getName());
            });
    return ctor.newInstance(null, thriftClient, null);
  }

  private static RoutingMetaStoreHandler guardedHandler(
      AtomicInteger backendCalls,
      AtomicReference<Table> capturedTable,
      String clientAddressRule
  ) throws Exception {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(false),
        new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.REJECT, List.of(clientAddressRule)));

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
    return new RoutingMetaStoreHandler(config, router, null);
  }

  private static RoutingMetaStoreHandler accessModeHandler(
      ProxyConfig.CatalogAccessMode accessMode,
      List<String> writeDbWhitelist,
      AtomicInteger backendCalls,
      AtomicReference<Table> capturedTable
  ) throws Exception {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            new ProxyConfig.CatalogConfig(
                "catalog1",
                "c1",
                "file:///c1",
                false,
                accessMode,
                writeDbWhitelist,
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(false),
        new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.DISABLED, List.of()));

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
    return new RoutingMetaStoreHandler(config, router, null);
  }

  private static BackendInvocationSession newSession(java.lang.reflect.InvocationHandler invocationHandler) throws Exception {
    Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    ctor.setAccessible(true);
    org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface thriftClient =
        (org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface) java.lang.reflect.Proxy.newProxyInstance(
            org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class.getClassLoader(),
            new Class<?>[] {org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class},
            invocationHandler);
    return ctor.newInstance(null, thriftClient, null);
  }

  private static ProxyConfig.CatalogConfig catalogConfig(
      String name,
      String description,
      MetastoreRuntimeProfile runtimeProfile,
      String backendStandaloneMetastoreJar,
      Map<String, String> hiveConf
  ) {
    return new ProxyConfig.CatalogConfig(
        name,
        description,
        "file:///" + description,
        false,
        ProxyConfig.CatalogAccessMode.READ_WRITE,
        List.of(),
        runtimeProfile,
        backendStandaloneMetastoreJar,
        hiveConf);
  }

  private static Table table(String dbName, String tableName, Map<String, String> parameters) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(parameters);
    return table;
  }

  private static final class TestBackendAdapter extends AbstractBackendAdapter {
    private TestBackendAdapter(MetastoreRuntimeProfile runtimeProfile) {
      super(runtimeProfile);
    }

    @Override
    public Object invokeRequest(
        CatalogBackend backend,
        String methodName,
        Object request,
        RoutingMetaStoreHandler.ImpersonationContext impersonation
    ) throws Throwable {
      return super.invokeRequest(backend, methodName, request, impersonation);
    }
  }

}
