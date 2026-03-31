package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Constructor;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class RoutingMetaStoreHandlerTest {
  private static final Path HDP_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();
  private static final ProxyConfig CUSTOM_SEPARATOR_CONFIG = new ProxyConfig(
      new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
      new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
      "__",
      "catalog1",
      Map.of(
          "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false, null, null,
              Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", false, null, null,
              Map.of("hive.metastore.uris", "thrift://two"))));

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
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_open_txns_info"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_locks"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_active_resource_plan"));
    Assert.assertTrue(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_runtime_stats"));
  }

  @Test
  public void backendLocalTxnAndLockMethodsDoNotUseDefaultBackendPath() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("open_txns"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("commit_txn"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("abort_txn"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("abort_txns"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("check_lock"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("unlock"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("heartbeat"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("heartbeat_txn_range"));
  }

  @Test
  public void backendLocalTxnAndLockMethodsAreClassifiedExplicitly() {
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("open_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("commit_txn"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("abort_txn"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("abort_txns"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("check_lock"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("unlock"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("heartbeat"));
    Assert.assertTrue(RoutingMetaStoreHandler.usesBackendLocalStateMethod("heartbeat_txn_range"));
    Assert.assertFalse(RoutingMetaStoreHandler.usesBackendLocalStateMethod("compact"));
    Assert.assertFalse(RoutingMetaStoreHandler.usesBackendLocalStateMethod("allocate_table_write_ids"));
  }

  @Test
  public void removedPrefixBasedMethodsNoLongerUseDefaultBackendPath() {
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_functions"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_role_names"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_all_token_identifiers"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("get_master_keys"));
    Assert.assertFalse(RoutingMetaStoreHandler.isDefaultBackendGlobalMethod("list_roles"));
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
        Map.of("catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false,
            null, null, Map.of("hive.metastore.uris", "thrift://one"))),
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
  public void openTxnsWithoutCatalogContextFailsExplicitlyInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    java.lang.reflect.Method method =
        org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class.getMethod(
            "open_txns", OpenTxnRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {new OpenTxnRequest()}));

    Assert.assertTrue(error.getMessage().contains("backend-local txn/lock state"));
  }

  @Test
  public void checkLockWithoutCatalogContextFailsExplicitlyInMultiCatalogMode() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);
    java.lang.reflect.Method method =
        org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface.class.getMethod(
            "check_lock", CheckLockRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {new CheckLockRequest(7L)}));

    Assert.assertTrue(error.getMessage().contains("backend-local txn/lock state"));
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
            "catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false,
                null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", new ProxyConfig.CatalogConfig("catalog2", "c2", "file:///c2", false,
                MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(),
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
        Map.of("catalog1", new ProxyConfig.CatalogConfig("catalog1", "c1", "file:///c1", false,
            null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
            HDP_JAR.toString(),
            null,
            false));

    CatalogBackend apacheBackend = newBackend(config, config.catalogs().get("catalog1"), new ApacheBackendAdapter("3.1.3"),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession("3.1.3")));
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

  private static CatalogBackend newIsolatedHortonworksBackend(
      ProxyConfig proxyConfig,
      ProxyConfig.CatalogConfig catalogConfig,
      AtomicReference<String> capturedDb,
      AtomicReference<String> capturedTable
  ) throws Exception {
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreHandlerTest.class.getClassLoader());
    Class<?> ifaceClass = Class.forName("org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Iface", true, classLoader);
    Object delegate = java.lang.reflect.Proxy.newProxyInstance(
        classLoader,
        new Class<?>[] {ifaceClass},
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
        new TestBackendAdapter("3.1.0.3.1.0.0-78", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78);
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

  private static BackendInvocationSession newSession(String version) throws Exception {
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
              if ("getVersion".equals(method.getName())) {
                return version;
              }
              throw new UnsupportedOperationException(method.getName());
            });
    return ctor.newInstance(null, thriftClient, null);
  }

  private static final class TestBackendAdapter extends AbstractBackendAdapter {
    private TestBackendAdapter(String backendVersion, MetastoreRuntimeProfile runtimeProfileOverride) {
      super(backendVersion, runtimeProfileOverride);
    }

    @Override
    public Object invokeGetTableReq(
        CatalogBackend backend,
        org.apache.hadoop.hive.metastore.api.GetTableRequest request,
        RoutingMetaStoreHandler.ImpersonationContext impersonation
    ) throws Throwable {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object invokeGetTablesReq(
        CatalogBackend backend,
        org.apache.hadoop.hive.metastore.api.GetTablesRequest request,
        RoutingMetaStoreHandler.ImpersonationContext impersonation
    ) throws Throwable {
      throw new UnsupportedOperationException();
    }
  }

}
