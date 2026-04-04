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
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
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
  public void catalogManagementRpcsAreRejected() throws Throwable {
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(CUSTOM_SEPARATOR_CONFIG, routerFor(CUSTOM_SEPARATOR_CONFIG), null);

    assertCatalogManagementRejected(handler, "create_catalog");
    assertCatalogManagementRejected(handler, "alter_catalog");
    assertCatalogManagementRejected(handler, "drop_catalog");
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
  public void lockRoutesByNamespaceAndRewritesNestedDbName() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    AtomicReference<LockRequest> capturedRequest = new AtomicReference<>();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("lock".equals(method.getName())) {
        capturedRequest.set((LockRequest) args[0]);
        LockResponse response = new LockResponse();
        response.setLockid(7L);
        response.setState(LockState.ACQUIRED);
        return response;
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
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    Object result = handler.invoke(null, method, new Object[] {lockRequest("catalog2__sales", "events")});

    Assert.assertTrue(result instanceof LockResponse);
    Assert.assertEquals("sales", capturedRequest.get().getComponent().get(0).getDbname());
    Assert.assertEquals("events", capturedRequest.get().getComponent().get(0).getTablename());
  }

  @Test
  public void backendLockTransportFailuresAreSurfacedAsMetaExceptions() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    BackendAdapter failingAdapter = new BackendAdapter() {
      @Override
      public Object invoke(
          CatalogBackend backend,
          Method backendMethod,
          Object[] args,
          RoutingMetaStoreHandler.ImpersonationContext impersonation
      ) throws Throwable {
        throw new TApplicationException(TApplicationException.INTERNAL_ERROR, "backend lock failed");
      }

      @Override
      public Object invokeRequest(
          CatalogBackend backend,
          String methodName,
          Object request,
          RoutingMetaStoreHandler.ImpersonationContext impersonation
      ) throws Throwable {
        throw new UnsupportedOperationException(methodName);
      }

      @Override
      public MetastoreCompatibility.BackendProfile backendProfile() {
        return MetastoreCompatibility.BackendProfile.MODERN_REQUESTS;
      }

      @Override
      public MetastoreRuntimeProfile runtimeProfile() {
        return MetastoreRuntimeProfile.APACHE_3_1_3;
      }

      @Override
      public String backendVersion() {
        return null;
      }
    };
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        failingAdapter,
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", backend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {lockRequest("catalog2__sales", "events")}));

    Assert.assertTrue(error.getMessage().contains("catalog2"));
    Assert.assertTrue(error.getMessage().contains("lock"));
    Assert.assertTrue(error.getMessage().contains("TApplicationException"));
    Assert.assertTrue(error.getMessage().contains("backend lock failed"));
  }

  @Test
  public void syntheticReadLockLifecycleStaysInsideProxyForNonDefaultCatalog() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    AtomicInteger defaultBackendCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              defaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, new CatalogRouter(config, backends), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 41L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(41L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method unlockMethod = ThriftHiveMetastore.Iface.class.getMethod("unlock", UnlockRequest.class);
    handler.invoke(null, unlockMethod, new Object[] {new UnlockRequest(lock.getLockid())});

    NoSuchLockException error = Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
    Assert.assertTrue(error.getMessage().contains("Synthetic read lock"));
    Assert.assertEquals(0, defaultBackendCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
  }

  @Test
  public void syntheticReadLockHeartbeatForwardsTxnHeartbeatToDefaultBackend() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    AtomicReference<HeartbeatRequest> capturedHeartbeat = new AtomicReference<>();
    AtomicInteger defaultBackendCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("heartbeat".equals(method.getName())) {
                defaultBackendCalls.incrementAndGet();
                capturedHeartbeat.set((HeartbeatRequest) args[0]);
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, new CatalogRouter(config, backends), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 52L)});

    Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
    HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
    heartbeatRequest.setTxnid(52L);
    heartbeatRequest.setLockid(lock.getLockid());
    handler.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

    Assert.assertEquals(1, defaultBackendCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertNotNull(capturedHeartbeat.get());
    Assert.assertTrue(capturedHeartbeat.get().isSetTxnid());
    Assert.assertEquals(52L, capturedHeartbeat.get().getTxnid());
    Assert.assertFalse(capturedHeartbeat.get().isSetLockid());
  }

  @Test
  public void syntheticNoTxnDbLockForNonDefaultCatalogUsesShim() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    AtomicReference<HeartbeatRequest> capturedHeartbeat = new AtomicReference<>();
    AtomicInteger defaultHeartbeatCalls = new AtomicInteger();
    AtomicInteger defaultAbortCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("heartbeat".equals(method.getName())) {
                defaultHeartbeatCalls.incrementAndGet();
                capturedHeartbeat.set((HeartbeatRequest) args[0]);
                return null;
              }
              if ("abort_txn".equals(method.getName())) {
                defaultAbortCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, new CatalogRouter(config, backends), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticNoTxnDbLockRequest("catalog2__sales", 52L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(52L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
    HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
    heartbeatRequest.setTxnid(52L);
    heartbeatRequest.setLockid(lock.getLockid());
    handler.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

    Method abortMethod = ThriftHiveMetastore.Iface.class.getMethod("abort_txn", AbortTxnRequest.class);
    handler.invoke(null, abortMethod, new Object[] {new AbortTxnRequest(52L)});

    Assert.assertEquals(1, defaultHeartbeatCalls.get());
    Assert.assertEquals(1, defaultAbortCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertNotNull(capturedHeartbeat.get());
    Assert.assertEquals(52L, capturedHeartbeat.get().getTxnid());
    Assert.assertFalse(capturedHeartbeat.get().isSetLockid());
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void syntheticNoTxnExclusivePartitionLockForNonDefaultCatalogUsesShim() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    AtomicInteger defaultAbortCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("abort_txn".equals(method.getName())) {
                defaultAbortCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, new CatalogRouter(config, backends), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticNoTxnExclusivePartitionLockRequest(
            "catalog2__sales",
            "events",
            "p=2026-03-31",
            61L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(61L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method abortMethod = ThriftHiveMetastore.Iface.class.getMethod("abort_txn", AbortTxnRequest.class);
    handler.invoke(null, abortMethod, new Object[] {new AbortTxnRequest(61L)});

    Assert.assertEquals(1, defaultAbortCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void syntheticReadLocksAreReleasedWhenTxnCompletes() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    AtomicInteger commitCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("commit_txn".equals(method.getName())) {
                commitCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, new CatalogRouter(config, backends), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 77L)});

    Method commitMethod = ThriftHiveMetastore.Iface.class.getMethod("commit_txn", CommitTxnRequest.class);
    handler.invoke(null, commitMethod, new Object[] {new CommitTxnRequest(77L)});

    Assert.assertEquals(1, commitCalls.get());
    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void syntheticReadLockMetricsAreRecordedForInMemoryShim() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))));

    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("heartbeat".equals(method.getName())) {
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    ProxyObservability observability = new ProxyObservability(config);
    RoutingMetaStoreHandler handler =
        new RoutingMetaStoreHandler(config, new CatalogRouter(config, backends), null, observability);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 91L)});

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(91L);
    handler.invoke(null, checkLockMethod, new Object[] {checkRequest});

    Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
    HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
    heartbeatRequest.setTxnid(91L);
    heartbeatRequest.setLockid(lock.getLockid());
    handler.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

    Method unlockMethod = ThriftHiveMetastore.Iface.class.getMethod("unlock", UnlockRequest.class);
    handler.invoke(null, unlockMethod, new Object[] {new UnlockRequest(lock.getLockid())});

    String rendered = observability.metrics().render();

    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"acquire\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"acquired\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"check_lock\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"hit\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"touched\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"txn_forwarded\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"unlock\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"released\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_store_info{store_mode=\"in_memory\"} 1.0"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_locks_active{store_mode=\"in_memory\"} 0.0"));
  }

  @Test
  public void syntheticReadLocksCanFailOverAcrossProxyInstancesViaZooKeeperStore() throws Throwable {
    try (TestingServer zooKeeper = startTestingServerOrSkip()) {
      ProxyConfig config = new ProxyConfig(
          new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
          new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
          "__",
          "catalog1",
          Map.of(
              "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
              "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))),
          new ProxyConfig.BackendConfig(Map.of()),
          new ProxyConfig.CompatibilityConfig(false),
          new ProxyConfig.FederationConfig(false, ProxyConfig.ViewTextRewriteMode.DISABLED, false),
          new ProxyConfig.TransactionalDdlGuardConfig(ProxyConfig.TransactionalDdlGuardMode.DISABLED, List.of()),
          new ProxyConfig.ManagementConfig(false, "127.0.0.1", 10083),
          syntheticReadLockStoreConfig(zooKeeper.getConnectString()));

      AtomicInteger firstProxyBackendCalls = new AtomicInteger();
      CatalogBackend defaultBackendA = newBackend(
          config,
          config.catalogs().get("catalog1"),
          new ApacheBackendAdapter(),
          newBackendRuntime(
              config,
              config.catalogs().get("catalog1"),
              newSession((proxy, method, args) -> {
                firstProxyBackendCalls.incrementAndGet();
                throw new UnsupportedOperationException(method.getName());
              })));
      CatalogBackend nonDefaultBackendA = newBackend(
          config,
          config.catalogs().get("catalog2"),
          new ApacheBackendAdapter(),
          newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));

      AtomicReference<HeartbeatRequest> capturedHeartbeat = new AtomicReference<>();
      AtomicInteger secondProxyCommitCalls = new AtomicInteger();
      CatalogBackend defaultBackendB = newBackend(
          config,
          config.catalogs().get("catalog1"),
          new ApacheBackendAdapter(),
          newBackendRuntime(
              config,
              config.catalogs().get("catalog1"),
              newSession((proxy, method, args) -> {
                if ("heartbeat".equals(method.getName())) {
                  capturedHeartbeat.set((HeartbeatRequest) args[0]);
                  return null;
                }
                if ("commit_txn".equals(method.getName())) {
                  secondProxyCommitCalls.incrementAndGet();
                  return null;
                }
                throw new UnsupportedOperationException(method.getName());
              })));
      CatalogBackend nonDefaultBackendB = newBackend(
          config,
          config.catalogs().get("catalog2"),
          new ApacheBackendAdapter(),
          newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));

      LinkedHashMap<String, CatalogBackend> backendsA = new LinkedHashMap<>();
      backendsA.put("catalog1", defaultBackendA);
      backendsA.put("catalog2", nonDefaultBackendA);
      LinkedHashMap<String, CatalogBackend> backendsB = new LinkedHashMap<>();
      backendsB.put("catalog1", defaultBackendB);
      backendsB.put("catalog2", nonDefaultBackendB);
      ProxyObservability observabilityA = new ProxyObservability(config);
      ProxyObservability observabilityB = new ProxyObservability(config);

      try (CatalogRouter routerA = new CatalogRouter(config, backendsA);
           CatalogRouter routerB = new CatalogRouter(config, backendsB);
           RoutingMetaStoreHandler secondProxy =
               new RoutingMetaStoreHandler(config, routerB, null, observabilityB)) {
        Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
        LockResponse lock;
        RoutingMetaStoreHandler firstProxy = new RoutingMetaStoreHandler(config, routerA, null, observabilityA);
        try {
          lock = (LockResponse) firstProxy.invoke(
              null,
              lockMethod,
              new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 88L)});
        } finally {
          firstProxy.close();
        }

        Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
        CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
        checkRequest.setTxnid(88L);
        LockResponse checked = (LockResponse) secondProxy.invoke(null, checkLockMethod, new Object[] {checkRequest});

        Assert.assertEquals(LockState.ACQUIRED, checked.getState());

        Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
        HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
        heartbeatRequest.setTxnid(88L);
        heartbeatRequest.setLockid(lock.getLockid());
        secondProxy.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

        Assert.assertNotNull(capturedHeartbeat.get());
        Assert.assertEquals(88L, capturedHeartbeat.get().getTxnid());
        Assert.assertFalse(capturedHeartbeat.get().isSetLockid());

        Method commitMethod = ThriftHiveMetastore.Iface.class.getMethod("commit_txn", CommitTxnRequest.class);
        secondProxy.invoke(null, commitMethod, new Object[] {new CommitTxnRequest(88L)});

        Assert.assertEquals(1, secondProxyCommitCalls.get());
        Assert.assertEquals(0, firstProxyBackendCalls.get());
        Assert.assertThrows(
            NoSuchLockException.class,
            () -> secondProxy.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));

        String rendered = observabilityB.metrics().render();
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_handoffs_total{operation=\"check_lock\",catalog=\"catalog2\",store_mode=\"zookeeper\"} 1"));
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_handoffs_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"zookeeper\"} 1"));
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_handoffs_total{operation=\"release_txn\",catalog=\"all\",store_mode=\"zookeeper\"} 1"));
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_store_info{store_mode=\"zookeeper\"} 1.0"));
      }
    }
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
  public void getAllDatabasesFiltersHiddenDatabasesByExposurePolicy() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
                List.of("sales", "finance"),
                Map.of(),
                Map.of("hive.metastore.uris", "thrift://one"))));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[0]);

    Assert.assertEquals(List.of("sales", "Finance"), result);
  }

  @Test
  public void getTableRejectsHiddenTableByExposurePolicyWithoutBackendCall() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);

    NoSuchObjectException error = Assert.assertThrows(
        NoSuchObjectException.class,
        () -> handler.invoke(null, method, new Object[] {"sales", "secret"}));

    Assert.assertTrue(error.getMessage().contains("not exposed"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void getAllTablesFiltersHiddenTablesByExposurePolicyCaseInsensitively() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_tables", String.class);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[] {"sales"});

    Assert.assertEquals(List.of("Orders"), result);
  }

  @Test
  public void getTableMetaFiltersHiddenTablesByExposurePolicy() throws Throwable {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            catalogConfigWithExposure(
                "catalog1",
                "c1",
                null,
                null,
                ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("orders")),
                Map.of("hive.metastore.uris", "thrift://one"))));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);
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
  public void getTablesExtFiltersHiddenTablesByExposurePolicy() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog2",
        Map.of(
            "catalog2",
            catalogConfigWithExposure(
                "catalog2",
                "c2",
                MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
                HDP_6150_JAR.toString(),
                ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
                List.of(),
                Map.of("sales", List.of("events")),
                Map.of("hive.metastore.uris", "thrift://two"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1,
            HDP_6150_JAR.toString(),
            HDP_6150_JAR.toString(),
            false));

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
    RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_6150_JAR.toUri().toURL()},
        RoutingMetaStoreHandlerTest.class.getClassLoader());
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

  private static void assertCatalogManagementRejected(RoutingMetaStoreHandler handler, String methodName) throws Throwable {
    Method method = Arrays.stream(ThriftHiveMetastore.Iface.class.getMethods())
        .filter(candidate -> candidate.getName().equals(methodName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No HMS method found for " + methodName));

    Object[] args = new Object[method.getParameterCount()];
    Class<?>[] parameterTypes = method.getParameterTypes();
    for (int index = 0; index < parameterTypes.length; index++) {
      args[index] = placeholderArgument(parameterTypes[index], methodName);
    }

    MetaException error = Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, args));
    Assert.assertTrue(error.getMessage().contains("managed by proxy config"));
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

  private static ProxyConfig.CatalogConfig catalogConfigWithExposure(
      String name,
      String description,
      MetastoreRuntimeProfile runtimeProfile,
      String backendStandaloneMetastoreJar,
      ProxyConfig.CatalogExposureMode exposeMode,
      List<String> exposeDbPatterns,
      Map<String, List<String>> exposeTablePatterns,
      Map<String, String> hiveConf
  ) {
    return new ProxyConfig.CatalogConfig(
        name,
        description,
        "file:///" + description,
        false,
        ProxyConfig.CatalogAccessMode.READ_WRITE,
        List.of(),
        exposeMode,
        exposeDbPatterns,
        exposeTablePatterns,
        runtimeProfile,
        backendStandaloneMetastoreJar,
        hiveConf);
  }

  private static Object placeholderArgument(Class<?> parameterType, String methodName) throws Exception {
    if (parameterType == String.class) {
      return "catalog2";
    }
    if (parameterType == Catalog.class) {
      Catalog catalog = new Catalog();
      catalog.setName("catalog2");
      catalog.setLocationUri("file:///catalog2");
      return catalog;
    }
    if (parameterType.isPrimitive()) {
      throw new IllegalArgumentException("Unsupported primitive parameter for " + methodName + ": " + parameterType);
    }
    return parameterType.getConstructor().newInstance();
  }

  private static Table table(String dbName, String tableName, Map<String, String> parameters) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(parameters);
    return table;
  }

  private static LockRequest lockRequest(String dbName, String tableName) {
    LockComponent component = new LockComponent();
    component.setType(LockType.SHARED_READ);
    component.setLevel(LockLevel.TABLE);
    component.setDbname(dbName);
    component.setTablename(tableName);

    LockRequest request = new LockRequest();
    request.setComponent(List.of(component));
    request.setUser("alice");
    request.setHostname("host");
    return request;
  }

  private static LockRequest syntheticReadLockRequest(String dbName, String tableName, long txnId) {
    LockRequest request = lockRequest(dbName, tableName);
    request.setTxnid(txnId);
    request.getComponent().get(0).setOperationType(DataOperationType.SELECT);
    request.getComponent().get(0).setIsTransactional(false);
    return request;
  }

  private static LockRequest syntheticNoTxnDbLockRequest(String dbName, long txnId) {
    LockComponent component = new LockComponent();
    component.setType(LockType.SHARED_READ);
    component.setLevel(LockLevel.DB);
    component.setDbname(dbName);
    component.setOperationType(DataOperationType.NO_TXN);

    LockRequest request = new LockRequest();
    request.setComponent(List.of(component));
    request.setTxnid(txnId);
    request.setUser("alice");
    request.setHostname("host");
    return request;
  }

  private static LockRequest syntheticNoTxnExclusivePartitionLockRequest(
      String dbName,
      String tableName,
      String partitionName,
      long txnId
  ) {
    LockComponent component = new LockComponent();
    component.setType(LockType.EXCLUSIVE);
    component.setLevel(LockLevel.PARTITION);
    component.setDbname(dbName);
    component.setTablename(tableName);
    component.setPartitionname(partitionName);
    component.setOperationType(DataOperationType.NO_TXN);
    component.setIsTransactional(false);

    LockRequest request = new LockRequest();
    request.setComponent(List.of(component));
    request.setTxnid(txnId);
    request.setUser("alice");
    request.setHostname("host");
    return request;
  }

  private static ProxyConfig.SyntheticReadLockStoreConfig syntheticReadLockStoreConfig(String connectString) {
    return new ProxyConfig.SyntheticReadLockStoreConfig(
        ProxyConfig.SyntheticReadLockStoreMode.ZOOKEEPER,
        new ProxyConfig.SyntheticReadLockStoreZooKeeperConfig(
            connectString,
            "/hms-proxy-test-synthetic-read-locks",
            15_000,
            60_000,
            250,
            3));
  }

  private static TestingServer startTestingServerOrSkip() throws Exception {
    try {
      return new TestingServer();
    } catch (Throwable t) {
      if (isLocalPortBindRestriction(t)) {
        Assume.assumeTrue(
            "Skipping ZooKeeper integration test because embedded TestingServer cannot bind a local port in this environment",
            false);
      }
      if (t instanceof Exception exception) {
        throw exception;
      }
      throw (Error) t;
    }
  }

  private static boolean isLocalPortBindRestriction(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof SocketException socketException) {
        String message = socketException.getMessage();
        if (message != null && (
            message.contains("Operation not permitted")
                || message.contains("Permission denied")
                || message.contains("Can't assign requested address"))) {
          return true;
        }
      }
      current = current.getCause();
    }
    return false;
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
