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
import io.github.mmalykhin.hmsproxy.config.routing.ConfigValueCacheConfig;
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

public class RoutingMetaStoreProxyCompatibilityTest {
  @Test
  public void onlyExplicitCompatibilityMethodsUseDefaultBackendPath() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("set_ugi"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("flushCache"));
  }

  @Test
  public void currentNotificationEventIdUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
  }

  @Test
  public void explicitlyListedOperationalMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_all_resource_plans"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("show_locks"));
  }

  @Test
  public void notificationMethodsHaveCompatibilityFallbacks() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_current_notificationEventId"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_next_notification"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_notification_events_count"));
  }

  @Test
  public void refreshPrivilegesUsesContextRoutingButHasCompatibilityFallback() {
    Assert.assertFalse(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("refresh_privileges"));
  }

  @Test
  public void compatibilityFallbackAppliesOnlyWhenTheBackendHasNoSuchMethod() {
    Assert.assertTrue(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "get_next_notification",
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported")));
    Assert.assertTrue(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "show_compact", new TApplicationException(TApplicationException.UNKNOWN_METHOD, "unsupported")));

    Assert.assertFalse(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "get_next_notification", new MetaException("not allowed")));
    Assert.assertFalse(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "refresh_privileges", new TTransportException()));
    Assert.assertFalse(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "show_compact", new TApplicationException(TApplicationException.INTERNAL_ERROR, "boom")));
  }

  @Test
  public void optionalServiceReadsKeepFallingBackOnBackendFailures() {
    Assert.assertTrue(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "get_runtime_stats", new MetaException("backend catalog1 is unavailable")));
    Assert.assertTrue(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "get_all_resource_plans", new TTransportException()));
  }

  @Test
  public void nonCompatibilityMethodsDoNotSilentlyFallback() {
    Assert.assertFalse(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "create_role", new MetaException("boom")));
    Assert.assertFalse(RoutingMetaStoreProxy.shouldUseCompatibilityFallback(
        "get_delegation_token", new MetaException("boom")));
  }

  @Test
  public void serviceReadMethodsUseDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("getMetaConf"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_all_functions"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_metastore_db_uuid"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_open_txns"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_open_txns_info"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("show_locks"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("show_compact"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_active_resource_plan"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_runtime_stats"));
  }

  @Test
  public void backendLocalTxnAndLockMethodsUseDefaultBackendPathWhenCatalogContextIsMissing() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("open_txns"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("commit_txn"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("abort_txn"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("abort_txns"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("check_lock"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("unlock"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("heartbeat"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("heartbeat_txn_range"));
  }

  @Test
  public void partitionValidationWithoutNamespaceUsesDefaultBackendCompatibilityPath() {
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("partition_name_has_valid_characters"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("partition_name_to_spec"));
    Assert.assertTrue(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("partition_name_to_vals"));
  }

  @Test
  public void removedPrefixBasedMethodsNoLongerUseDefaultBackendPath() {
    Assert.assertFalse(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_role_names"));
    Assert.assertFalse(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_all_token_identifiers"));
    Assert.assertFalse(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("get_master_keys"));
    Assert.assertFalse(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("list_roles"));
  }

  @Test
  public void getAllFunctionsWithoutCatalogContextUsesDefaultBackend() throws Throwable {
    GetAllFunctionsResponse backendResponse = new GetAllFunctionsResponse();
    backendResponse.setFunctions(List.of());

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

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
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_functions");

    Object result = handler.invoke(null, method, new Object[0]);

    Assert.assertSame(backendResponse, result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void getMetaConfWithoutCatalogContextUsesDefaultBackend() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

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
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("getMetaConf", String.class);

    Object result = handler.invoke(null, method, new Object[] {"metastore.thrift.uris"});

    Assert.assertEquals("thrift://backend", result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void getMetastoreDbUuidWithoutCatalogContextUsesDefaultBackend() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

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
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_metastore_db_uuid");

    Object result = handler.invoke(null, method, new Object[0]);

    Assert.assertEquals("uuid-1", result);
    Assert.assertEquals(1, backendCalls.get());
  }

  @Test
  public void unrelatedGlobalMethodStillRequiresExplicitHandling() {
    Assert.assertFalse(RoutingMetaStoreProxy.isDefaultBackendGlobalMethod("create_role"));
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
  public void getConfigValueCachesBackendResultAndDoesNotUseImpersonation() throws Throwable {
    CatalogConfig catalog1Config = new CatalogConfig(
        "catalog1",
        "c1",
        "file:///c1",
        true,
        CatalogAccessMode.READ_WRITE,
        List.of(),
        null,
        null,
        Map.of("hive.metastore.uris", "thrift://one"));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, true, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalog1Config))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .latencyRouting(new LatencyRoutingConfig(
            null, null, null, null, null, null, null,
            new ConfigValueCacheConfig(60_000L, 100), false))
        .build();

    AtomicInteger backendCalls = new AtomicInteger();
    AtomicReference<Object[]> capturedArgs = new AtomicReference<>();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_config_value".equals(method.getName())) {
        backendCalls.incrementAndGet();
        capturedArgs.set(args);
        if ("hive.metastore.try.direct.sql".equals(args[0])) {
          return "true";
        }
        // Missing property: metastore echoes back the sentinel default value
        return (String) args[1];
      }
      throw new UnsupportedOperationException(method.getName());
    });

    BackendRuntime.SessionFactory sessionFactory = new BackendRuntime.SessionFactory() {
      @Override
      public BackendInvocationSession open(
          ProxyConfig ignoredProxyConfig,
          CatalogConfig ignoredCatalogConfig,
          HiveConf ignoredHiveConf,
          boolean ignoredBackendKerberosEnabled,
          MetastoreRuntimeProfile ignoredRuntimeProfile
      ) {
        return session;
      }

      @Override
      public BackendInvocationSession openImpersonating(
          ProxyConfig ignoredProxyConfig,
          CatalogConfig ignoredCatalogConfig,
          HiveConf ignoredHiveConf,
          boolean ignoredBackendKerberosEnabled,
          MetastoreRuntimeProfile ignoredRuntimeProfile,
          String ignoredUserName,
          List<String> ignoredGroupNames
      ) {
        throw new AssertionError("openImpersonating must not be called for get_config_value");
      }
    };

    java.lang.reflect.Constructor<BackendRuntime> ctor = BackendRuntime.class.getDeclaredConstructor(
        ProxyConfig.class,
        CatalogConfig.class,
        HiveConf.class,
        boolean.class,
        BackendRuntime.SessionFactory.class,
        MetastoreRuntimeProfile.class,
        BackendInvocationSession.class,
        io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics.class);
    ctor.setAccessible(true);
    BackendRuntime runtime = ctor.newInstance(
        config, catalog1Config, new HiveConf(), false, sessionFactory,
        MetastoreRuntimeProfile.APACHE_3_1_3, session, null);

    CatalogBackend backend = newBackend(
        config,
        catalog1Config,
        new ApacheBackendAdapter(),
        runtime);
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_config_value", String.class, String.class);

    // Simulate an authenticated caller context
    String previousUser = ClientRequestContext.setRemoteUser("impersonated_user");
    try {
      // First call: misses cache, invokes backend with sentinel, passes impersonation=null to dispatcher
      Object first = handler.invoke(null, method, new Object[]{"hive.metastore.try.direct.sql", "false"});
      Assert.assertEquals("true", first);
      Assert.assertEquals(1, backendCalls.get());
      Assert.assertNotNull(capturedArgs.get());
      Assert.assertEquals("hive.metastore.try.direct.sql", capturedArgs.get()[0]);
      Assert.assertEquals(ConfigValueCache.SENTINEL, capturedArgs.get()[1]);

      // Second call: served from in-memory cache, no backend call
      Object second = handler.invoke(null, method, new Object[]{"hive.metastore.try.direct.sql", "false"});
      Assert.assertEquals("true", second);
      Assert.assertEquals(1, backendCalls.get());

      // Third call: missing key on backend (backend echoes sentinel)
      Object third = handler.invoke(null, method, new Object[]{"custom.missing.key", "custom_default_1"});
      Assert.assertEquals("custom_default_1", third);
      Assert.assertEquals(2, backendCalls.get());
      Assert.assertEquals(ConfigValueCache.SENTINEL, capturedArgs.get()[1]);

      // Fourth call: missing key served from cache, returns new default without calling backend
      Object fourth = handler.invoke(null, method, new Object[]{"custom.missing.key", "custom_default_2"});
      Assert.assertEquals("custom_default_2", fourth);
      Assert.assertEquals(2, backendCalls.get());
    } finally {
      ClientRequestContext.restoreRemoteUser(previousUser);
    }
  }

  @Test
  public void partitionNameToSpecAndValsAreHandledLocallyWithoutBackendCall() throws Throwable {
    Map<String, String> expectedSpec = Map.of("time_key", "2024-01-01");
    List<String> expectedVals = List.of("2024-01-01");

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
      backendCalls.incrementAndGet();
      throw new AssertionError("Backend must not be invoked for local partition syntax operations: " + method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend1, "catalog2", backend2)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method specMethod = ThriftHiveMetastore.Iface.class.getMethod("partition_name_to_spec", String.class);
    Method valsMethod = ThriftHiveMetastore.Iface.class.getMethod("partition_name_to_vals", String.class);

    Object specResult = handler.invoke(null, specMethod, new Object[]{"time_key=2024-01-01"});
    Assert.assertEquals(expectedSpec, specResult);

    Object valsResult = handler.invoke(null, valsMethod, new Object[]{"time_key=2024-01-01"});
    Assert.assertEquals(expectedVals, valsResult);

    Assert.assertEquals(0, backendCalls.get());
  }

}

