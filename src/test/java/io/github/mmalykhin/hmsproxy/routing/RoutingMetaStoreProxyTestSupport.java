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

final class RoutingMetaStoreProxyTestSupport {
  private RoutingMetaStoreProxyTestSupport() {}

  static final Path HDP_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();
  static final Path HDP_6150_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar").toAbsolutePath();
  static final ProxyConfig CUSTOM_SEPARATOR_CONFIG = ProxyConfig.builder()
      .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
      .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
      .catalogDbSeparator("__")
      .defaultCatalog("catalog1")
      .catalogs(Map.of(
          "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
          "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
      .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
      .build();

  static final CatalogRouter CUSTOM_SEPARATOR_ROUTER = routerFor(CUSTOM_SEPARATOR_CONFIG);

  static CatalogRouter routerFor(ProxyConfig config) {
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String name : config.catalogs().keySet()) {
      backends.put(name, null);
    }
    return new CatalogRouter(config, backends);
  }

  static CatalogBackend newIsolatedHortonworksBackend(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
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

  static CatalogBackend newIsolatedHortonworksBackend(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      Path jar,
      MetastoreRuntimeProfile runtimeProfile,
      java.lang.reflect.InvocationHandler delegateHandler
  ) throws Exception {
    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {jar.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
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

  static CatalogBackend newBackend(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      BackendAdapter adapter,
      BackendRuntime runtime
  ) throws Exception {
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    Constructor<CatalogBackend> ctor = CatalogBackend.class.getDeclaredConstructor(
        ProxyConfig.class,
        CatalogConfig.class,
        HiveConf.class,
        BackendAdapter.class,
        BackendRuntime.class,
        Catalog.class,
        io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics.class);
    ctor.setAccessible(true);
    HiveConf hiveConf = new HiveConf();
    for (Map.Entry<String, String> entry : catalogConfig.hiveConf().entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }
    return ctor.newInstance(proxyConfig, catalogConfig, hiveConf, adapter, runtime, catalog, null);
  }

  static void assertCatalogManagementRejected(RoutingMetaStoreProxy handler, String methodName) throws Throwable {
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
    Assert.assertTrue(error.getMessage().contains("policy-owned by proxy config"));
  }

  static Method thriftMethod(String methodName) {
    return Arrays.stream(ThriftHiveMetastore.Iface.class.getMethods())
        .filter(candidate -> candidate.getName().equals(methodName))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No HMS method found for " + methodName));
  }

  static Object[] dropTableArguments(Method method, String dbName, String tableName) {
    Object[] args = new Object[method.getParameterCount()];
    int stringIndex = 0;
    for (int index = 0; index < method.getParameterCount(); index++) {
      Class<?> parameterType = method.getParameterTypes()[index];
      if (parameterType == String.class) {
        args[index] = stringIndex++ == 0 ? dbName : tableName;
      } else if (parameterType == boolean.class) {
        args[index] = Boolean.TRUE;
      } else if (parameterType == EnvironmentContext.class) {
        args[index] = new EnvironmentContext();
      } else {
        throw new IllegalArgumentException("Unsupported drop_table parameter: " + parameterType);
      }
    }
    return args;
  }

  static ProxyConfig dropPurgeConfig() {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfig(
                "catalog1",
                "c1",
                MetastoreRuntimeProfile.APACHE_3_1_3,
                null,
                Map.of(
                    "hive.metastore.uris", "thrift://one",
                    FileSystemExternalTableDropPurger.ALLOWED_PREFIXES_CONF_KEY, "hdfs://ns-dev3/tmp/"))))
        .compatibility(new CompatibilityConfig(false))
        .federation(new FederationConfig(
            false,
            ViewTextRewriteMode.DISABLED,
            false,
            ExternalTableLocationRewriteMode.DISABLED,
            null,
            ExternalTableDropPurgeMode.BEST_EFFORT))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  static Table externalTable(String dbName, String tableName, String location, boolean purgeEnabled) {
    Table table = table(
        dbName,
        tableName,
        purgeEnabled ? Map.of(FileSystemExternalTableDropPurger.EXTERNAL_TABLE_PURGE_KEY, "true") : Map.of());
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(storageDescriptor(location));
    return table;
  }

  static final class RecordingExternalTableDropPurger implements ExternalTableDropPurger {
    final List<String> events;
    final AtomicReference<Table> preparedTable = new AtomicReference<>();
    final AtomicInteger purgeCalls = new AtomicInteger();
    Optional<PurgeRequest> preparedRequest = Optional.empty();
    Exception purgeFailure;

    RecordingExternalTableDropPurger(List<String> events) {
      this.events = events;
    }

    @Override
    public boolean enabledFor(CatalogBackend backend) {
      return true;
    }

    @Override
    public Optional<PurgeRequest> prepare(CatalogBackend backend, Table table) {
      events.add("prepare");
      preparedTable.set(table);
      return preparedRequest;
    }

    @Override
    public void purge(CatalogBackend backend, PurgeRequest request) throws Exception {
      events.add("purge");
      purgeCalls.incrementAndGet();
      if (purgeFailure != null) {
        throw purgeFailure;
      }
    }
  }

  static Object childTable(ClassLoader classLoader, String dbName, String tableName) throws Exception {
    Object table = Class.forName("org.apache.hadoop.hive.metastore.api.Table", true, classLoader)
        .getConstructor()
        .newInstance();
    table.getClass().getMethod("setDbName", String.class).invoke(table, dbName);
    table.getClass().getMethod("setTableName", String.class).invoke(table, tableName);
    return table;
  }

  static BackendRuntime newBackendRuntime(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      BackendInvocationSession session
  ) throws Exception {
    Constructor<BackendRuntime> ctor = BackendRuntime.class.getDeclaredConstructor(
        ProxyConfig.class,
        CatalogConfig.class,
        HiveConf.class,
        boolean.class,
        BackendRuntime.SessionFactory.class,
        MetastoreRuntimeProfile.class,
        BackendInvocationSession.class,
        io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics.class);
    ctor.setAccessible(true);
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
        return session;
      }
    };
    MetastoreRuntimeProfile profile = catalogConfig.runtimeProfile() != null
        ? catalogConfig.runtimeProfile()
        : MetastoreRuntimeProfile.APACHE_3_1_3;
    return ctor.newInstance(proxyConfig, catalogConfig, new HiveConf(), false, sessionFactory, profile, session, null);
  }

  static BackendInvocationSession newSession() throws Exception {
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

  static RoutingMetaStoreProxy guardedHandler(
      AtomicInteger backendCalls,
      AtomicReference<Table> capturedTable,
      String clientAddressRule
  ) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.REJECT_TRANSACTIONAL, List.of(clientAddressRule)))
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
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
  }

  static ProxyConfig rateLimitedConfig(RateLimitConfig rateLimit) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))))
        .backend(new BackendConfig(Map.of()))
        .compatibility(new CompatibilityConfig(false))
        .federation(new FederationConfig(false, ViewTextRewriteMode.DISABLED, false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
        .management(new ManagementConfig(false, "127.0.0.1", 10083))
        .syntheticReadLockStore(new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null))
        .rateLimit(rateLimit)
        .build();
  }

  static ProxyConfig latencyAwareConfig(
      Map<String, CatalogConfig> catalogs,
      LatencyRoutingConfig latencyRouting
  ) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(catalogs)
        .backend(new BackendConfig(Map.of()))
        .compatibility(new CompatibilityConfig(false))
        .federation(new FederationConfig(false, ViewTextRewriteMode.DISABLED, false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
        .management(new ManagementConfig(false, "127.0.0.1", 10083))
        .syntheticReadLockStore(new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null))
        .rateLimit(RateLimitConfig.disabled())
        .latencyRouting(latencyRouting)
        .build();
  }

  static RoutingMetaStoreProxy accessModeHandler(
      CatalogAccessMode accessMode,
      List<String> writeDbWhitelist,
      AtomicInteger backendCalls,
      AtomicReference<Table> capturedTable
  ) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            new CatalogConfig(
                "catalog1",
                "c1",
                "file:///c1",
                false,
                accessMode,
                writeDbWhitelist,
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://one"))))
        .compatibility(new CompatibilityConfig(false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
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
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
  }

  static RoutingMetaStoreProxy locationRewriteHandler(
      ExternalTableLocationRewriteMode mode,
      String sourceDefaultFs,
      AtomicReference<Table> capturedTable
  ) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            catalogConfig(
                "catalog1", "c1", null, null,
                Map.of("hive.metastore.uris", "thrift://one", "fs.defaultFS", "hdfs://ns-catalog1")),
            "catalog2",
            catalogConfig(
                "catalog2", "c2", null, null,
                Map.of("hive.metastore.uris", "thrift://two", "fs.defaultFS", "hdfs://ns-catalog2"))))
        .compatibility(new CompatibilityConfig(false))
        .federation(new FederationConfig(false, ViewTextRewriteMode.DISABLED, false, mode, sourceDefaultFs))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendInvocationSession backend1Session = newSession((proxy, method, args) -> null);
    BackendInvocationSession backend2Session = newSession((proxy, method, args) -> {
      if (args != null) {
        for (Object argument : args) {
          if (argument instanceof Table table) {
            capturedTable.set(table);
          }
        }
      }
      return null;
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), backend1Session));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), backend2Session));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
  }

  static BackendInvocationSession newSession(java.lang.reflect.InvocationHandler invocationHandler) throws Exception {
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

  static CatalogConfig catalogConfig(
      String name,
      String description,
      MetastoreRuntimeProfile runtimeProfile,
      String backendStandaloneMetastoreJar,
      Map<String, String> hiveConf
  ) {
    return new CatalogConfig(
        name,
        description,
        "file:///" + description,
        false,
        CatalogAccessMode.READ_WRITE,
        List.of(),
        runtimeProfile,
        backendStandaloneMetastoreJar,
        hiveConf);
  }

  static CatalogConfig catalogConfigWithExposure(
      String name,
      String description,
      MetastoreRuntimeProfile runtimeProfile,
      String backendStandaloneMetastoreJar,
      CatalogExposureMode exposeMode,
      List<String> exposeDbPatterns,
      Map<String, List<String>> exposeTablePatterns,
      Map<String, String> hiveConf
  ) {
    return new CatalogConfig(
        name,
        description,
        "file:///" + description,
        false,
        CatalogAccessMode.READ_WRITE,
        List.of(),
        exposeMode,
        exposeDbPatterns,
        exposeTablePatterns,
        runtimeProfile,
        backendStandaloneMetastoreJar,
        hiveConf);
  }

  static Object placeholderArgument(Class<?> parameterType, String methodName) throws Exception {
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

  static Table table(String dbName, String tableName, Map<String, String> parameters) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(parameters);
    return table;
  }

  static StorageDescriptor storageDescriptor(String location) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setLocation(location);
    return storageDescriptor;
  }

  static LockRequest lockRequest(String dbName, String tableName) {
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

  static LockRequest syntheticReadLockRequest(String dbName, String tableName, long txnId) {
    LockRequest request = lockRequest(dbName, tableName);
    request.setTxnid(txnId);
    request.getComponent().get(0).setOperationType(DataOperationType.SELECT);
    request.getComponent().get(0).setIsTransactional(false);
    return request;
  }

  static LockRequest syntheticNoTxnDbLockRequest(String dbName, long txnId) {
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

  static LockRequest syntheticNoTxnExclusivePartitionLockRequest(
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

  static LockComponent noTxnLockComponent(LockType lockType, String dbName, String tableName) {
    LockComponent component = new LockComponent();
    component.setType(lockType);
    component.setLevel(LockLevel.TABLE);
    component.setDbname(dbName);
    component.setTablename(tableName);
    component.setOperationType(DataOperationType.NO_TXN);
    component.setIsTransactional(false);
    return component;
  }

  static LockRequest multiComponentLockRequest(long txnId, LockComponent... components) {
    LockRequest request = new LockRequest();
    request.setComponent(List.of(components));
    request.setTxnid(txnId);
    request.setUser("alice");
    request.setHostname("host");
    return request;
  }

  static SyntheticReadLockStoreConfig syntheticReadLockStoreConfig(String connectString) {
    return new SyntheticReadLockStoreConfig(
        SyntheticReadLockStoreMode.ZOOKEEPER,
        new SyntheticReadLockStoreZooKeeperConfig(
            connectString,
            "/hms-proxy-test-synthetic-read-locks",
            15_000,
            60_000,
            250,
            3));
  }

  static TestingServer startTestingServerOrSkip() throws Exception {
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

  static boolean isLocalPortBindRestriction(Throwable error) {
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

  static final class TestBackendAdapter extends AbstractBackendAdapter {
    private TestBackendAdapter(MetastoreRuntimeProfile runtimeProfile) {
      super(runtimeProfile);
    }

    @Override
    public Object invokeRequest(
        CatalogBackend backend,
        String methodName,
        Object request,
        ImpersonationContext impersonation
    ) throws Throwable {
      return super.invokeRequest(backend, methodName, request, impersonation);
    }
  }

}
