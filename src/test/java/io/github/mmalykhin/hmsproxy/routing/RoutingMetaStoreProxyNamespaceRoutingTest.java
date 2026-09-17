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
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
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

public class RoutingMetaStoreProxyNamespaceRoutingTest {
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

    Assert.assertTrue(error.getMessage().contains("requires a Hortonworks or Hive 4 backend runtime"));
  }

  @Test
  public void setAggrStatsForRoutesToResolvedCatalogAndPreservesWriteState() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<Long> capturedWriteId = new AtomicReference<>();
    AtomicReference<String> capturedValidWriteIds = new AtomicReference<>();

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

    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config,
        config.catalogs().get("catalog2"),
        HDP_JAR,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        (proxy, method, args) -> {
          if ("set_aggr_stats_for".equals(method.getName())) {
            Object req = args[0];
            List<?> colStats = (List<?>) req.getClass().getMethod("getColStats").invoke(req);
            if (colStats != null && !colStats.isEmpty()) {
              Object desc = colStats.get(0).getClass().getMethod("getStatsDesc").invoke(colStats.get(0));
              capturedDb.set((String) desc.getClass().getMethod("getDbName").invoke(desc));
            }
            capturedWriteId.set((Long) req.getClass().getMethod("getWriteId").invoke(req));
            capturedValidWriteIds.set((String) req.getClass().getMethod("getValidWriteIdList").invoke(req));
            return true;
          }
          if ("getVersion".equals(method.getName())) {
            return "3.1.0.3.1.0.0-78";
          }
          throw new UnsupportedOperationException(method.getName());
        });
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest", true, classLoader);
    Class<?> colStatsClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.ColumnStatistics", true, classLoader);
    Class<?> statsDescClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc", true, classLoader);
    Object statsDesc = statsDescClass.getConstructor(boolean.class, String.class, String.class)
        .newInstance(true, "catalog2__sales", "events");
    Object colStats = colStatsClass.getConstructor().newInstance();
    colStatsClass.getMethod("setStatsDesc", statsDescClass).invoke(colStats, statsDesc);
    colStatsClass.getMethod("setStatsObj", List.class).invoke(colStats, List.of());

    Object request = requestClass.getConstructor().newInstance();
    requestClass.getMethod("setColStats", List.class).invoke(request, List.of(colStats));
    requestClass.getMethod("setWriteId", long.class).invoke(request, 42L);
    requestClass.getMethod("setValidWriteIdList", String.class).invoke(request, "catalog2__sales.events:42:1::");

    Object response = handler.set_aggr_stats_for(request);

    Assert.assertEquals(Boolean.TRUE, response);
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals(Long.valueOf(42L), capturedWriteId.get());
    Assert.assertEquals("sales.events:42:1::", capturedValidWriteIds.get());
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

  @Test
  public void getTableMetaRoutesConvertedDoubleDotSchemaPatternToRemoteCatalog() throws Throwable {
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
      if ("get_table_meta".equals(method.getName())) {
        backendCalls.incrementAndGet();
        Assert.assertEquals("sales", args[0]);
        Assert.assertEquals(".*", args[1]);
        return List.of(new TableMeta("sales", "orders", "MANAGED_TABLE"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((p, m, a) -> {
          throw new AssertionError("catalog1 should not be invoked");
        })));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), session));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_table_meta", String.class, String.class, List.class);

    @SuppressWarnings("unchecked")
    List<TableMeta> result = (List<TableMeta>) handler.invoke(null, method, new Object[] {"catalog2..sales", ".*", List.of()});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("catalog2__sales", result.get(0).getDbName());
    Assert.assertEquals("orders", result.get(0).getTableName());
  }

  @Test
  public void getDatabasesRoutesConvertedDoubleDotSchemaPatternToRemoteCatalog() throws Throwable {
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
      if ("get_databases".equals(method.getName())) {
        backendCalls.incrementAndGet();
        Assert.assertEquals("sales", args[0]);
        return List.of("sales");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((p, m, a) -> {
          throw new AssertionError("catalog1 should not be invoked");
        })));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), session));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_databases", String.class);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[] {"catalog2..sales"});

    Assert.assertEquals(1, backendCalls.get());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("catalog2__sales", result.get(0));
  }

  @Test
  public void getTableMetaRoutesDefaultCatalogWithConvertedDotPattern() throws Throwable {
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

    AtomicInteger c1Calls = new AtomicInteger();
    BackendInvocationSession session1 = newSession((proxy, method, args) -> {
      if ("get_table_meta".equals(method.getName())) {
        c1Calls.incrementAndGet();
        Assert.assertEquals("smoke.pattern.db", args[0]);
        Assert.assertEquals("*", args[1]);
        return List.of(new TableMeta("smoke_pattern_db", "tbl1", "MANAGED_TABLE"));
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session1));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((p, m, a) -> {
          throw new AssertionError("catalog2 should not be invoked");
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_table_meta", String.class, String.class, List.class);

    @SuppressWarnings("unchecked")
    List<TableMeta> result = (List<TableMeta>) handler.invoke(null, method, new Object[] {"smoke.pattern.db", "*", null});

    Assert.assertEquals(1, c1Calls.get());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("smoke_pattern_db", result.get(0).getDbName());
    Assert.assertEquals("tbl1", result.get(0).getTableName());
  }

  @Test
  public void getDatabasesRoutesDefaultCatalogWithConvertedDotPattern() throws Throwable {
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

    AtomicInteger c1Calls = new AtomicInteger();
    BackendInvocationSession session1 = newSession((proxy, method, args) -> {
      if ("get_databases".equals(method.getName())) {
        c1Calls.incrementAndGet();
        Assert.assertEquals("smoke.pattern.db", args[0]);
        return List.of("smoke_pattern_db");
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session1));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((p, m, a) -> {
          throw new AssertionError("catalog2 should not be invoked");
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_databases", String.class);

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[] {"smoke.pattern.db"});

    Assert.assertEquals(1, c1Calls.get());
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("smoke_pattern_db", result.get(0));
  }

  @Test
  public void getValidWriteIdsWithEmptyTableListRoutesToDefaultBackend() throws Throwable {
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

    AtomicInteger c1Calls = new AtomicInteger();
    BackendInvocationSession session1 = newSession((proxy, method, args) -> {
      if ("get_valid_write_ids".equals(method.getName())) {
        c1Calls.incrementAndGet();
        GetValidWriteIdsRequest req = (GetValidWriteIdsRequest) args[0];
        Assert.assertTrue(req.getFullTableNames() == null || req.getFullTableNames().isEmpty());
        return new GetValidWriteIdsResponse(List.of());
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session1));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((p, m, a) -> {
          throw new AssertionError("catalog2 should not be invoked");
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_valid_write_ids", GetValidWriteIdsRequest.class);

    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(List.of(), "1:1::");
    Object result = handler.invoke(null, method, new Object[] {request});

    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof GetValidWriteIdsResponse);
    Assert.assertEquals(1, c1Calls.get());
  }

  @Test
  public void getValidWriteIdsWithDefaultCatalogTableRoutesToDefaultBackend() throws Throwable {
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

    AtomicInteger c1Calls = new AtomicInteger();
    BackendInvocationSession session1 = newSession((proxy, method, args) -> {
      if ("get_valid_write_ids".equals(method.getName())) {
        c1Calls.incrementAndGet();
        GetValidWriteIdsRequest req = (GetValidWriteIdsRequest) args[0];
        Assert.assertEquals(List.of("sales.events"), req.getFullTableNames());
        return new GetValidWriteIdsResponse(List.of());
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session1));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((p, m, a) -> {
          throw new AssertionError("catalog2 should not be invoked");
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_valid_write_ids", GetValidWriteIdsRequest.class);

    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(List.of("catalog1__sales.events"), "1:1::");
    Object result = handler.invoke(null, method, new Object[] {request});

    Assert.assertNotNull(result);
    Assert.assertEquals(1, c1Calls.get());
  }

  @Test(expected = MetaException.class)
  public void getValidWriteIdsWithNonDefaultCatalogTableIsRejected() throws Throwable {
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

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((p, m, a) -> {
          throw new AssertionError("catalog1 should not be invoked");
        })));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((p, m, a) -> {
          throw new AssertionError("catalog2 should not be invoked");
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_valid_write_ids", GetValidWriteIdsRequest.class);

    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(List.of("catalog2__sales.events"), "1:1::");
    handler.invoke(null, method, new Object[] {request});
  }

  @Test
  public void alterTableReqRoutesToResolvedCatalogAndRewritesDb() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();
    AtomicReference<Long> capturedWriteId = new AtomicReference<>();

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog2")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(), HDP_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config,
        config.catalogs().get("catalog2"),
        HDP_JAR,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        (proxy, method, args) -> {
          if ("alter_table_req".equals(method.getName())) {
            Object req = args[0];
            capturedDb.set((String) req.getClass().getMethod("getDbName").invoke(req));
            capturedTable.set((String) req.getClass().getMethod("getTableName").invoke(req));
            capturedWriteId.set((Long) req.getClass().getMethod("getWriteId").invoke(req));
            return req.getClass().getClassLoader()
                .loadClass("org.apache.hadoop.hive.metastore.api.AlterTableResponse")
                .getConstructor()
                .newInstance();
          }
          if ("get_table".equals(method.getName()) || "get_table_req".equals(method.getName())) {
            Object table = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.Table").getConstructor().newInstance();
            table.getClass().getMethod("setDbName", String.class).invoke(table, args[0]);
            table.getClass().getMethod("setTableName", String.class).invoke(table, args[1]);
            table.getClass().getMethod("setParameters", Map.class).invoke(table, Map.of());
            return table;
          }
          throw new UnsupportedOperationException(method.getName());
        });

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.AlterTableRequest", true, classLoader);
    Class<?> tableClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.Table", true, classLoader);
    Object table = tableClass.getConstructor().newInstance();
    tableClass.getMethod("setDbName", String.class).invoke(table, "catalog2__sales");
    tableClass.getMethod("setTableName", String.class).invoke(table, "events");
    tableClass.getMethod("setParameters", Map.class).invoke(table, Map.of());

    Object request = requestClass
        .getConstructor(String.class, String.class, tableClass)
        .newInstance("catalog2__sales", "events", table);
    requestClass.getMethod("setWriteId", long.class).invoke(request, 101L);

    Object response = handler.alter_table_req(request);

    Assert.assertNotNull(response);
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("events", capturedTable.get());
    Assert.assertEquals(Long.valueOf(101L), capturedWriteId.get());
  }

  @Test
  public void truncateTableReqRoutesToResolvedCatalogAndRewritesDb() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();
    AtomicReference<Long> capturedWriteId = new AtomicReference<>();

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog2")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_JAR.toString(), HDP_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config,
        config.catalogs().get("catalog2"),
        HDP_JAR,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        (proxy, method, args) -> {
          if ("truncate_table_req".equals(method.getName())) {
            Object req = args[0];
            capturedDb.set((String) req.getClass().getMethod("getDbName").invoke(req));
            capturedTable.set((String) req.getClass().getMethod("getTableName").invoke(req));
            capturedWriteId.set((Long) req.getClass().getMethod("getWriteId").invoke(req));
            return req.getClass().getClassLoader()
                .loadClass("org.apache.hadoop.hive.metastore.api.TruncateTableResponse")
                .getConstructor()
                .newInstance();
          }
          throw new UnsupportedOperationException(method.getName());
        });

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.TruncateTableRequest", true, classLoader);
    Object request = requestClass
        .getConstructor(String.class, String.class)
        .newInstance("catalog2__sales", "events");
    requestClass.getMethod("setWriteId", long.class).invoke(request, 55L);

    Object response = handler.truncate_table_req(request);

    Assert.assertNotNull(response);
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("events", capturedTable.get());
    Assert.assertEquals(Long.valueOf(55L), capturedWriteId.get());
  }

  @Test
  public void getTableStatisticsReqRoutesToResolvedCatalogAndRewritesValidWriteIds() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();
    AtomicReference<String> capturedValidWriteIds = new AtomicReference<>();

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

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    CatalogBackend hdpBackend = newIsolatedHortonworksBackend(
        config,
        config.catalogs().get("catalog2"),
        HDP_JAR,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        (proxy, method, args) -> {
          if ("get_table_statistics_req".equals(method.getName())) {
            Object req = args[0];
            capturedDb.set((String) req.getClass().getMethod("getDbName").invoke(req));
            capturedTable.set((String) req.getClass().getMethod("getTblName").invoke(req));
            capturedValidWriteIds.set((String) req.getClass().getMethod("getValidWriteIdList").invoke(req));
            return req.getClass().getClassLoader()
                .loadClass("org.apache.hadoop.hive.metastore.api.TableStatsResult")
                .getConstructor(List.class)
                .newInstance(List.of());
          }
          throw new UnsupportedOperationException(method.getName());
        });

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hdpBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Class<?> requestClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.TableStatsRequest", true, classLoader);
    Object request = requestClass
        .getConstructor(String.class, String.class, List.class)
        .newInstance("catalog2__sales", "events", List.of("id"));
    requestClass.getMethod("setValidWriteIdList", String.class).invoke(request, "catalog2__sales.events:77:1::");

    Object response = handler.get_table_statistics_req(request);

    Assert.assertNotNull(response);
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("events", capturedTable.get());
    Assert.assertEquals("sales.events:77:1::", capturedValidWriteIds.get());
  }

  @Test
  public void getTableReqRoutesToHive4PreservingColStatsAndRewritesDb() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();
    AtomicReference<String> capturedValidWriteIds = new AtomicReference<>();

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig(
                "catalog2", "c2", MetastoreRuntimeProfile.APACHE_4_1_0, HIVE_4_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://two"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.APACHE_4_1_0, HIVE_4_JAR.toString(), null, false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    ClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(HIVE_4_JAR),
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());

    CatalogBackend hive4Backend = newIsolatedHive4Backend(
        config,
        config.catalogs().get("catalog2"),
        HIVE_4_JAR,
        (proxy, method, args) -> {
          if ("get_table_req".equals(method.getName())) {
            Object req = args[0];
            capturedDb.set((String) req.getClass().getMethod("getDbName").invoke(req));
            capturedTable.set((String) req.getClass().getMethod("getTblName").invoke(req));
            capturedValidWriteIds.set((String) req.getClass().getMethod("getValidWriteIdList").invoke(req));

            ClassLoader cl = req.getClass().getClassLoader();
            Class<?> resClass = cl.loadClass("org.apache.hadoop.hive.metastore.api.GetTableResult");
            Class<?> tableClass = cl.loadClass("org.apache.hadoop.hive.metastore.api.Table");
            Class<?> colStatsClass = cl.loadClass("org.apache.hadoop.hive.metastore.api.ColumnStatistics");
            Class<?> colStatsDescClass = cl.loadClass("org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc");

            Object table = tableClass.getConstructor().newInstance();
            tableClass.getMethod("setDbName", String.class).invoke(table, capturedDb.get());
            tableClass.getMethod("setTableName", String.class).invoke(table, capturedTable.get());

            Object colStatsDesc = colStatsDescClass.getConstructor(boolean.class, String.class, String.class)
                .newInstance(true, capturedDb.get(), capturedTable.get());
            Object colStats = colStatsClass.getConstructor(colStatsDescClass, List.class)
                .newInstance(colStatsDesc, List.of());
            tableClass.getMethod("setColStats", colStatsClass).invoke(table, colStats);

            Object res = resClass.getConstructor(tableClass).newInstance(table);
            resClass.getMethod("setIsStatsCompliant", boolean.class).invoke(res, true);
            return res;
          }
          throw new UnsupportedOperationException(method.getName());
        });

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", hive4Backend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Class<?> requestClass = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.GetTableRequest");
    Object request = requestClass.getConstructor(String.class, String.class)
        .newInstance("catalog2__sales", "events");
    requestClass.getMethod("setGetColumnStats", boolean.class).invoke(request, true);
    requestClass.getMethod("setValidWriteIdList", String.class).invoke(request, "catalog2__sales.events:77:1::");

    Object response = handler.get_table_req(request);

    Assert.assertNotNull(response);
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("events", capturedTable.get());
    Assert.assertEquals("sales.events:77:1::", capturedValidWriteIds.get());

    Assert.assertTrue((boolean) response.getClass().getMethod("isIsStatsCompliant").invoke(response));
    Object table = response.getClass().getMethod("getTable").invoke(response);
    Assert.assertEquals("catalog2__sales", table.getClass().getMethod("getDbName").invoke(table));
    Assert.assertNotNull(table.getClass().getMethod("getColStats").invoke(table));
  }

  @Test
  public void partitionRequestsFallbackOnLegacyApacheBackend() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<String> invokedMethod = new AtomicReference<>();
    AtomicReference<String> capturedDb = new AtomicReference<>();
    AtomicReference<String> capturedTable = new AtomicReference<>();

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

    BackendInvocationSession session = newSession((proxy, method, args) -> {
      invokedMethod.set(method.getName());
      capturedDb.set((String) args[0]);
      capturedTable.set((String) args[1]);
      if ("get_partition".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.Partition part = new org.apache.hadoop.hive.metastore.api.Partition();
        part.setDbName((String) args[0]);
        part.setTableName((String) args[1]);
        part.setValues((List<String>) args[2]);
        return part;
      }
      if ("get_partitions".equals(method.getName()) || "get_partitions_by_names".equals(method.getName())
          || "get_partitions_by_filter".equals(method.getName())) {
        org.apache.hadoop.hive.metastore.api.Partition part = new org.apache.hadoop.hive.metastore.api.Partition();
        part.setDbName((String) args[0]);
        part.setTableName((String) args[1]);
        return List.of(part);
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend legacyBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), session));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", legacyBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(HIVE_4_JAR),
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());

    // 1. get_partition_req
    Class<?> getPartReqClass = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.GetPartitionRequest");
    Object getPartReq = getPartReqClass.getConstructor().newInstance();
    getPartReqClass.getMethod("setDbName", String.class).invoke(getPartReq, "catalog2__sales");
    getPartReqClass.getMethod("setTblName", String.class).invoke(getPartReq, "events");
    getPartReqClass.getMethod("setPartVals", List.class).invoke(getPartReq, List.of("2026-01-01"));
    Object partResp = handler.get_partition_req(getPartReq);
    Assert.assertEquals("get_partition", invokedMethod.get());
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertEquals("events", capturedTable.get());
    Assert.assertNotNull(partResp);
    Object part = partResp.getClass().getMethod("getPartition").invoke(partResp);
    Assert.assertEquals("catalog2__sales", part.getClass().getMethod("getDbName").invoke(part));

    // 2. get_partitions_req
    Class<?> getPartsReqClass = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.PartitionsRequest");
    Object getPartsReq = getPartsReqClass.getConstructor().newInstance();
    getPartsReqClass.getMethod("setDbName", String.class).invoke(getPartsReq, "catalog2__sales");
    getPartsReqClass.getMethod("setTblName", String.class).invoke(getPartsReq, "events");
    getPartsReqClass.getMethod("setMaxParts", short.class).invoke(getPartsReq, (short) 10);
    Object partsResp = handler.get_partitions_req(getPartsReq);
    Assert.assertEquals("get_partitions", invokedMethod.get());
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertNotNull(partsResp);
    List<?> partsList = (List<?>) partsResp.getClass().getMethod("getPartitions").invoke(partsResp);
    Assert.assertEquals(1, partsList.size());
    Assert.assertEquals("catalog2__sales", partsList.get(0).getClass().getMethod("getDbName").invoke(partsList.get(0)));

    // 3. get_partitions_by_names_req
    Class<?> byNamesReqClass = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest");
    Object byNamesReq = byNamesReqClass.getConstructor().newInstance();
    byNamesReqClass.getMethod("setDb_name", String.class).invoke(byNamesReq, "catalog2__sales");
    byNamesReqClass.getMethod("setTbl_name", String.class).invoke(byNamesReq, "events");
    byNamesReqClass.getMethod("setNames", List.class).invoke(byNamesReq, List.of("dt=2026-01-01"));
    Object byNamesResp = handler.get_partitions_by_names_req(byNamesReq);
    Assert.assertEquals("get_partitions_by_names", invokedMethod.get());
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertNotNull(byNamesResp);
    List<?> byNamesParts = (List<?>) byNamesResp.getClass().getMethod("getPartitions").invoke(byNamesResp);
    Assert.assertEquals(1, byNamesParts.size());
    Assert.assertEquals("catalog2__sales", byNamesParts.get(0).getClass().getMethod("getDbName").invoke(byNamesParts.get(0)));

    // 4. get_partitions_by_filter_req
    Class<?> byFilterReqClass = classLoader.loadClass("org.apache.hadoop.hive.metastore.api.GetPartitionsByFilterRequest");
    Object byFilterReq = byFilterReqClass.getConstructor().newInstance();
    byFilterReqClass.getMethod("setDbName", String.class).invoke(byFilterReq, "catalog2__sales");
    byFilterReqClass.getMethod("setTblName", String.class).invoke(byFilterReq, "events");
    byFilterReqClass.getMethod("setFilter", String.class).invoke(byFilterReq, "dt = '2026-01-01'");
    Object byFilterResp = handler.get_partitions_by_filter_req(byFilterReq);
    Assert.assertEquals("get_partitions_by_filter", invokedMethod.get());
    Assert.assertEquals("sales", capturedDb.get());
    Assert.assertNotNull(byFilterResp);
    List<?> byFilterParts = (List<?>) byFilterResp;
    Assert.assertEquals(1, byFilterParts.size());
    Assert.assertEquals("catalog2__sales", byFilterParts.get(0).getClass().getMethod("getDbName").invoke(byFilterParts.get(0)));
  }
}

