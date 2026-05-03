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

public class RoutingMetaStoreProxyFanoutDegradedTest {
  @Test
  public void safeFanoutReadsCanOmitDegradedBackendResults() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(true, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(true, 1, 200L),
            new HedgedReadConfig(true, 2, 30_000L),
            DegradedRoutingPolicy.SAFE_FANOUT_READS));

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                return List.of("sales");
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                throw new TTransportException("catalog2 unavailable");
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[0]);

    Assert.assertEquals(List.of("sales"), result);
    Assert.assertEquals("degraded", observability.runtimeState().backendStatus("catalog2").degraded() ? "degraded" : "ok");
    Assert.assertTrue(observability.metrics().render().contains(
        "hms_proxy_requests_total{method=\"get_all_databases\",catalog=\"all\",backend=\"fanout\",status=\"degraded\"} 1"));
  }

  @Test
  public void safeFanoutReadsDegradesOnTimeoutWithoutThrowingCancellationException() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(true, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(true, 1, 200L),
            new HedgedReadConfig(true, 2, 100L),
            DegradedRoutingPolicy.SAFE_FANOUT_READS));

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                return List.of("sales");
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                Thread.sleep(5_000L);
                return List.of("reports");
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[0]);

    Assert.assertEquals(List.of("sales"), result);
    Assert.assertTrue(observability.metrics().render().contains(
        "hms_proxy_requests_total{method=\"get_all_databases\",catalog=\"all\",backend=\"fanout\",status=\"degraded\"} 1"));
  }

  @Test
  public void safeFanoutHarvestsReadyResultWhenFirstBackendIsSlow() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(true, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(true, 1, 200L),
            new HedgedReadConfig(true, 2, 100L),
            DegradedRoutingPolicy.SAFE_FANOUT_READS));

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                Thread.sleep(5_000L);
                return List.of("sales");
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                return List.of("reports");
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) handler.invoke(null, method, new Object[0]);

    Assert.assertEquals(List.of("catalog2__reports"), result);
    Assert.assertTrue(observability.metrics().render().contains(
        "hms_proxy_requests_total{method=\"get_all_databases\",catalog=\"all\",backend=\"fanout\",status=\"degraded\"} 1"));
  }

  @Test
  public void circuitBreakerFailsFastAndHalfOpenRetryRecovers() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(true, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(true, 1, 150L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT));

    AtomicInteger backendCalls = new AtomicInteger();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("get_database".equals(method.getName())) {
        if (backendCalls.incrementAndGet() <= 2) {
          throw new TTransportException("backend down");
        }
        Database database = new Database();
        database.setName("sales");
        return database;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), session));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    ProxyObservability observability = new ProxyObservability(config);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    Assert.assertThrows(MetaException.class, () -> handler.invoke(null, method, new Object[] {"sales"}));

    MetaException fastReject = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {"sales"}));
    Assert.assertTrue(fastReject.getMessage().contains("circuit_open"));
    Assert.assertEquals(2, backendCalls.get());

    Thread.sleep(220L);

    Database database = (Database) handler.invoke(null, method, new Object[] {"sales"});

    Assert.assertEquals("sales", database.getName());
    Assert.assertEquals(3, backendCalls.get());
    Assert.assertEquals(
        ProxyRuntimeState.CircuitState.CLOSED,
        observability.runtimeState().backendStatus("catalog1").circuitState());
  }

  @Test
  public void backendStatePollingUpdatesProbeStatus() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(true, 50, 5_000L),
            new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 3, 30_000L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT));

    AtomicInteger probeCalls = new AtomicInteger();
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("getStatus".equals(method.getName())) {
                probeCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));
    ProxyObservability observability = new ProxyObservability(config);

    try (RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability)) {
      Thread.sleep(180L);
    }

    Assert.assertTrue(probeCalls.get() >= 2);
    Assert.assertTrue(observability.runtimeState().backendStatus("catalog1").lastProbeEpochSecond() > 0L);
  }

}
