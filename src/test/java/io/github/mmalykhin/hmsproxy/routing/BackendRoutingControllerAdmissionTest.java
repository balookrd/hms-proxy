package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendInvocationSession;
import io.github.mmalykhin.hmsproxy.backend.BackendRuntime;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class BackendRoutingControllerAdmissionTest {

  @Test
  public void failedAdaptiveReconnectDoesNotLeaveBackendStuckInHalfOpen() throws Exception {
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(true, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(true, 1, 50L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT));
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        failingReconnectRuntime(config, config.catalogs().get("catalog1"), newSession()));
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, new LinkedHashMap<>(Map.of("catalog1", backend)));

    try (BackendRoutingController controller = new BackendRoutingController(config, router, observability)) {
      observability.runtimeState().recordBackendFailure(
          "catalog1", new TTransportException("backend down"), 5L, config.latencyRouting());
      Assert.assertEquals(
          ProxyRuntimeState.CircuitState.OPEN,
          observability.runtimeState().backendStatus("catalog1").circuitState());
      Thread.sleep(80L);

      MetaException error = Assert.assertThrows(MetaException.class, () -> controller.admit(backend));
      Assert.assertTrue(error.getMessage().contains("unreachable during reconnect"));

      Assert.assertFalse(observability.runtimeState().backendStatus("catalog1").halfOpenInFlight());
      ProxyRuntimeState.BackendCallAdmission next =
          observability.runtimeState().admitBackendCall("catalog1", config.latencyRouting());
      Assert.assertTrue(next.allowed());
      Assert.assertNull(next.rejectionReason());
    }
  }

  private static BackendRuntime failingReconnectRuntime(
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
      ) throws MetaException {
        throw new MetaException("Backend catalog is unreachable during reconnect");
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
      ) throws MetaException {
        throw new MetaException("Backend catalog is unreachable during reconnect");
      }
    };
    return ctor.newInstance(
        proxyConfig,
        catalogConfig,
        new HiveConf(),
        false,
        sessionFactory,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        session,
        null);
  }
}
