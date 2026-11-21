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
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TApplicationException;
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

  @Test
  public void applicationErrorsOnALiveConnectionDoNotOpenTheCircuit() {
    LatencyRoutingConfig latencyRouting = new LatencyRoutingConfig(
        new BackendStatePollingConfig(false, 10_000, 5_000L),
        new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(true, 2, 60_000L),
        new HedgedReadConfig(false, 1, 30_000L),
        DegradedRoutingPolicy.STRICT);
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        latencyRouting);
    ProxyRuntimeState state = new ProxyObservability(config).runtimeState();

    for (int i = 0; i < 5; i++) {
      state.recordBackendFailure(
          "catalog1", new TApplicationException(TApplicationException.UNKNOWN_METHOD, "get_table_req"), 5L,
          latencyRouting);
      // How libthrift 0.9.3 reports any server exception of a method whose IDL declares none.
      state.recordBackendFailure(
          "catalog1", new TApplicationException(TApplicationException.INTERNAL_ERROR,
              "Internal error processing open_txns"), 5L, latencyRouting);
    }
    ProxyRuntimeState.BackendRuntimeStatus status = state.backendStatus("catalog1");
    Assert.assertEquals(ProxyRuntimeState.CircuitState.CLOSED, status.circuitState());
    Assert.assertTrue("a backend that answers is connected", status.connected());
    Assert.assertFalse(status.degraded());

    state.recordBackendFailure("catalog1", new TTransportException("connection reset"), 5L, latencyRouting);
    state.recordBackendFailure("catalog1", new TTransportException("connection reset"), 5L, latencyRouting);
    Assert.assertEquals(
        "transport failures must still open the circuit",
        ProxyRuntimeState.CircuitState.OPEN,
        state.backendStatus("catalog1").circuitState());
  }

  @Test
  public void halfOpenAllowsOnlyOneProbeAtATimeAndPreservesProbeFlagAgainstStrayFailures() throws Exception {
    LatencyRoutingConfig latencyRouting = new LatencyRoutingConfig(
        new BackendStatePollingConfig(false, 10_000, 5_000L),
        new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(true, 1, 30L),
        new HedgedReadConfig(false, 1, 30_000L),
        DegradedRoutingPolicy.STRICT);
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        latencyRouting);
    ProxyRuntimeState state = new ProxyObservability(config).runtimeState();

    state.recordBackendFailure("catalog1", new TTransportException("backend down"), 5L, latencyRouting, false);
    Assert.assertEquals(ProxyRuntimeState.CircuitState.OPEN, state.backendStatus("catalog1").circuitState());
    Assert.assertFalse(state.backendStatus("catalog1").connected());

    // Stray application error while circuit is OPEN must not mark the backend as connected.
    state.recordBackendFailure("catalog1", new NoSuchObjectException("missing table"), 5L, latencyRouting, false);
    Assert.assertEquals(ProxyRuntimeState.CircuitState.OPEN, state.backendStatus("catalog1").circuitState());
    Assert.assertFalse(state.backendStatus("catalog1").connected());

    Thread.sleep(50L);
    ProxyRuntimeState.BackendCallAdmission firstProbe = state.admitBackendCall("catalog1", latencyRouting);
    Assert.assertTrue(firstProbe.allowed());
    Assert.assertTrue(firstProbe.halfOpen());
    Assert.assertTrue(state.backendStatus("catalog1").halfOpenInFlight());

    ProxyRuntimeState.BackendCallAdmission rejectedConcurrent = state.admitBackendCall("catalog1", latencyRouting);
    Assert.assertFalse(rejectedConcurrent.allowed());
    Assert.assertEquals("half_open_probe_in_flight", rejectedConcurrent.rejectionReason());

    // A non-probe in-flight request failing with an application error must not clear the probe flag or raise connected=true.
    state.recordBackendFailure("catalog1", new NoSuchObjectException("missing"), 5L, latencyRouting, false);
    Assert.assertEquals(ProxyRuntimeState.CircuitState.HALF_OPEN, state.backendStatus("catalog1").circuitState());
    Assert.assertTrue(state.backendStatus("catalog1").halfOpenInFlight());
    Assert.assertFalse(state.backendStatus("catalog1").connected());

    // When the half-open probe itself fails with an application error, circuit stays HALF_OPEN,
    // halfOpenInFlight clears, connected stays false, and only one next probe is admitted.
    state.recordBackendFailure("catalog1", new NoSuchObjectException("missing"), 5L, latencyRouting, true);
    Assert.assertEquals(ProxyRuntimeState.CircuitState.HALF_OPEN, state.backendStatus("catalog1").circuitState());
    Assert.assertFalse(state.backendStatus("catalog1").halfOpenInFlight());
    Assert.assertFalse(state.backendStatus("catalog1").connected());

    ProxyRuntimeState.BackendCallAdmission secondProbe = state.admitBackendCall("catalog1", latencyRouting);
    Assert.assertTrue(secondProbe.allowed());
    Assert.assertTrue(secondProbe.halfOpen());
    ProxyRuntimeState.BackendCallAdmission concurrentAfterSecondProbe =
        state.admitBackendCall("catalog1", latencyRouting);
    Assert.assertFalse(concurrentAfterSecondProbe.allowed());
    Assert.assertEquals("half_open_probe_in_flight", concurrentAfterSecondProbe.rejectionReason());
  }

  @Test
  public void domainMetaExceptionTimedOutDoesNotOpenCircuitWhileTransportTimeoutDoes() {
    LatencyRoutingConfig latencyRouting = new LatencyRoutingConfig(
        new BackendStatePollingConfig(false, 10_000, 5_000L),
        new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(true, 1, 60_000L),
        new HedgedReadConfig(false, 1, 30_000L),
        DegradedRoutingPolicy.STRICT);
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        latencyRouting);
    ProxyRuntimeState state = new ProxyObservability(config).runtimeState();

    state.recordBackendFailure("catalog1", new MetaException("Lock acquisition timed out"), 5L, latencyRouting, false);
    Assert.assertEquals(ProxyRuntimeState.CircuitState.CLOSED, state.backendStatus("catalog1").circuitState());
    Assert.assertTrue(state.backendStatus("catalog1").connected());

    state.recordBackendFailure(
        "catalog1",
        new TTransportException(TTransportException.TIMED_OUT, new SocketTimeoutException("Read timed out")),
        5L,
        latencyRouting,
        false);
    Assert.assertEquals(ProxyRuntimeState.CircuitState.OPEN, state.backendStatus("catalog1").circuitState());
    Assert.assertFalse(state.backendStatus("catalog1").connected());
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
