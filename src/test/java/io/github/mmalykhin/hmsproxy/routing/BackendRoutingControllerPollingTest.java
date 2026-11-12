package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class BackendRoutingControllerPollingTest {

  @Test
  public void hungProbeOfOneBackendDoesNotStarveTheProbesOfAnother() throws Exception {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("dead", catalogConfig("dead", "dead", null, null, Map.of("hive.metastore.uris", "thrift://dead")));
    catalogs.put("live", catalogConfig("live", "live", null, null, Map.of("hive.metastore.uris", "thrift://live")));
    ProxyConfig config = latencyAwareConfig(
        catalogs,
        new LatencyRoutingConfig(
            // The pool has exactly one thread per backend, as with the default max-parallelism.
            new BackendStatePollingConfig(true, 20, 50L, 2),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 3, 60_000L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT));
    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (CatalogConfig catalog : catalogs.values()) {
      backends.put(catalog.name(), newBackend(
          config, catalog, new ApacheBackendAdapter(), newBackendRuntime(config, catalog, newSession())));
    }
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);

    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger deadProbes = new AtomicInteger();
    AtomicInteger liveProbes = new AtomicInteger();
    BackendProbeRunner.BackendProbe probe = (backend, timeoutMs) -> {
      if (backend.name().equals("dead")) {
        deadProbes.incrementAndGet();
        // Like the metastore client's connect retries: cancellation does not stop it.
        boolean interrupted = false;
        while (release.getCount() > 0) {
          try {
            release.await();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
        return;
      }
      liveProbes.incrementAndGet();
    };

    try (BackendRoutingController controller = new BackendRoutingController(config, router, observability, probe)) {
      Thread.sleep(1_000L);
      ProxyRuntimeState.BackendRuntimeStatus live = observability.runtimeState().backendStatus("live");
      ProxyRuntimeState.BackendRuntimeStatus dead = observability.runtimeState().backendStatus("dead");

      Assert.assertTrue("live must stay healthy across many cycles: " + live,
          live.connected() && !live.degraded());
      Assert.assertTrue("live must keep being probed, got " + liveProbes.get(), liveProbes.get() >= 5);
      // Not even the first cycle, where the dead probe uses up the shared deadline before live is
      // awaited, may count against live.
      Assert.assertEquals("live must never record a failure: " + live, 0L, live.lastFailureEpochSecond());
      Assert.assertEquals("a hung probe is never started twice", 1, deadProbes.get());
      Assert.assertTrue("dead must be reported down: " + dead, dead.degraded());
      // Before close(), which otherwise waits for the hung probe thread.
      release.countDown();
    } finally {
      release.countDown();
    }
  }
}
