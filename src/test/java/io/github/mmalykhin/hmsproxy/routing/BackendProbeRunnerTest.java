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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

// The /readyz prober and background polling share this round. The cross-DC stand saw /readyz
// answer 503 for ~25 s while only the remote catalog was down: its hung probe was awaited first,
// used up the shared deadline, and the default catalog's finished probe was failed with it.
public class BackendProbeRunnerTest {

  private final CountDownLatch release = new CountDownLatch(1);
  private final AtomicInteger deadProbes = new AtomicInteger();
  private ExecutorService executor;
  private List<CatalogBackend> backends;

  @Before
  public void setUp() throws Exception {
    // One thread per backend, as with the default max-parallelism.
    executor = Executors.newFixedThreadPool(2);
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    // Awaited first, so its hang uses up the deadline before "live" is looked at.
    catalogs.put("dead", catalogConfig("dead", "dead", null, null, Map.of("hive.metastore.uris", "thrift://dead")));
    catalogs.put("live", catalogConfig("live", "live", null, null, Map.of("hive.metastore.uris", "thrift://live")));
    ProxyConfig config = latencyAwareConfig(
        catalogs,
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 1_000, 50L, 2),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 3, 60_000L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT));
    backends = new ArrayList<>();
    for (CatalogConfig catalog : catalogs.values()) {
      backends.add(newBackend(
          config, catalog, new ApacheBackendAdapter(), newBackendRuntime(config, catalog, newSession())));
    }
  }

  @After
  public void tearDown() throws Exception {
    release.countDown();
    executor.shutdownNow();
    executor.awaitTermination(5L, TimeUnit.SECONDS);
  }

  @Test
  public void finishedProbeCountsByItsOwnResultAfterAnEarlierProbeUsedUpTheDeadline() {
    BackendProbeRunner runner = new BackendProbeRunner(executor, this::probe);
    Outcomes outcomes = new Outcomes();

    runner.runRound(backends, 50L, outcomes);

    Assert.assertEquals("live answered at once: " + outcomes, "ok", outcomes.of("live"));
    Assert.assertTrue("dead hung past the deadline: " + outcomes,
        outcomes.of("dead").contains("TimeoutException"));
  }

  @Test
  public void hungProbeIsNotStartedAgainByTheNextRound() {
    BackendProbeRunner runner = new BackendProbeRunner(executor, this::probe);
    runner.runRound(backends, 50L, new Outcomes());

    Outcomes second = new Outcomes();
    runner.runRound(backends, 50L, second);

    Assert.assertEquals("a hung probe is never started twice", 1, deadProbes.get());
    Assert.assertTrue("dead is reported without a new probe: " + second,
        second.of("dead").contains("previous probe still running"));
    Assert.assertEquals("live keeps being probed: " + second, "ok", second.of("live"));
  }

  private void probe(CatalogBackend backend, long timeoutMs) {
    if (!backend.name().equals("dead")) {
      return;
    }
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
  }

  private static final class Outcomes implements BackendProbeRunner.ProbeListener {
    private final Map<String, String> byBackend = new ConcurrentHashMap<>();

    @Override
    public void succeeded(CatalogBackend backend, long latencyMs) {
      byBackend.put(backend.name(), "ok");
    }

    @Override
    public void failed(CatalogBackend backend, Throwable error) {
      byBackend.put(backend.name(), String.valueOf(error));
    }

    String of(String backend) {
      return byBackend.getOrDefault(backend, "not reported");
    }

    @Override
    public String toString() {
      return byBackend.toString();
    }
  }
}
