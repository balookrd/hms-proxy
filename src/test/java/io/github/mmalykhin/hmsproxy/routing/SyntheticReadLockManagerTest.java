package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.syntheticReadLockRequest;
import static io.github.mmalykhin.hmsproxy.routing.SyntheticReadLockStoreTest.storeConfig;

public class SyntheticReadLockManagerTest {
  @Test
  public void txnTimeoutIsReadFromHiveStyleDurationValue() throws Exception {
    try (SyntheticReadLockManager manager = newManager(Map.of("metastore.txn.timeout", "600s"))) {
      Assert.assertEquals(600_000L, manager.timeoutMs());
    }
  }

  @Test
  public void txnTimeoutAcceptsBareSecondsForBackwardCompatibility() throws Exception {
    try (SyntheticReadLockManager manager = newManager(Map.of("metastore.txn.timeout", " 600 "))) {
      Assert.assertEquals(600_000L, manager.timeoutMs());
    }
  }

  @Test
  public void txnTimeoutSupportsMinuteSuffix() throws Exception {
    try (SyntheticReadLockManager manager = newManager(Map.of("metastore.txn.timeout", "10m"))) {
      Assert.assertEquals(600_000L, manager.timeoutMs());
    }
  }

  @Test
  public void txnTimeoutFallsBackToDefaultForUnparsableValue() throws Exception {
    try (SyntheticReadLockManager manager = newManager(Map.of("metastore.txn.timeout", "ten minutes"))) {
      Assert.assertEquals(300_000L, manager.timeoutMs());
    }
  }

  @Test
  public void txnTimeoutFallsBackToDefaultWhenUnset() throws Exception {
    try (SyntheticReadLockManager manager = newManager(Map.of())) {
      Assert.assertEquals(300_000L, manager.timeoutMs());
    }
  }

  @Test
  public void backgroundSweepThreadRunsAndStopsWithTheManager() throws Exception {
    // Other test classes leave daemon sweep threads behind, so compare against a baseline.
    long baseline = sweepThreadCount();
    SyntheticReadLockManager manager = newManager(Map.of());
    try {
      Assert.assertTrue("sweep thread should be running", awaitSweepThreadCount(baseline + 1));
    } finally {
      manager.close();
    }
    Assert.assertTrue("sweep thread should stop after close()", awaitSweepThreadCount(baseline));
  }

  @Test
  public void sweepExpiresLocksAndRepublishesActiveGauge() throws Exception {
    PrometheusMetrics metrics = new PrometheusMetrics();
    try (SyntheticReadLockManager manager = newManager(Map.of("metastore.txn.timeout", "1s"), metrics)) {
      SyntheticReadLockManager.SyntheticLockState state = manager.tryAcquire(
          syntheticReadLockRequest("catalog2__sales", "events", 77L),
          new CatalogRouter.ResolvedNamespace(null, "catalog2", "catalog2__sales", "sales"));

      Assert.assertNotNull(state);
      Assert.assertTrue(metrics.render().contains(
          "hms_proxy_synthetic_read_locks_active{store_mode=\"in_memory\"} 1.0"));

      Thread.sleep(manager.timeoutMs() + 200L);
      manager.runExpiredLockSweep();

      String rendered = metrics.render();
      Assert.assertTrue(rendered.contains(
          "hms_proxy_synthetic_read_lock_events_total{operation=\"cleanup\",catalog=\"all\",store_mode=\"in_memory\",result=\"expired\"} 1"));
      Assert.assertTrue(rendered.contains(
          "hms_proxy_synthetic_read_locks_active{store_mode=\"in_memory\"} 0.0"));
    }
  }

  private static SyntheticReadLockManager newManager(Map<String, String> defaultCatalogHiveConf) {
    return newManager(defaultCatalogHiveConf, new PrometheusMetrics());
  }

  private static SyntheticReadLockManager newManager(
      Map<String, String> defaultCatalogHiveConf,
      PrometheusMetrics metrics
  ) {
    ProxyConfig config = storeConfig(SyntheticReadLockStoreConfig.inMemory(), defaultCatalogHiveConf);
    return new SyntheticReadLockManager(config, metrics);
  }

  private static boolean awaitSweepThreadCount(long expectedCount) throws InterruptedException {
    for (int attempt = 0; attempt < 50; attempt++) {
      if (sweepThreadCount() == expectedCount) {
        return true;
      }
      Thread.sleep(100L);
    }
    return false;
  }

  private static long sweepThreadCount() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .filter(thread -> thread.getName().startsWith("hms-proxy-synthetic-lock-sweep"))
        .count();
  }
}
