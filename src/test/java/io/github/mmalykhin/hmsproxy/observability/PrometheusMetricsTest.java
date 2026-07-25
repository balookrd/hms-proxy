package io.github.mmalykhin.hmsproxy.observability;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class PrometheusMetricsTest {
  @Test
  public void rendersConfiguredCountersAndHistogramSamples() {
    PrometheusMetrics metrics = new PrometheusMetrics();

    metrics.recordRequest("get_table", "catalog1", "catalog1", "ok", 0.012);
    metrics.recordBackendFailure("catalog1", new IllegalStateException("boom"));
    metrics.recordBackendFallback("get_table", "APACHE_3_1_3", "HORTONWORKS_3_1_0_3_1_0_78");
    metrics.recordRoutingAmbiguous();
    metrics.recordDefaultCatalogRoute("get_table");
    metrics.recordRateLimited("principal", "default", "get_table", "metadata_read", "catalog1");
    metrics.recordFilteredObject("get_all_tables", "catalog1", "table", 2L);
    metrics.recordSyntheticReadLockEvent("acquire", "catalog2", "zookeeper", "acquired");
    metrics.recordSyntheticReadLockEvent("cleanup", "all", "zookeeper", "expired", 2L);
    metrics.recordSyntheticReadLockStoreFailure("heartbeat", "zookeeper", new IllegalStateException("zk down"));
    metrics.recordSyntheticReadLockHandoff("heartbeat", "catalog2", "zookeeper");
    metrics.setSyntheticReadLocksActive("zookeeper", 7L);
    metrics.setSyntheticReadLockStoreMode("zookeeper");

    String rendered = metrics.render();

    Assert.assertTrue(rendered.contains("hms_proxy_requests_total{method=\"get_table\",catalog=\"catalog1\",backend=\"catalog1\",status=\"ok\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_request_duration_seconds_count{method=\"get_table\",catalog=\"catalog1\",backend=\"catalog1\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_backend_failures_total{backend=\"catalog1\",exception=\"IllegalStateException\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_backend_fallback_total{method=\"get_table\",from_api=\"APACHE_3_1_3\",to_api=\"HORTONWORKS_3_1_0_3_1_0_78\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_routing_ambiguous_total 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_default_catalog_routed_total{method=\"get_table\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_rate_limited_total{dimension=\"principal\",scope=\"default\",method=\"get_table\",method_family=\"metadata_read\",catalog=\"catalog1\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_filtered_objects_total{method=\"get_all_tables\",catalog=\"catalog1\",object_type=\"table\"} 2"));
    Assert.assertTrue(rendered.contains("hms_proxy_synthetic_read_lock_events_total{operation=\"acquire\",catalog=\"catalog2\",store_mode=\"zookeeper\",result=\"acquired\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_synthetic_read_lock_events_total{operation=\"cleanup\",catalog=\"all\",store_mode=\"zookeeper\",result=\"expired\"} 2"));
    Assert.assertTrue(rendered.contains("hms_proxy_synthetic_read_lock_store_failures_total{operation=\"heartbeat\",store_mode=\"zookeeper\",exception=\"IllegalStateException\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_synthetic_read_lock_handoffs_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"zookeeper\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_synthetic_read_locks_active{store_mode=\"zookeeper\"} 7.0"));
    Assert.assertTrue(rendered.contains("hms_proxy_synthetic_read_lock_store_info{store_mode=\"zookeeper\"} 1.0"));
  }

  @Test
  public void unknownExceptionTypesCollapseIntoOtherLabel() {
    PrometheusMetrics metrics = new PrometheusMetrics();

    metrics.recordBackendFailure("catalog1", new ExoticVendorException("synthetic"));
    metrics.recordBackendFailure("catalog1", new ExoticVendorException("synthetic"));
    metrics.recordBackendFailure("catalog1", new IllegalStateException("known"));

    String rendered = metrics.render();

    Assert.assertTrue(rendered.contains(
        "hms_proxy_backend_failures_total{backend=\"catalog1\",exception=\"other\"} 2"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_backend_failures_total{backend=\"catalog1\",exception=\"IllegalStateException\"} 1"));
    Assert.assertFalse(rendered.contains("ExoticVendorException"));
  }

  @Test
  public void labelCardinalityCapRedirectsExcessSeriesToOverflow() {
    PrometheusMetrics metrics = new PrometheusMetrics();
    int cap = PrometheusMetrics.DEFAULT_MAX_SERIES_PER_METRIC;

    for (int index = 0; index < cap; index++) {
      metrics.recordDefaultCatalogRoute("method_" + index);
    }
    metrics.recordDefaultCatalogRoute("method_overflow_a");
    metrics.recordDefaultCatalogRoute("method_overflow_b");

    String rendered = metrics.render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_default_catalog_routed_total{method=\"overflow\"} 2"));
    Assert.assertFalse(rendered.contains("method_overflow_a"));
    Assert.assertFalse(rendered.contains("method_overflow_b"));
  }

  @Test
  public void rendersCumulativeHistogramBucketsWithLeLabelLast() {
    PrometheusMetrics metrics = new PrometheusMetrics();

    metrics.recordRequest("get_table", "catalog1", "catalog1", "ok", 0.012);
    metrics.recordRequest("get_table", "catalog1", "catalog1", "ok", 7.0);

    String rendered = metrics.render();
    String prefix = "hms_proxy_request_duration_seconds_bucket"
        + "{method=\"get_table\",catalog=\"catalog1\",backend=\"catalog1\",le=\"";

    Assert.assertTrue(rendered.contains(prefix + "0.005\"} 0"));
    Assert.assertTrue(rendered.contains(prefix + "0.025\"} 1"));
    Assert.assertTrue(rendered.contains(prefix + "5.0\"} 1"));
    Assert.assertTrue(rendered.contains(prefix + "10.0\"} 2"));
    Assert.assertTrue(rendered.contains(prefix + "+Inf\"} 2"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_request_duration_seconds_sum{method=\"get_table\",catalog=\"catalog1\",backend=\"catalog1\"} 7.012"));
  }

  @Test
  public void escapesQuotesAndNewlinesInsideLabelValues() {
    PrometheusMetrics metrics = new PrometheusMetrics();

    metrics.recordDefaultCatalogRoute("odd\"method\\name\nnext");

    Assert.assertTrue(metrics.render().contains(
        "hms_proxy_default_catalog_routed_total{method=\"odd\\\"method\\\\name\\nnext\"} 1"));
  }

  @Test
  public void concurrentSeriesCreationNeverExceedsCardinalityCap() throws Exception {
    PrometheusMetrics metrics = new PrometheusMetrics();
    int cap = PrometheusMetrics.DEFAULT_MAX_SERIES_PER_METRIC;
    for (int index = 0; index < cap - 1; index++) {
      metrics.recordDefaultCatalogRoute("method_" + index);
    }

    int racers = 32;
    CyclicBarrier startLine = new CyclicBarrier(racers);
    ExecutorService pool = Executors.newFixedThreadPool(racers);
    List<Future<?>> races = new ArrayList<>(racers);
    for (int index = 0; index < racers; index++) {
      String method = "racing_method_" + index;
      races.add(pool.submit(() -> {
        startLine.await(30, TimeUnit.SECONDS);
        metrics.recordDefaultCatalogRoute(method);
        return null;
      }));
    }
    for (Future<?> race : races) {
      race.get(30, TimeUnit.SECONDS);
    }
    pool.shutdown();
    Assert.assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

    List<String> series = seriesOf(metrics.render(), "hms_proxy_default_catalog_routed_total");
    long distinctMethods = series.stream()
        .filter(line -> !line.contains("method=\"overflow\""))
        .count();
    long observedRequests = series.stream()
        .mapToLong(line -> Long.parseLong(line.substring(line.lastIndexOf(' ') + 1)))
        .sum();

    Assert.assertTrue(
        "admitted " + distinctMethods + " distinct series with cap " + cap,
        distinctMethods <= cap);
    Assert.assertEquals(cap - 1 + racers, observedRequests);
  }

  private static List<String> seriesOf(String rendered, String metricName) {
    List<String> series = new ArrayList<>();
    for (String line : rendered.split("\n")) {
      if (line.startsWith(metricName + "{") || line.startsWith(metricName + " ")) {
        series.add(line);
      }
    }
    return series;
  }

  private static final class ExoticVendorException extends RuntimeException {
    private ExoticVendorException(String message) {
      super(message);
    }
  }
}
