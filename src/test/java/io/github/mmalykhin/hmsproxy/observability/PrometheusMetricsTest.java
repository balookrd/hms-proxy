package io.github.mmalykhin.hmsproxy.observability;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.transport.TTransportException;
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
  public void rendersRestListenerSeriesAfterRecordingRestRequestsAndListenerInfo() {
    PrometheusMetrics metrics = new PrometheusMetrics();

    metrics.recordRestRequest("hdp", "load_table", 200, 0.05);
    metrics.setRestListenerInfo("0.0.0.0", 9183);

    String rendered = metrics.render();

    Assert.assertTrue(rendered.contains(
        "hms_proxy_rest_requests_total{prefix=\"hdp\",route=\"load_table\",status=\"200\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_rest_request_duration_seconds_count{prefix=\"hdp\",route=\"load_table\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_rest_listener_info{bind_host=\"0.0.0.0\",port=\"9183\"} 1"));
  }

  @Test
  public void reportsProcessLivenessWithoutBoundRuntimeState() {
    String rendered = new PrometheusMetrics().render();

    Assert.assertTrue(rendered, rendered.contains("hms_proxy_up 1.0"));
    Assert.assertFalse(rendered, rendered.contains("hms_proxy_backend_up{"));
  }

  @Test
  public void exportsBackendHealthFromRuntimeStateOnEveryRender() {
    LatencyRoutingConfig latencyRouting = new LatencyRoutingConfig(
        new BackendStatePollingConfig(true, 10_000, 5_000L),
        new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(true, 1, 60_000L),
        new HedgedReadConfig(false, 1, 30_000L),
        DegradedRoutingPolicy.STRICT);
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of())
        .latencyRouting(latencyRouting)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
    ProxyObservability observability = new ProxyObservability(config);
    ProxyRuntimeState state = observability.runtimeState();

    state.recordBackendProbeSuccess("catalog1", 3L, latencyRouting);
    state.recordBackendProbeSuccess("catalog2", 3L, latencyRouting);
    String healthy = observability.metrics().render();

    Assert.assertTrue(healthy, healthy.contains("hms_proxy_up 1.0"));
    Assert.assertTrue(healthy, healthy.contains(
        "hms_proxy_start_time_seconds " + (double) state.startedAtEpochSecond()));
    Assert.assertTrue(healthy, healthy.contains("hms_proxy_backend_up{backend=\"catalog1\"} 1.0"));
    Assert.assertTrue(healthy, healthy.contains("hms_proxy_backend_up{backend=\"catalog2\"} 1.0"));
    Assert.assertTrue(healthy, healthy.contains(
        "hms_proxy_backend_last_probe_timestamp_seconds{backend=\"catalog2\"} "
            + (double) state.backendStatus("catalog2").lastProbeEpochSecond()));
    Assert.assertTrue(state.backendStatus("catalog2").lastProbeEpochSecond() > 0L);

    // A transport failure opens the circuit (threshold 1); the next scrape must see it, not a
    // value cached from the previous render.
    state.recordBackendProbeFailure("catalog2", new TTransportException("connection refused"), latencyRouting);
    String failed = observability.metrics().render();

    Assert.assertTrue(failed, failed.contains("hms_proxy_backend_up{backend=\"catalog1\"} 1.0"));
    Assert.assertTrue(failed, failed.contains("hms_proxy_backend_up{backend=\"catalog2\"} 0.0"));
    Assert.assertTrue(failed, failed.contains(
        "hms_proxy_backend_circuit_state{backend=\"catalog2\",state=\"open\"} 1.0"));
    Assert.assertTrue(failed, failed.contains(
        "hms_proxy_backend_circuit_state{backend=\"catalog2\",state=\"closed\"} 0.0"));
    Assert.assertTrue(failed, failed.contains(
        "hms_proxy_backend_circuit_state{backend=\"catalog1\",state=\"closed\"} 1.0"));
  }

  @Test
  public void backendStaysDownWhileDegradedEvenAfterANonTransportFailure() {
    LatencyRoutingConfig latencyRouting = new LatencyRoutingConfig(
        new BackendStatePollingConfig(true, 10_000, 5_000L),
        new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(false, 1, 60_000L),
        new HedgedReadConfig(false, 1, 30_000L),
        DegradedRoutingPolicy.STRICT);
    ProxyObservability observability = new ProxyObservability(ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of())
        .latencyRouting(latencyRouting)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build());
    ProxyRuntimeState state = observability.runtimeState();

    state.recordBackendProbeSuccess("catalog1", 3L, latencyRouting);
    state.recordBackendProbeFailure("catalog1", new TTransportException("connection refused"), latencyRouting);
    // What a probe sees once the backend host is gone: the Hive client reports the transport
    // failure as a MetaException without a cause, so connected flips back to true.
    state.recordBackendProbeFailure("catalog1",
        new MetaException("Could not connect to meta store"), latencyRouting);
    Assert.assertTrue(state.backendStatus("catalog1").connected());
    Assert.assertTrue(state.backendStatus("catalog1").degraded());

    String rendered = observability.metrics().render();
    Assert.assertTrue(rendered, rendered.contains("hms_proxy_backend_up{backend=\"catalog1\"} 0.0"));

    state.recordBackendProbeSuccess("catalog1", 3L, latencyRouting);
    rendered = observability.metrics().render();
    Assert.assertTrue(rendered, rendered.contains("hms_proxy_backend_up{backend=\"catalog1\"} 1.0"));
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

  @Test
  public void rendersCacheAndRangerMetrics() {
    PrometheusMetrics metrics = new PrometheusMetrics();

    metrics.recordCacheRequest("database_list", "hdp", "hit");
    metrics.recordCacheRequest("database_metadata", "hdp", "miss");
    metrics.setCacheEntries("database_list", "hdp", 12L);
    metrics.setCacheEntries("database_metadata", "hdp", 45L);
    metrics.recordCacheInvalidation("database_list", "hdp", "write", 3L);
    metrics.recordCacheInvalidation("database_metadata", "all", "prune", 10L);
    metrics.recordCacheRefresh("database_list", "hdp", "success", 2L);
    metrics.recordCacheRefresh("database_metadata", "hdp", "failure", 1L);

    metrics.recordRangerEvaluation("hdp", "database", "select", "allowed", 0.00025);
    metrics.recordRangerEvaluation("hdp", "table", "select", "denied", 0.00035);
    metrics.recordRangerFilteredObjects("hdp", "database", 5L);
    metrics.recordRangerFilteredObjects("hdp", "table", 8L);
    metrics.setRangerPluginInfo("hdp", "hive_service", "hive", "hms-proxy");

    String rendered = metrics.render();

    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_requests_total{cache=\"database_list\",catalog=\"hdp\",result=\"hit\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_requests_total{cache=\"database_metadata\",catalog=\"hdp\",result=\"miss\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_entries{cache=\"database_list\",catalog=\"hdp\"} 12.0"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_entries{cache=\"database_metadata\",catalog=\"hdp\"} 45.0"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_invalidations_total{cache=\"database_list\",catalog=\"hdp\",reason=\"write\"} 3"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_invalidations_total{cache=\"database_metadata\",catalog=\"all\",reason=\"prune\"} 10"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_refreshes_total{cache=\"database_list\",catalog=\"hdp\",result=\"success\"} 2"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_refreshes_total{cache=\"database_metadata\",catalog=\"hdp\",result=\"failure\"} 1"));

    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_evaluations_total{catalog=\"hdp\",resource_type=\"database\",access_type=\"select\",result=\"allowed\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_evaluations_total{catalog=\"hdp\",resource_type=\"table\",access_type=\"select\",result=\"denied\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_evaluation_duration_seconds_count{catalog=\"hdp\",resource_type=\"database\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_filtered_objects_total{catalog=\"hdp\",resource_type=\"database\"} 5"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_filtered_objects_total{catalog=\"hdp\",resource_type=\"table\"} 8"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_ranger_plugin_info{catalog=\"hdp\",service_name=\"hive_service\",service_type=\"hive\",app_id=\"hms-proxy\"} 1.0"));
  }

  @Test
  public void rendersJvmMemoryMetrics() {
    PrometheusMetrics metrics = new PrometheusMetrics();
    String rendered = metrics.render();

    Assert.assertTrue(rendered.contains("# HELP hms_proxy_jvm_memory_used_bytes"));
    Assert.assertTrue(rendered.contains("# TYPE hms_proxy_jvm_memory_used_bytes gauge"));
    Assert.assertTrue(rendered.contains("hms_proxy_jvm_memory_used_bytes{area=\"heap\"}"));
    Assert.assertTrue(rendered.contains("hms_proxy_jvm_memory_used_bytes{area=\"nonheap\"}"));

    Assert.assertTrue(rendered.contains("# HELP hms_proxy_jvm_memory_committed_bytes"));
    Assert.assertTrue(rendered.contains("hms_proxy_jvm_memory_committed_bytes{area=\"heap\"}"));

    Assert.assertTrue(rendered.contains("# HELP hms_proxy_jvm_memory_max_bytes"));
    Assert.assertTrue(rendered.contains("hms_proxy_jvm_memory_max_bytes{area=\"heap\"}"));

    Assert.assertTrue(rendered.contains("# HELP hms_proxy_jvm_memory_pool_used_bytes"));
    Assert.assertTrue(rendered.contains("hms_proxy_jvm_memory_pool_used_bytes{"));

    Assert.assertTrue(rendered.contains("# HELP hms_proxy_jvm_buffer_pool_used_bytes"));
    Assert.assertTrue(rendered.contains("hms_proxy_jvm_buffer_pool_used_bytes{pool=\"direct\"}"));
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
