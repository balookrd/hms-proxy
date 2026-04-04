package io.github.mmalykhin.hmsproxy.observability;

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
}
