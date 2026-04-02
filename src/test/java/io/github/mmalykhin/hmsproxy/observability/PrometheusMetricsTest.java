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

    String rendered = metrics.render();

    Assert.assertTrue(rendered.contains("hms_proxy_requests_total{method=\"get_table\",catalog=\"catalog1\",backend=\"catalog1\",status=\"ok\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_request_duration_seconds_count{method=\"get_table\",catalog=\"catalog1\",backend=\"catalog1\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_backend_failures_total{backend=\"catalog1\",exception=\"IllegalStateException\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_backend_fallback_total{method=\"get_table\",from_api=\"APACHE_3_1_3\",to_api=\"HORTONWORKS_3_1_0_3_1_0_78\"} 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_routing_ambiguous_total 1"));
    Assert.assertTrue(rendered.contains("hms_proxy_default_catalog_routed_total{method=\"get_table\"} 1"));
  }
}
