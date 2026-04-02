package io.github.mmalykhin.hmsproxy.observability;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;

public final class ProxyObservability {
  private final PrometheusMetrics metrics;
  private final ProxyRuntimeState runtimeState;

  public ProxyObservability(ProxyConfig config) {
    this.metrics = new PrometheusMetrics();
    this.runtimeState = new ProxyRuntimeState(config);
  }

  public PrometheusMetrics metrics() {
    return metrics;
  }

  public ProxyRuntimeState runtimeState() {
    return runtimeState;
  }
}
