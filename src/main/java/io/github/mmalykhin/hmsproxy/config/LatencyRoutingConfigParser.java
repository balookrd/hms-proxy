package io.github.mmalykhin.hmsproxy.config;

import java.util.Locale;

final class LatencyRoutingConfigParser {
  private LatencyRoutingConfigParser() {
  }

  static LatencyRoutingConfig parse(PropertyReader reader, int catalogCount) {
    boolean backendStatePollingEnabled = reader.getBoolean("routing.backend-state-polling.enabled", false);
    int backendStatePollingIntervalMs =
        reader.getPositiveInt("routing.backend-state-polling.interval-ms", 10_000);
    long backendStatePollingProbeTimeoutMs =
        reader.getPositiveLong("routing.backend-state-polling.probe-timeout-ms", 5_000L);
    boolean adaptiveTimeoutEnabled = reader.getBoolean("routing.adaptive-timeout.enabled", false);
    long adaptiveTimeoutInitialMs = reader.getPositiveLong("routing.adaptive-timeout.initial-ms", 5_000L);
    long adaptiveTimeoutMinMs = reader.getPositiveLong("routing.adaptive-timeout.min-ms", 1_000L);
    long adaptiveTimeoutMaxMs = reader.getPositiveLong("routing.adaptive-timeout.max-ms", 60_000L);
    double adaptiveTimeoutMultiplier = reader.getPositiveDouble("routing.adaptive-timeout.multiplier", 4.0d);
    double adaptiveTimeoutAlpha = reader.getBoundedDouble("routing.adaptive-timeout.alpha", 0.2d, 0.0d, 1.0d);
    boolean circuitBreakerEnabled = reader.getBoolean("routing.circuit-breaker.enabled", false);
    int circuitBreakerFailureThreshold = reader.getPositiveInt("routing.circuit-breaker.failure-threshold", 3);
    long circuitBreakerOpenStateMs = reader.getPositiveLong("routing.circuit-breaker.open-state-ms", 30_000L);
    boolean hedgedReadEnabled = reader.getBoolean("routing.hedged-read.enabled", false);
    long hedgedReadFanoutTimeoutMs = reader.getPositiveLong("routing.hedged-read.fanout-timeout-ms", 30_000L);
    DegradedRoutingPolicy degradedRoutingPolicy = parseDegradedRoutingPolicy(
        reader.getOrNull("routing.degraded-routing-policy"));
    return new LatencyRoutingConfig(
        new BackendStatePollingConfig(
            backendStatePollingEnabled, backendStatePollingIntervalMs, backendStatePollingProbeTimeoutMs),
        new AdaptiveTimeoutConfig(
            adaptiveTimeoutEnabled,
            adaptiveTimeoutInitialMs,
            adaptiveTimeoutMinMs,
            adaptiveTimeoutMaxMs,
            adaptiveTimeoutMultiplier,
            adaptiveTimeoutAlpha),
        new CircuitBreakerConfig(
            circuitBreakerEnabled,
            circuitBreakerFailureThreshold,
            circuitBreakerOpenStateMs),
        new HedgedReadConfig(
            hedgedReadEnabled,
            Math.max(1, Math.min(catalogCount, reader.getPositiveInt(
                "routing.hedged-read.max-parallelism",
                Math.max(1, catalogCount)))),
            hedgedReadFanoutTimeoutMs),
        degradedRoutingPolicy);
  }

  private static DegradedRoutingPolicy parseDegradedRoutingPolicy(String value) {
    if (value == null) {
      return DegradedRoutingPolicy.STRICT;
    }
    try {
      return DegradedRoutingPolicy.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for routing.degraded-routing-policy: " + value
              + ". Expected one of: STRICT, SAFE_FANOUT_READS",
          e);
    }
  }
}
