package io.github.mmalykhin.hmsproxy.config.routing;

public record LatencyRoutingConfig(
    BackendStatePollingConfig backendStatePolling,
    AdaptiveTimeoutConfig adaptiveTimeout,
    CircuitBreakerConfig circuitBreaker,
    HedgedReadConfig hedgedRead,
    DegradedRoutingPolicy degradedRoutingPolicy
) {
  public LatencyRoutingConfig {
    backendStatePolling =
        backendStatePolling == null ? new BackendStatePollingConfig(false, 10_000, 5_000L) : backendStatePolling;
    adaptiveTimeout = adaptiveTimeout == null
        ? new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d)
        : adaptiveTimeout;
    circuitBreaker = circuitBreaker == null ? new CircuitBreakerConfig(false, 3, 30_000L) : circuitBreaker;
    hedgedRead = hedgedRead == null ? new HedgedReadConfig(false, 8, 30_000L) : hedgedRead;
    degradedRoutingPolicy =
        degradedRoutingPolicy == null ? DegradedRoutingPolicy.STRICT : degradedRoutingPolicy;
  }

  public static LatencyRoutingConfig disabled() {
    return new LatencyRoutingConfig(
        new BackendStatePollingConfig(false, 10_000, 5_000L),
        new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(false, 3, 30_000L),
        new HedgedReadConfig(false, 8, 30_000L),
        DegradedRoutingPolicy.STRICT);
  }
}
