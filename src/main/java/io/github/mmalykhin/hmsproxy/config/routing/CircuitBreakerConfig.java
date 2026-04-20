package io.github.mmalykhin.hmsproxy.config.routing;

public record CircuitBreakerConfig(
    boolean enabled,
    int failureThreshold,
    long openStateMs
) {
  public CircuitBreakerConfig {
    failureThreshold = failureThreshold <= 0 ? 3 : failureThreshold;
    openStateMs = openStateMs <= 0 ? 30_000L : openStateMs;
  }
}
