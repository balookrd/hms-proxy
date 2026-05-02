package io.github.mmalykhin.hmsproxy.config.routing;

public record BackendStatePollingConfig(
    boolean enabled,
    int intervalMs,
    long probeTimeoutMs,
    int maxParallelism
) {
  public BackendStatePollingConfig {
    intervalMs = intervalMs <= 0 ? 10_000 : intervalMs;
    probeTimeoutMs = probeTimeoutMs <= 0 ? 5_000L : probeTimeoutMs;
    maxParallelism = maxParallelism <= 0 ? 1 : maxParallelism;
  }

  public BackendStatePollingConfig(boolean enabled, int intervalMs, long probeTimeoutMs) {
    this(enabled, intervalMs, probeTimeoutMs, 1);
  }
}
