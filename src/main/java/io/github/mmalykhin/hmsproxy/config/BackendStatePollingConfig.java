package io.github.mmalykhin.hmsproxy.config;

public record BackendStatePollingConfig(
    boolean enabled,
    int intervalMs,
    long probeTimeoutMs
) {
  public BackendStatePollingConfig {
    intervalMs = intervalMs <= 0 ? 10_000 : intervalMs;
    probeTimeoutMs = probeTimeoutMs <= 0 ? 5_000L : probeTimeoutMs;
  }
}
