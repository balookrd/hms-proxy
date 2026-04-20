package io.github.mmalykhin.hmsproxy.config;

public record AdaptiveTimeoutConfig(
    boolean enabled,
    long initialTimeoutMs,
    long minTimeoutMs,
    long maxTimeoutMs,
    double multiplier,
    double alpha
) {
  public AdaptiveTimeoutConfig {
    initialTimeoutMs = initialTimeoutMs <= 0 ? 5_000L : initialTimeoutMs;
    minTimeoutMs = minTimeoutMs <= 0 ? 1_000L : minTimeoutMs;
    maxTimeoutMs = maxTimeoutMs <= 0 ? 60_000L : maxTimeoutMs;
    if (maxTimeoutMs < minTimeoutMs) {
      maxTimeoutMs = minTimeoutMs;
    }
    if (initialTimeoutMs < minTimeoutMs) {
      initialTimeoutMs = minTimeoutMs;
    }
    if (initialTimeoutMs > maxTimeoutMs) {
      initialTimeoutMs = maxTimeoutMs;
    }
    multiplier = multiplier <= 1.0d ? 4.0d : multiplier;
    alpha = alpha <= 0.0d || alpha > 1.0d ? 0.2d : alpha;
  }
}
