package io.github.mmalykhin.hmsproxy.config.routing;

public record HedgedReadConfig(
    boolean enabled,
    int maxParallelism,
    long fanoutTimeoutMs
) {
  public HedgedReadConfig {
    maxParallelism = maxParallelism <= 0 ? 8 : maxParallelism;
    fanoutTimeoutMs = fanoutTimeoutMs <= 0 ? 30_000L : fanoutTimeoutMs;
  }
}
