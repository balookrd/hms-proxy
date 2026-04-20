package io.github.mmalykhin.hmsproxy.config;

public record RateLimitPolicyConfig(
    int requestsPerSecond,
    int burst
) {
  public RateLimitPolicyConfig {
    requestsPerSecond = Math.max(requestsPerSecond, 0);
    burst = burst <= 0 ? requestsPerSecond : burst;
  }

  public static RateLimitPolicyConfig disabled() {
    return new RateLimitPolicyConfig(0, 0);
  }

  public boolean enabled() {
    return requestsPerSecond > 0 && burst > 0;
  }
}
