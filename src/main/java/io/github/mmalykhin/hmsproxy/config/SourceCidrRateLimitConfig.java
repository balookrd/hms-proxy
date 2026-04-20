package io.github.mmalykhin.hmsproxy.config;

import java.util.List;

public record SourceCidrRateLimitConfig(
    List<String> cidrRules,
    RateLimitPolicyConfig policy
) {
  public SourceCidrRateLimitConfig {
    cidrRules = cidrRules == null ? List.of() : List.copyOf(cidrRules);
    policy = policy == null ? RateLimitPolicyConfig.disabled() : policy;
  }

  public boolean enabled() {
    return !cidrRules.isEmpty() && policy.enabled();
  }
}
