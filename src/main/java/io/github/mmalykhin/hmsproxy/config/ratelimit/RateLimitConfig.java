package io.github.mmalykhin.hmsproxy.config.ratelimit;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public record RateLimitConfig(
    RateLimitPolicyConfig principal,
    RateLimitPolicyConfig source,
    Map<String, SourceCidrRateLimitConfig> sourceCidrs,
    Map<String, RateLimitPolicyConfig> methodFamilies,
    Map<String, RateLimitPolicyConfig> catalogs,
    Map<String, RateLimitPolicyConfig> rpcClasses
) {
  public RateLimitConfig {
    principal = principal == null ? RateLimitPolicyConfig.disabled() : principal;
    source = source == null ? RateLimitPolicyConfig.disabled() : source;
    sourceCidrs = copySourceCidrs(sourceCidrs);
    methodFamilies = copyPolicies(methodFamilies);
    catalogs = copyPolicies(catalogs);
    rpcClasses = copyPolicies(rpcClasses);
  }

  public static RateLimitConfig disabled() {
    return new RateLimitConfig(
        RateLimitPolicyConfig.disabled(),
        RateLimitPolicyConfig.disabled(),
        Map.of(),
        Map.of(),
        Map.of(),
        Map.of());
  }

  public boolean enabled() {
    return principal.enabled()
        || source.enabled()
        || hasEnabledPolicies(sourceCidrs.values().stream().map(SourceCidrRateLimitConfig::policy).toList())
        || hasEnabledPolicies(methodFamilies.values())
        || hasEnabledPolicies(catalogs.values())
        || hasEnabledPolicies(rpcClasses.values());
  }

  private static Map<String, SourceCidrRateLimitConfig> copySourceCidrs(
      Map<String, SourceCidrRateLimitConfig> sourceCidrs
  ) {
    if (sourceCidrs == null || sourceCidrs.isEmpty()) {
      return Map.of();
    }
    Map<String, SourceCidrRateLimitConfig> copied = new LinkedHashMap<>();
    sourceCidrs.forEach((name, config) -> copied.put(name, config));
    return Collections.unmodifiableMap(copied);
  }

  private static Map<String, RateLimitPolicyConfig> copyPolicies(Map<String, RateLimitPolicyConfig> policies) {
    if (policies == null || policies.isEmpty()) {
      return Map.of();
    }
    Map<String, RateLimitPolicyConfig> copied = new LinkedHashMap<>();
    policies.forEach((name, config) -> copied.put(name, config == null ? RateLimitPolicyConfig.disabled() : config));
    return Collections.unmodifiableMap(copied);
  }

  private static boolean hasEnabledPolicies(Iterable<RateLimitPolicyConfig> policies) {
    for (RateLimitPolicyConfig policy : policies) {
      if (policy != null && policy.enabled()) {
        return true;
      }
    }
    return false;
  }
}
