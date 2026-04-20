package io.github.mmalykhin.hmsproxy.config.ratelimit;

import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationClass;
public final class RateLimitConfigParser {
  private static final Set<String> SUPPORTED_METHOD_FAMILIES =
      Arrays.stream(HmsOperationClass.values())
          .map(HmsOperationClass::wireName)
          .collect(Collectors.toUnmodifiableSet());
  private static final Set<String> SUPPORTED_RPC_CLASSES = Set.of("write", "ddl", "txn", "lock");

  private RateLimitConfigParser() {
  }

  public static RateLimitConfig parse(
      PropertyReader reader,
      Map<String, CatalogConfig> catalogs
  ) {
    RateLimitPolicyConfig principalRateLimit = parsePolicy(reader, "rate-limit.principal");
    RateLimitPolicyConfig sourceRateLimit = parsePolicy(reader, "rate-limit.source");
    Map<String, SourceCidrRateLimitConfig> sourceCidrRateLimits = parseSourceCidrRateLimits(reader);
    Map<String, RateLimitPolicyConfig> methodFamilyRateLimits =
        parsePolicies(reader, "rate-limit.method-family.", SUPPORTED_METHOD_FAMILIES, true);
    Map<String, RateLimitPolicyConfig> catalogRateLimits =
        parsePolicies(reader, "rate-limit.catalog.", null, false);
    for (String catalogName : catalogRateLimits.keySet()) {
      if (!catalogs.containsKey(catalogName)) {
        throw new IllegalArgumentException("Unknown rate-limit.catalog entry: " + catalogName);
      }
    }
    Map<String, RateLimitPolicyConfig> rpcClassRateLimits =
        parsePolicies(reader, "rate-limit.rpc-class.", SUPPORTED_RPC_CLASSES, true);
    return new RateLimitConfig(
        principalRateLimit,
        sourceRateLimit,
        sourceCidrRateLimits,
        methodFamilyRateLimits,
        catalogRateLimits,
        rpcClassRateLimits);
  }

  private static Map<String, SourceCidrRateLimitConfig> parseSourceCidrRateLimits(PropertyReader reader) {
    String prefix = "rate-limit.source-cidr.";
    Map<String, SourceCidrRateLimitConfig> parsed = new LinkedHashMap<>();
    for (String ruleName : reader.scopedNames(prefix)) {
      String baseKey = prefix + ruleName;
      List<String> cidrRules = Arrays.asList(PropertyReader.splitCsv(reader.get(baseKey + ".cidrs", "")));
      RateLimitPolicyConfig policy = parsePolicy(reader, baseKey);
      if (cidrRules.isEmpty() && !policy.enabled()) {
        continue;
      }
      if (cidrRules.isEmpty()) {
        throw new IllegalArgumentException("Missing required property: " + baseKey + ".cidrs");
      }
      ClientAddressMatcher.parseAll(cidrRules);
      if (!policy.enabled()) {
        throw new IllegalArgumentException(
            baseKey + ".requests-per-second must be >= 1 when " + baseKey + " is configured");
      }
      parsed.put(ruleName, new SourceCidrRateLimitConfig(cidrRules, policy));
    }
    return parsed;
  }

  private static Map<String, RateLimitPolicyConfig> parsePolicies(
      PropertyReader reader,
      String prefix,
      Set<String> allowedNames,
      boolean normalizeToLowerCase
  ) {
    Map<String, RateLimitPolicyConfig> parsed = new LinkedHashMap<>();
    for (String rawName : reader.scopedNames(prefix)) {
      String normalizedName = normalizeToLowerCase ? rawName.toLowerCase(Locale.ROOT) : rawName;
      if (allowedNames != null && !allowedNames.contains(normalizedName)) {
        throw new IllegalArgumentException("Unsupported rate-limit scope '" + rawName + "' under " + prefix);
      }
      RateLimitPolicyConfig policy = parsePolicy(reader, prefix + rawName);
      if (policy.enabled()) {
        parsed.put(normalizedName, policy);
      }
    }
    return parsed;
  }

  private static RateLimitPolicyConfig parsePolicy(PropertyReader reader, String baseKey) {
    boolean rateConfigured = reader.has(baseKey + ".requests-per-second");
    boolean burstConfigured = reader.has(baseKey + ".burst");
    int requestsPerSecond = reader.getNonNegativeInt(baseKey + ".requests-per-second", 0);
    int burst = reader.getNonNegativeInt(baseKey + ".burst", 0);
    if (!rateConfigured && !burstConfigured) {
      return RateLimitPolicyConfig.disabled();
    }
    if (requestsPerSecond < 1) {
      throw new IllegalArgumentException(baseKey + ".requests-per-second must be >= 1");
    }
    if (burstConfigured && burst < 1) {
      throw new IllegalArgumentException(baseKey + ".burst must be >= 1");
    }
    return new RateLimitPolicyConfig(requestsPerSecond, burst);
  }
}
