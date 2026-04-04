package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.security.ClientAddressMatcher;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

final class RequestRateLimiter {
  private static final long BUCKET_IDLE_TTL_NANOS = 15L * 60L * 1_000_000_000L;
  private static final Set<String> DDL_METHOD_OVERRIDES = Set.of(
      "add_partition",
      "add_partitions",
      "add_partitions_req",
      "append_partition",
      "append_partition_by_name",
      "append_partition_by_name_with_environment_context",
      "append_partition_with_environment_context",
      "alter_partition",
      "alter_partitions",
      "drop_partition",
      "drop_partition_by_name",
      "drop_partition_by_name_with_environment_context",
      "drop_partition_with_environment_context",
      "rename_partition");

  private final ProxyConfig.RateLimitConfig config;
  private final PrometheusMetrics metrics;
  private final LongSupplier clockNanos;
  private final TokenBucketGroup principalLimits;
  private final TokenBucketGroup sourceLimits;
  private final List<SourceCidrLimit> sourceCidrLimits;
  private final Map<String, TokenBucketGroup> methodFamilyLimits;
  private final Map<String, TokenBucketGroup> catalogLimits;
  private final Map<String, TokenBucketGroup> rpcClassLimits;

  RequestRateLimiter(ProxyConfig config, PrometheusMetrics metrics) {
    this(config.rateLimit(), metrics, System::nanoTime);
  }

  RequestRateLimiter(
      ProxyConfig.RateLimitConfig config,
      PrometheusMetrics metrics,
      LongSupplier clockNanos
  ) {
    this.config = Objects.requireNonNull(config, "config");
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.clockNanos = Objects.requireNonNull(clockNanos, "clockNanos");
    this.principalLimits = new TokenBucketGroup("principal", "default", config.principal());
    this.sourceLimits = new TokenBucketGroup("source", "default", config.source());
    this.sourceCidrLimits = buildSourceCidrLimits(config.sourceCidrs());
    this.methodFamilyLimits = buildGroups("method_family", config.methodFamilies());
    this.catalogLimits = buildGroups("catalog", config.catalogs());
    this.rpcClassLimits = buildGroups("rpc_class", config.rpcClasses());
  }

  boolean enabled() {
    return config.enabled();
  }

  RequestClassification classify(String methodName) {
    return classifyRequest(methodName);
  }

  void enforceRequest(String methodName) throws RateLimitExceededException {
    if (!enabled()) {
      return;
    }
    RequestClassification classification = classifyRequest(methodName);
    long nowNanos = clockNanos.getAsLong();

    String principal = ClientRequestContext.remoteUser().orElse(null);
    if (principal != null && !principal.isBlank()) {
      consume(principalLimits, principal, nowNanos, methodName, classification, null, principal);
    }

    String sourceAddress = ClientRequestContext.remoteAddress().orElse(null);
    if (sourceAddress != null && !sourceAddress.isBlank()) {
      consume(sourceLimits, sourceAddress, nowNanos, methodName, classification, null, sourceAddress);
      for (SourceCidrLimit sourceCidrLimit : sourceCidrLimits) {
        if (sourceCidrLimit.matches(sourceAddress)) {
          consume(
              sourceCidrLimit.group(),
              sourceCidrLimit.name(),
              nowNanos,
              methodName,
              classification,
              null,
              sourceCidrLimit.description());
        }
      }
    }

    consume(methodFamilyLimits.get(classification.methodFamily()),
        classification.methodFamily(),
        nowNanos,
        methodName,
        classification,
        null,
        classification.methodFamily());
    for (String rpcClass : classification.rpcClasses()) {
      consume(rpcClassLimits.get(rpcClass), rpcClass, nowNanos, methodName, classification, null, rpcClass);
    }
  }

  void enforceCatalog(String methodName, String catalogName) throws RateLimitExceededException {
    if (!enabled() || catalogName == null || catalogName.isBlank()) {
      return;
    }
    RequestClassification classification = classifyRequest(methodName);
    consume(
        catalogLimits.get(catalogName),
        catalogName,
        clockNanos.getAsLong(),
        methodName,
        classification,
        catalogName,
        catalogName);
  }

  private void consume(
      TokenBucketGroup group,
      String bucketKey,
      long nowNanos,
      String methodName,
      RequestClassification classification,
      String catalogName,
      String subject
  ) throws RateLimitExceededException {
    if (group == null || !group.enabled() || bucketKey == null || bucketKey.isBlank()) {
      return;
    }
    if (group.tryAcquire(bucketKey, nowNanos)) {
      return;
    }
    metrics.recordRateLimited(
        group.dimension(),
        group.scope(),
        methodName,
        classification.methodFamily(),
        catalogName);
    throw new RateLimitExceededException(rejectionMessage(group, methodName, subject));
  }

  private static String rejectionMessage(TokenBucketGroup group, String methodName, String subject) {
    String qualifier = switch (group.dimension()) {
      case "principal" -> "principal '" + subject + "'";
      case "source" -> "source IP '" + subject + "'";
      case "source_cidr" -> "source CIDR rule '" + subject + "'";
      case "method_family" -> "method family '" + subject + "'";
      case "catalog" -> "catalog '" + subject + "'";
      case "rpc_class" -> "rpc class '" + subject + "'";
      default -> group.dimension() + " '" + subject + "'";
    };
    return "Request rate limit exceeded for " + qualifier
        + " while handling '" + methodName + "'"
        + " (" + group.policy().requestsPerSecond() + " req/s, burst " + group.policy().burst() + ")";
  }

  private static Map<String, TokenBucketGroup> buildGroups(
      String dimension,
      Map<String, ProxyConfig.RateLimitPolicyConfig> policies
  ) {
    if (policies.isEmpty()) {
      return Map.of();
    }
    ConcurrentHashMap<String, TokenBucketGroup> groups = new ConcurrentHashMap<>();
    policies.forEach((scope, policy) -> groups.put(scope, new TokenBucketGroup(dimension, scope, policy)));
    return Map.copyOf(groups);
  }

  private static List<SourceCidrLimit> buildSourceCidrLimits(
      Map<String, ProxyConfig.SourceCidrRateLimitConfig> sourceCidrs
  ) {
    if (sourceCidrs.isEmpty()) {
      return List.of();
    }
    List<SourceCidrLimit> limits = new ArrayList<>(sourceCidrs.size());
    sourceCidrs.forEach((name, config) -> {
      if (config.enabled()) {
        limits.add(new SourceCidrLimit(
            name,
            config.cidrRules(),
            ClientAddressMatcher.parseAll(config.cidrRules()),
            new TokenBucketGroup("source_cidr", name, config.policy())));
      }
    });
    return List.copyOf(limits);
  }

  private static RequestClassification classifyRequest(String methodName) {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe(methodName);
    String canonicalMethod = canonicalize(methodName);
    LinkedHashSet<String> rpcClasses = new LinkedHashSet<>();
    if (operation.mutating()) {
      rpcClasses.add("write");
    }
    if (isDdl(canonicalMethod, operation)) {
      rpcClasses.add("ddl");
    }
    if (isTxn(canonicalMethod)) {
      rpcClasses.add("txn");
    }
    if (isLock(canonicalMethod)) {
      rpcClasses.add("lock");
    }
    return new RequestClassification(operation.operationClass().wireName(), List.copyOf(rpcClasses));
  }

  private static boolean isDdl(String canonicalMethod, HmsOperationRegistry.OperationMetadata operation) {
    if (!operation.mutating()) {
      return false;
    }
    return canonicalMethod.startsWith("create_")
        || canonicalMethod.startsWith("alter_")
        || canonicalMethod.startsWith("drop_")
        || canonicalMethod.startsWith("truncate_")
        || canonicalMethod.startsWith("rename_")
        || canonicalMethod.startsWith("exchange_")
        || DDL_METHOD_OVERRIDES.contains(canonicalMethod);
  }

  private static boolean isTxn(String canonicalMethod) {
    return canonicalMethod.contains("txn")
        || canonicalMethod.contains("write_id")
        || canonicalMethod.contains("writeid")
        || canonicalMethod.contains("compact");
  }

  private static boolean isLock(String canonicalMethod) {
    return canonicalMethod.contains("lock");
  }

  private static String canonicalize(String methodName) {
    String normalized = methodName == null ? "" : methodName.trim();
    StringBuilder builder = new StringBuilder(normalized.length() + 8);
    for (int i = 0; i < normalized.length(); i++) {
      char current = normalized.charAt(i);
      if (Character.isUpperCase(current) && i > 0 && builder.charAt(builder.length() - 1) != '_') {
        builder.append('_');
      }
      builder.append(Character.toLowerCase(current));
    }
    return builder.toString();
  }

  record RequestClassification(
      String methodFamily,
      List<String> rpcClasses
  ) {
  }

  private record SourceCidrLimit(
      String name,
      List<String> cidrRules,
      List<ClientAddressMatcher> matchers,
      TokenBucketGroup group
  ) {
    private boolean matches(String sourceAddress) {
      for (ClientAddressMatcher matcher : matchers) {
        if (matcher.matches(sourceAddress)) {
          return true;
        }
      }
      return false;
    }

    private String description() {
      return name + "=" + String.join(",", cidrRules);
    }
  }

  private static final class TokenBucketGroup {
    private final String dimension;
    private final String scope;
    private final ProxyConfig.RateLimitPolicyConfig policy;
    private final ConcurrentMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();
    private final AtomicLong cleanupTicker = new AtomicLong();

    private TokenBucketGroup(String dimension, String scope, ProxyConfig.RateLimitPolicyConfig policy) {
      this.dimension = dimension;
      this.scope = scope;
      this.policy = policy == null ? ProxyConfig.RateLimitPolicyConfig.disabled() : policy;
    }

    private boolean enabled() {
      return policy.enabled();
    }

    private String dimension() {
      return dimension;
    }

    private String scope() {
      return scope;
    }

    private ProxyConfig.RateLimitPolicyConfig policy() {
      return policy;
    }

    private boolean tryAcquire(String bucketKey, long nowNanos) {
      if (!enabled()) {
        return true;
      }
      cleanupIfNeeded(nowNanos);
      return buckets.computeIfAbsent(bucketKey, ignored -> new TokenBucket(policy.burst(), nowNanos))
          .tryAcquire(nowNanos, policy.requestsPerSecond(), policy.burst());
    }

    private void cleanupIfNeeded(long nowNanos) {
      if ((cleanupTicker.incrementAndGet() & 1023L) != 0L) {
        return;
      }
      for (Map.Entry<String, TokenBucket> entry : buckets.entrySet()) {
        if (entry.getValue().idle(nowNanos, BUCKET_IDLE_TTL_NANOS)) {
          buckets.remove(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  private static final class TokenBucket {
    private double tokens;
    private long lastRefillNanos;
    private long lastSeenNanos;

    private TokenBucket(int burst, long nowNanos) {
      this.tokens = burst;
      this.lastRefillNanos = nowNanos;
      this.lastSeenNanos = nowNanos;
    }

    private synchronized boolean tryAcquire(long nowNanos, int requestsPerSecond, int burst) {
      refill(nowNanos, requestsPerSecond, burst);
      lastSeenNanos = nowNanos;
      if (tokens < 1.0d) {
        return false;
      }
      tokens -= 1.0d;
      return true;
    }

    private synchronized boolean idle(long nowNanos, long idleTtlNanos) {
      return nowNanos - lastSeenNanos >= idleTtlNanos;
    }

    private void refill(long nowNanos, int requestsPerSecond, int burst) {
      if (nowNanos <= lastRefillNanos) {
        return;
      }
      double replenished = (nowNanos - lastRefillNanos) * (double) requestsPerSecond / 1_000_000_000d;
      if (replenished > 0.0d) {
        tokens = Math.min(burst, tokens + replenished);
        lastRefillNanos = nowNanos;
      }
    }
  }
}
