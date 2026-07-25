package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.operation.HmsMethodNames;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitPolicyConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.SourceCidrRateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.operation.OperationMetadata;

final class RequestRateLimiter {
  private static final long BUCKET_IDLE_TTL_NANOS = 15L * 60L * 1_000_000_000L;
  private static final long BUCKET_CLEANUP_INTERVAL_NANOS = 60L * 1_000_000_000L;
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

  // Classification is a pure function of the method name; it runs at least twice per request.
  private static final ConcurrentMap<String, RequestClassification> CLASSIFICATION_CACHE =
      new ConcurrentHashMap<>();

  private final RateLimitConfig config;
  private final boolean enabled;
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
      RateLimitConfig config,
      PrometheusMetrics metrics,
      LongSupplier clockNanos
  ) {
    this.config = Objects.requireNonNull(config, "config");
    // Rate limit configuration is immutable after startup, so resolve the aggregate switch once.
    this.enabled = this.config.enabled();
    this.metrics = Objects.requireNonNull(metrics, "metrics");
    this.clockNanos = Objects.requireNonNull(clockNanos, "clockNanos");
    long startNanos = this.clockNanos.getAsLong();
    this.principalLimits = new TokenBucketGroup("principal", "default", config.principal(), startNanos);
    this.sourceLimits = new TokenBucketGroup("source", "default", config.source(), startNanos);
    this.sourceCidrLimits = buildSourceCidrLimits(config.sourceCidrs(), startNanos);
    this.methodFamilyLimits = buildGroups("method_family", config.methodFamilies(), startNanos);
    this.catalogLimits = buildGroups("catalog", config.catalogs(), startNanos);
    this.rpcClassLimits = buildGroups("rpc_class", config.rpcClasses(), startNanos);
  }

  boolean enabled() {
    return enabled;
  }

  RequestClassification classify(String methodName) {
    return classifyRequest(methodName);
  }

  // Visible for tests: idle-bucket pruning is only observable through the bucket population.
  int trackedBucketCount() {
    int total = principalLimits.bucketCount() + sourceLimits.bucketCount();
    for (SourceCidrLimit sourceCidrLimit : sourceCidrLimits) {
      total += sourceCidrLimit.group().bucketCount();
    }
    for (Map<String, TokenBucketGroup> groups
        : List.of(methodFamilyLimits, catalogLimits, rpcClassLimits)) {
      for (TokenBucketGroup group : groups.values()) {
        total += group.bucketCount();
      }
    }
    return total;
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
      byte[] decodedSourceAddress =
          sourceCidrLimits.isEmpty() ? null : ClientAddressMatcher.decodeAddress(sourceAddress);
      for (SourceCidrLimit sourceCidrLimit : sourceCidrLimits) {
        if (sourceCidrLimit.matches(decodedSourceAddress)) {
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
      Map<String, RateLimitPolicyConfig> policies,
      long startNanos
  ) {
    if (policies.isEmpty()) {
      return Map.of();
    }
    ConcurrentHashMap<String, TokenBucketGroup> groups = new ConcurrentHashMap<>();
    policies.forEach(
        (scope, policy) -> groups.put(scope, new TokenBucketGroup(dimension, scope, policy, startNanos)));
    return Map.copyOf(groups);
  }

  private static List<SourceCidrLimit> buildSourceCidrLimits(
      Map<String, SourceCidrRateLimitConfig> sourceCidrs,
      long startNanos
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
            new TokenBucketGroup("source_cidr", name, config.policy(), startNanos)));
      }
    });
    return List.copyOf(limits);
  }

  private static RequestClassification classifyRequest(String methodName) {
    if (methodName == null) {
      return deriveClassification(null);
    }
    return CLASSIFICATION_CACHE.computeIfAbsent(methodName, RequestRateLimiter::deriveClassification);
  }

  private static RequestClassification deriveClassification(String methodName) {
    OperationMetadata operation = HmsOperationPolicy.describe(methodName);
    String canonicalMethod = HmsMethodNames.canonicalize(methodName);
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

  private static boolean isDdl(String canonicalMethod, OperationMetadata operation) {
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
    private boolean matches(byte[] sourceAddress) {
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
    private final RateLimitPolicyConfig policy;
    private final long intervalNanos;
    private final long burstWindowNanos;
    private final ConcurrentMap<String, TokenBucket> buckets = new ConcurrentHashMap<>();
    private final AtomicLong nextCleanupNanos;

    private TokenBucketGroup(String dimension, String scope, RateLimitPolicyConfig policy, long startNanos) {
      this.dimension = dimension;
      this.scope = scope;
      this.nextCleanupNanos = new AtomicLong(startNanos + BUCKET_CLEANUP_INTERVAL_NANOS);
      this.policy = policy == null ? RateLimitPolicyConfig.disabled() : policy;
      if (this.policy.enabled()) {
        this.intervalNanos = 1_000_000_000L / this.policy.requestsPerSecond();
        this.burstWindowNanos = (long) this.policy.burst() * this.intervalNanos;
      } else {
        this.intervalNanos = 0L;
        this.burstWindowNanos = 0L;
      }
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

    private RateLimitPolicyConfig policy() {
      return policy;
    }

    private boolean tryAcquire(String bucketKey, long nowNanos) {
      if (!enabled()) {
        return true;
      }
      cleanupIfNeeded(nowNanos);
      return buckets.computeIfAbsent(bucketKey, ignored -> new TokenBucket(intervalNanos, burstWindowNanos, nowNanos))
          .tryAcquire(nowNanos);
    }

    private int bucketCount() {
      return buckets.size();
    }

    // Bucket keys are unbounded (one per principal, one per source IP), so a burst of one-off
    // clients must not leave its buckets behind. The sweep runs on elapsed time, not on a call
    // counter: a counter stalls exactly when the burst is over and the keys are dead weight.
    private void cleanupIfNeeded(long nowNanos) {
      long due = nextCleanupNanos.get();
      if (nowNanos - due < 0L) {
        return;
      }
      if (!nextCleanupNanos.compareAndSet(due, nowNanos + BUCKET_CLEANUP_INTERVAL_NANOS)) {
        return;
      }
      for (Map.Entry<String, TokenBucket> entry : buckets.entrySet()) {
        if (entry.getValue().idle(nowNanos, BUCKET_IDLE_TTL_NANOS)) {
          buckets.remove(entry.getKey(), entry.getValue());
        }
      }
    }
  }

  // GCRA (virtual-scheduler) token bucket — lock-free via single AtomicLong CAS.
  // nextFreeNanos is the earliest time the next token becomes available.
  // Clamping it to (now - burstWindowNanos) implements the burst allowance.
  private static final class TokenBucket {
    private final long intervalNanos;
    private final long burstWindowNanos;
    private final AtomicLong nextFreeNanos;
    private final AtomicLong lastSeenNanos;

    private TokenBucket(long intervalNanos, long burstWindowNanos, long nowNanos) {
      this.intervalNanos = intervalNanos;
      this.burstWindowNanos = burstWindowNanos;
      // start full: (burst-1) intervals already "consumed" so burst tokens are immediately available
      this.nextFreeNanos = new AtomicLong(nowNanos - burstWindowNanos + intervalNanos);
      this.lastSeenNanos = new AtomicLong(nowNanos);
    }

    private boolean tryAcquire(long nowNanos) {
      lastSeenNanos.setOpaque(nowNanos);
      while (true) {
        long next = nextFreeNanos.get();
        long clamped = Math.max(next, nowNanos - burstWindowNanos);
        if (clamped > nowNanos) {
          return false;
        }
        if (nextFreeNanos.compareAndSet(next, clamped + intervalNanos)) {
          return true;
        }
      }
    }

    private boolean idle(long nowNanos, long idleTtlNanos) {
      return nowNanos - lastSeenNanos.getOpaque() >= idleTtlNanos;
    }
  }
}
