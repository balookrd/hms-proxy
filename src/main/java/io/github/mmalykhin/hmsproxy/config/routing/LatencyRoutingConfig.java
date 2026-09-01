package io.github.mmalykhin.hmsproxy.config.routing;

public record LatencyRoutingConfig(
    BackendStatePollingConfig backendStatePolling,
    AdaptiveTimeoutConfig adaptiveTimeout,
    CircuitBreakerConfig circuitBreaker,
    HedgedReadConfig hedgedRead,
    DegradedRoutingPolicy degradedRoutingPolicy,
    DatabaseListCacheConfig databaseListCache,
    DatabaseMetadataCacheConfig databaseMetadataCache,
    boolean refreshPrivilegesSyntheticSuccess
) {
  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy, null, null, false);
  }

  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy,
      DatabaseListCacheConfig databaseListCache
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy, databaseListCache, null, false);
  }

  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy,
      DatabaseListCacheConfig databaseListCache,
      DatabaseMetadataCacheConfig databaseMetadataCache
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy, databaseListCache, databaseMetadataCache, false);
  }

  public LatencyRoutingConfig {
    backendStatePolling =
        backendStatePolling == null ? new BackendStatePollingConfig(false, 10_000, 5_000L, 1) : backendStatePolling;
    adaptiveTimeout = adaptiveTimeout == null
        ? new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d)
        : adaptiveTimeout;
    circuitBreaker = circuitBreaker == null ? new CircuitBreakerConfig(false, 3, 30_000L) : circuitBreaker;
    hedgedRead = hedgedRead == null ? new HedgedReadConfig(false, 8, 30_000L) : hedgedRead;
    degradedRoutingPolicy =
        degradedRoutingPolicy == null ? DegradedRoutingPolicy.STRICT : degradedRoutingPolicy;
    databaseListCache =
        databaseListCache == null ? DatabaseListCacheConfig.disabled() : databaseListCache;
    databaseMetadataCache =
        databaseMetadataCache == null ? DatabaseMetadataCacheConfig.disabled() : databaseMetadataCache;
  }

  public static LatencyRoutingConfig disabled() {
    return new LatencyRoutingConfig(
        new BackendStatePollingConfig(false, 10_000, 5_000L, 1),
        new AdaptiveTimeoutConfig(false, 5_000L, 1_000L, 60_000L, 4.0d, 0.2d),
        new CircuitBreakerConfig(false, 3, 30_000L),
        new HedgedReadConfig(false, 8, 30_000L),
        DegradedRoutingPolicy.STRICT,
        DatabaseListCacheConfig.disabled(),
        DatabaseMetadataCacheConfig.disabled(),
        false);
  }
}
