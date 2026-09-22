package io.github.mmalykhin.hmsproxy.config.routing;

public record LatencyRoutingConfig(
    BackendStatePollingConfig backendStatePolling,
    AdaptiveTimeoutConfig adaptiveTimeout,
    CircuitBreakerConfig circuitBreaker,
    HedgedReadConfig hedgedRead,
    DegradedRoutingPolicy degradedRoutingPolicy,
    DatabaseListCacheConfig databaseListCache,
    DatabaseMetadataCacheConfig databaseMetadataCache,
    ConfigValueCacheConfig configValueCache,
    boolean refreshPrivilegesSyntheticSuccess,
    TableMetadataCacheConfig tableMetadataCache,
    PartitionMetadataCacheConfig partitionMetadataCache,
    boolean cacheServeStaleOnError,
    long cacheStaleGracePeriodMs
) {
  public static final boolean DEFAULT_CACHE_SERVE_STALE_ON_ERROR = false;
  public static final long DEFAULT_CACHE_STALE_GRACE_PERIOD_MS = 86_400_000L;

  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy,
        null, null, null, false, null, null, DEFAULT_CACHE_SERVE_STALE_ON_ERROR, DEFAULT_CACHE_STALE_GRACE_PERIOD_MS);
  }

  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy,
      DatabaseListCacheConfig databaseListCache
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy,
        databaseListCache, null, null, false, null, null, DEFAULT_CACHE_SERVE_STALE_ON_ERROR, DEFAULT_CACHE_STALE_GRACE_PERIOD_MS);
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
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy,
        databaseListCache, databaseMetadataCache, null, false, null, null, DEFAULT_CACHE_SERVE_STALE_ON_ERROR, DEFAULT_CACHE_STALE_GRACE_PERIOD_MS);
  }

  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy,
      DatabaseListCacheConfig databaseListCache,
      DatabaseMetadataCacheConfig databaseMetadataCache,
      boolean refreshPrivilegesSyntheticSuccess
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy,
        databaseListCache, databaseMetadataCache, null, refreshPrivilegesSyntheticSuccess, null, null,
        DEFAULT_CACHE_SERVE_STALE_ON_ERROR, DEFAULT_CACHE_STALE_GRACE_PERIOD_MS);
  }

  public LatencyRoutingConfig(
      BackendStatePollingConfig backendStatePolling,
      AdaptiveTimeoutConfig adaptiveTimeout,
      CircuitBreakerConfig circuitBreaker,
      HedgedReadConfig hedgedRead,
      DegradedRoutingPolicy degradedRoutingPolicy,
      DatabaseListCacheConfig databaseListCache,
      DatabaseMetadataCacheConfig databaseMetadataCache,
      ConfigValueCacheConfig configValueCache,
      boolean refreshPrivilegesSyntheticSuccess
  ) {
    this(backendStatePolling, adaptiveTimeout, circuitBreaker, hedgedRead, degradedRoutingPolicy,
        databaseListCache, databaseMetadataCache, configValueCache, refreshPrivilegesSyntheticSuccess,
        null, null, DEFAULT_CACHE_SERVE_STALE_ON_ERROR, DEFAULT_CACHE_STALE_GRACE_PERIOD_MS);
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
    configValueCache =
        configValueCache == null ? ConfigValueCacheConfig.defaultConfig() : configValueCache;
    tableMetadataCache =
        tableMetadataCache == null ? TableMetadataCacheConfig.disabled() : tableMetadataCache;
    partitionMetadataCache =
        partitionMetadataCache == null ? PartitionMetadataCacheConfig.disabled() : partitionMetadataCache;
    cacheStaleGracePeriodMs = Math.max(0L, cacheStaleGracePeriodMs);
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
        ConfigValueCacheConfig.disabled(),
        false,
        TableMetadataCacheConfig.disabled(),
        PartitionMetadataCacheConfig.disabled(),
        DEFAULT_CACHE_SERVE_STALE_ON_ERROR,
        DEFAULT_CACHE_STALE_GRACE_PERIOD_MS);
  }
}
