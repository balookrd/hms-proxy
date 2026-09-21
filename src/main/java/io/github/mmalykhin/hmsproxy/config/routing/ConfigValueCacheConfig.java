package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Configuration for in-memory caching of metastore configuration values fetched via {@code get_config_value}.
 *
 * <p>A TTL of 0 disables caching. When enabled, metastore configuration values are served from memory,
 * avoiding network round-trips to the backend metastore and bypassing per-user impersonation pools.
 */
public record ConfigValueCacheConfig(
    long ttlMs,
    int maxEntries
) {
  public static final long DEFAULT_TTL_MS = 3_600_000L; // 1 hour
  public static final int DEFAULT_MAX_ENTRIES = 1_000;

  public ConfigValueCacheConfig {
    if (ttlMs < 0L) {
      throw new IllegalArgumentException("routing.config-value-cache.ttl-ms must be >= 0, got: " + ttlMs);
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("routing.config-value-cache.max-entries must be >= 1, got: " + maxEntries);
    }
  }

  public static ConfigValueCacheConfig defaultConfig() {
    return new ConfigValueCacheConfig(DEFAULT_TTL_MS, DEFAULT_MAX_ENTRIES);
  }

  public static ConfigValueCacheConfig disabled() {
    return new ConfigValueCacheConfig(0L, DEFAULT_MAX_ENTRIES);
  }
}
