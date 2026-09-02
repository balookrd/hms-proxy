package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Optional short-lived cache for database-name listings such as {@code SHOW DATABASES}.
 *
 * <p>Zero TTL disables the cache. When enabled, repeated listings can return stale names until the
 * TTL expires, so this is intentionally opt-in.
 */
public record DatabaseListCacheConfig(
    long ttlMs,
    int maxEntries,
    boolean sharedAcrossUsers
) {
  public DatabaseListCacheConfig(long ttlMs, int maxEntries) {
    this(ttlMs, maxEntries, false);
  }

  public DatabaseListCacheConfig {
    if (ttlMs < 0L) {
      throw new IllegalArgumentException("routing.database-list-cache.ttl-ms must be >= 0, got: " + ttlMs);
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("routing.database-list-cache.max-entries must be >= 1, got: " + maxEntries);
    }
  }

  public static DatabaseListCacheConfig disabled() {
    return new DatabaseListCacheConfig(0L, 1_000, false);
  }
}
