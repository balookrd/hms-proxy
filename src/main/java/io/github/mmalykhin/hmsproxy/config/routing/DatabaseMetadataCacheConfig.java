package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Optional short-lived cache for database metadata objects fetched via {@code get_database} / {@code get_database_req}.
 *
 * <p>Zero TTL disables the cache. When enabled, repeated database metadata lookups are served from
 * memory until the TTL expires or the database is altered/dropped through the proxy.
 */
public record DatabaseMetadataCacheConfig(
    long ttlMs,
    int maxEntries
) {
  public DatabaseMetadataCacheConfig {
    if (ttlMs < 0L) {
      throw new IllegalArgumentException("routing.database-metadata-cache.ttl-ms must be >= 0, got: " + ttlMs);
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("routing.database-metadata-cache.max-entries must be >= 1, got: " + maxEntries);
    }
  }

  public static DatabaseMetadataCacheConfig disabled() {
    return new DatabaseMetadataCacheConfig(0L, 1_000);
  }
}
