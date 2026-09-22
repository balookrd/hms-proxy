package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Optional cache for table metadata objects fetched via {@code get_table}, {@code get_table_req},
 * and {@code get_table_objects_by_name}.
 */
public record TableMetadataCacheConfig(
    long ttlMs,
    int maxEntries,
    boolean sharedAcrossUsers
) {
  public static final long DEFAULT_TTL_MS = 0L;
  public static final int DEFAULT_MAX_ENTRIES = 5_000;
  public static final boolean DEFAULT_SHARED_ACROSS_USERS = true;

  public TableMetadataCacheConfig {
    if (ttlMs < 0L) {
      throw new IllegalArgumentException("routing.table-metadata-cache.ttl-ms must be >= 0, got: " + ttlMs);
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("routing.table-metadata-cache.max-entries must be >= 1, got: " + maxEntries);
    }
  }

  public static TableMetadataCacheConfig disabled() {
    return new TableMetadataCacheConfig(DEFAULT_TTL_MS, DEFAULT_MAX_ENTRIES, DEFAULT_SHARED_ACROSS_USERS);
  }
}
