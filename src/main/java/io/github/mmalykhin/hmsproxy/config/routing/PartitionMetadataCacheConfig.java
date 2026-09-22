package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Optional cache for partition metadata objects fetched via {@code get_partitions*},
 * {@code get_partition_specs}, etc.
 */
public record PartitionMetadataCacheConfig(
    long ttlMs,
    int maxEntries,
    boolean sharedAcrossUsers
) {
  public static final long DEFAULT_TTL_MS = 0L;
  public static final int DEFAULT_MAX_ENTRIES = 10_000;
  public static final boolean DEFAULT_SHARED_ACROSS_USERS = true;

  public PartitionMetadataCacheConfig {
    if (ttlMs < 0L) {
      throw new IllegalArgumentException("routing.partition-metadata-cache.ttl-ms must be >= 0, got: " + ttlMs);
    }
    if (maxEntries < 1) {
      throw new IllegalArgumentException("routing.partition-metadata-cache.max-entries must be >= 1, got: " + maxEntries);
    }
  }

  public static PartitionMetadataCacheConfig disabled() {
    return new PartitionMetadataCacheConfig(DEFAULT_TTL_MS, DEFAULT_MAX_ENTRIES, DEFAULT_SHARED_ACROSS_USERS);
  }
}
