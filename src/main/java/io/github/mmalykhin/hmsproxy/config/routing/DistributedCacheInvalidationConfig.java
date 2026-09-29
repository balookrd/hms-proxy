package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Configuration for distributed DDL cache invalidation across multiple hms-proxy replicas.
 */
public record DistributedCacheInvalidationConfig(
    DistributedCacheInvalidationMode mode,
    DistributedCacheInvalidationZooKeeperConfig zooKeeper
) {
  public DistributedCacheInvalidationConfig {
    mode = mode == null ? DistributedCacheInvalidationMode.NONE : mode;
  }

  public static DistributedCacheInvalidationConfig disabled() {
    return new DistributedCacheInvalidationConfig(DistributedCacheInvalidationMode.NONE, null);
  }

  public boolean isZooKeeper() {
    return mode == DistributedCacheInvalidationMode.ZOOKEEPER;
  }
}
