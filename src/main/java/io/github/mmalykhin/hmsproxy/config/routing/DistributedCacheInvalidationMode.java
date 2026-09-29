package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Operating mode for multi-instance distributed DDL cache invalidation.
 */
public enum DistributedCacheInvalidationMode {
  /**
   * Distributed cache invalidation is disabled. Cache entries are invalidated only locally
   * within this proxy process.
   */
  NONE,

  /**
   * Distributed cache invalidation is coordinated across proxy replicas via Apache ZooKeeper.
   */
  ZOOKEEPER
}
