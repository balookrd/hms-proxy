package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * ZooKeeper connection and bus configuration for distributed cache invalidation.
 */
public record DistributedCacheInvalidationZooKeeperConfig(
    String connectString,
    String znode,
    int connectionTimeoutMs,
    int sessionTimeoutMs,
    int baseSleepMs,
    int maxRetries,
    long eventRetentionMs
) {
  public static final String DEFAULT_ZNODE = "/hms-proxy-cache-invalidation";
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 15_000;
  public static final int DEFAULT_SESSION_TIMEOUT_MS = 60_000;
  public static final int DEFAULT_BASE_SLEEP_MS = 1_000;
  public static final int DEFAULT_MAX_RETRIES = 3;
  public static final long DEFAULT_EVENT_RETENTION_MS = 300_000L; // 5 minutes

  public DistributedCacheInvalidationZooKeeperConfig {
    znode = (znode == null || znode.isBlank()) ? DEFAULT_ZNODE : znode;
    connectionTimeoutMs = connectionTimeoutMs <= 0 ? DEFAULT_CONNECTION_TIMEOUT_MS : connectionTimeoutMs;
    sessionTimeoutMs = sessionTimeoutMs <= 0 ? DEFAULT_SESSION_TIMEOUT_MS : sessionTimeoutMs;
    baseSleepMs = baseSleepMs <= 0 ? DEFAULT_BASE_SLEEP_MS : baseSleepMs;
    maxRetries = maxRetries <= 0 ? DEFAULT_MAX_RETRIES : maxRetries;
    eventRetentionMs = eventRetentionMs <= 0 ? DEFAULT_EVENT_RETENTION_MS : eventRetentionMs;
  }
}
