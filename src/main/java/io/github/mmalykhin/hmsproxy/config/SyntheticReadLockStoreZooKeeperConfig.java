package io.github.mmalykhin.hmsproxy.config;

public record SyntheticReadLockStoreZooKeeperConfig(
    String connectString,
    String znode,
    int connectionTimeoutMs,
    int sessionTimeoutMs,
    int baseSleepMs,
    int maxRetries
) {
  public SyntheticReadLockStoreZooKeeperConfig {
    znode = (znode == null || znode.isBlank()) ? "/hms-proxy-synthetic-read-locks" : znode;
    connectionTimeoutMs = connectionTimeoutMs <= 0 ? 15_000 : connectionTimeoutMs;
    sessionTimeoutMs = sessionTimeoutMs <= 0 ? 60_000 : sessionTimeoutMs;
    baseSleepMs = baseSleepMs <= 0 ? 1_000 : baseSleepMs;
    maxRetries = maxRetries <= 0 ? 3 : maxRetries;
  }
}
