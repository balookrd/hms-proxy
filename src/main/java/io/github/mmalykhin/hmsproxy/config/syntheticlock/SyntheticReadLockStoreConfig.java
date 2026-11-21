package io.github.mmalykhin.hmsproxy.config.syntheticlock;

import java.util.Objects;

public record SyntheticReadLockStoreConfig(
    SyntheticReadLockStoreMode mode,
    SyntheticReadLockStoreZooKeeperConfig zooKeeper,
    int maxActiveLocks
) {
  public static final int DEFAULT_MAX_ACTIVE_LOCKS = 50_000;

  public SyntheticReadLockStoreConfig {
    Objects.requireNonNull(mode,
        "SyntheticReadLockStoreConfig.mode must be set explicitly (IN_MEMORY or ZOOKEEPER).");
    if (maxActiveLocks < 0) {
      throw new IllegalArgumentException("synthetic-read-lock.max-active-locks must be >= 0");
    }
    zooKeeper = zooKeeper == null
        ? new SyntheticReadLockStoreZooKeeperConfig(null, "/hms-proxy-synthetic-read-locks", 15_000, 60_000, 1_000, 3)
        : zooKeeper;
  }

  public SyntheticReadLockStoreConfig(
      SyntheticReadLockStoreMode mode,
      SyntheticReadLockStoreZooKeeperConfig zooKeeper
  ) {
    this(mode, zooKeeper, DEFAULT_MAX_ACTIVE_LOCKS);
  }

  public static SyntheticReadLockStoreConfig inMemory() {
    return new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null, DEFAULT_MAX_ACTIVE_LOCKS);
  }

  public boolean zooKeeperEnabled() {
    return mode == SyntheticReadLockStoreMode.ZOOKEEPER;
  }
}
