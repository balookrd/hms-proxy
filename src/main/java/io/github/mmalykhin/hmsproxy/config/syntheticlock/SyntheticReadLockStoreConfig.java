package io.github.mmalykhin.hmsproxy.config.syntheticlock;

import java.util.Objects;

public record SyntheticReadLockStoreConfig(
    SyntheticReadLockStoreMode mode,
    SyntheticReadLockStoreZooKeeperConfig zooKeeper
) {
  public SyntheticReadLockStoreConfig {
    Objects.requireNonNull(mode,
        "SyntheticReadLockStoreConfig.mode must be set explicitly (IN_MEMORY or ZOOKEEPER).");
    zooKeeper = zooKeeper == null
        ? new SyntheticReadLockStoreZooKeeperConfig(null, "/hms-proxy-synthetic-read-locks", 15_000, 60_000, 1_000, 3)
        : zooKeeper;
  }

  public static SyntheticReadLockStoreConfig inMemory() {
    return new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null);
  }

  public boolean zooKeeperEnabled() {
    return mode == SyntheticReadLockStoreMode.ZOOKEEPER;
  }
}
