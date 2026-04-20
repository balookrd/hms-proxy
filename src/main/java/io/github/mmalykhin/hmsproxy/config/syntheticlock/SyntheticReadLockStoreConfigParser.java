package io.github.mmalykhin.hmsproxy.config.syntheticlock;

import java.util.Locale;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
public final class SyntheticReadLockStoreConfigParser {
  private SyntheticReadLockStoreConfigParser() {
  }

  public static SyntheticReadLockStoreConfig parse(PropertyReader reader) {
    boolean zkConfigured = reader.hasPrefix("synthetic-read-lock.store.zookeeper.");
    String rawMode = reader.getOrNull("synthetic-read-lock.store.mode");
    if (rawMode == null && !zkConfigured) {
      throw new IllegalArgumentException(
          "synthetic-read-lock.store.mode must be set explicitly. "
              + "Use IN_MEMORY for single-instance deployments (non-default catalog SELECT locks "
              + "will be lost on proxy restart or load-balancer failover), or ZOOKEEPER with "
              + "synthetic-read-lock.store.zookeeper.connect-string for HA / multi-instance setups.");
    }
    SyntheticReadLockStoreMode mode = parseMode(rawMode, zkConfigured);
    String znode = reader.getOrNull("synthetic-read-lock.store.zookeeper.znode");
    if (reader.has("synthetic-read-lock.store.zookeeper.znode") && znode == null) {
      throw new IllegalArgumentException("synthetic-read-lock.store.zookeeper.znode must not be blank");
    }
    int connectionTimeoutMs = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.connection-timeout-ms", 15_000);
    int sessionTimeoutMs = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.session-timeout-ms", 60_000);
    int baseSleepMs = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.base-sleep-ms", 1_000);
    int maxRetries = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.max-retries", 3);
    SyntheticReadLockStoreZooKeeperConfig zk =
        new SyntheticReadLockStoreZooKeeperConfig(
            reader.getOrNull("synthetic-read-lock.store.zookeeper.connect-string"),
            znode,
            connectionTimeoutMs,
            sessionTimeoutMs,
            baseSleepMs,
            maxRetries);
    if (mode == SyntheticReadLockStoreMode.ZOOKEEPER) {
      ConfigParsing.requireNonBlank(zk.connectString(), "synthetic-read-lock.store.zookeeper.connect-string");
    }
    return new SyntheticReadLockStoreConfig(mode, zk);
  }

  private static SyntheticReadLockStoreMode parseMode(String value, boolean zooKeeperConfigured) {
    if (value == null) {
      return zooKeeperConfigured
          ? SyntheticReadLockStoreMode.ZOOKEEPER
          : SyntheticReadLockStoreMode.IN_MEMORY;
    }
    try {
      return SyntheticReadLockStoreMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for synthetic-read-lock.store.mode: " + value
              + ". Expected one of: IN_MEMORY, ZOOKEEPER",
          e);
    }
  }
}
