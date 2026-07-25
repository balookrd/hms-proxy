package io.github.mmalykhin.hmsproxy.config.syntheticlock;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
public final class SyntheticReadLockStoreConfigParser {
  private static final String ZOOKEEPER_PREFIX = "synthetic-read-lock.store.zookeeper.";

  private SyntheticReadLockStoreConfigParser() {
  }

  public static SyntheticReadLockStoreConfig parse(PropertyReader reader) {
    boolean zkConfigured = reader.hasPrefix(ZOOKEEPER_PREFIX);
    String rawMode = reader.getOrNull("synthetic-read-lock.store.mode");
    if (rawMode == null && !zkConfigured) {
      throw new IllegalArgumentException(
          "synthetic-read-lock.store.mode must be set explicitly. "
              + "Use IN_MEMORY for single-instance deployments (non-default catalog SELECT locks "
              + "will be lost on proxy restart or load-balancer failover), or ZOOKEEPER with "
              + "synthetic-read-lock.store.zookeeper.connect-string for HA / multi-instance setups.");
    }
    SyntheticReadLockStoreMode mode = parseMode(rawMode, zkConfigured);
    if (mode == SyntheticReadLockStoreMode.IN_MEMORY && zkConfigured) {
      throw new IllegalArgumentException(
          "synthetic-read-lock.store.mode=IN_MEMORY ignores every configured " + ZOOKEEPER_PREFIX
              + "* property, so locks would live in memory and be lost on restart or failover. "
              + "Remove " + String.join(", ", reader.namesWithPrefix(ZOOKEEPER_PREFIX))
              + ", or set synthetic-read-lock.store.mode=ZOOKEEPER.");
    }
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
    SyntheticReadLockStoreMode inferredDefault = zooKeeperConfigured
        ? SyntheticReadLockStoreMode.ZOOKEEPER
        : SyntheticReadLockStoreMode.IN_MEMORY;
    return ConfigParsing.parseEnum(
        SyntheticReadLockStoreMode.class, value, "synthetic-read-lock.store.mode", inferredDefault);
  }
}
