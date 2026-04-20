package io.github.mmalykhin.hmsproxy.config;

import java.util.Locale;

final class SyntheticReadLockStoreConfigParser {
  private SyntheticReadLockStoreConfigParser() {
  }

  static ProxyConfig.SyntheticReadLockStoreConfig parse(PropertyReader reader) {
    boolean zkConfigured = reader.hasPrefix("synthetic-read-lock.store.zookeeper.");
    String rawMode = reader.getOrNull("synthetic-read-lock.store.mode");
    if (rawMode == null && !zkConfigured) {
      throw new IllegalArgumentException(
          "synthetic-read-lock.store.mode must be set explicitly. "
              + "Use IN_MEMORY for single-instance deployments (non-default catalog SELECT locks "
              + "will be lost on proxy restart or load-balancer failover), or ZOOKEEPER with "
              + "synthetic-read-lock.store.zookeeper.connect-string for HA / multi-instance setups.");
    }
    ProxyConfig.SyntheticReadLockStoreMode mode = parseMode(rawMode, zkConfigured);
    String znode = reader.getOrNull("synthetic-read-lock.store.zookeeper.znode");
    if (reader.has("synthetic-read-lock.store.zookeeper.znode") && znode == null) {
      throw new IllegalArgumentException("synthetic-read-lock.store.zookeeper.znode must not be blank");
    }
    int connectionTimeoutMs = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.connection-timeout-ms", 15_000);
    int sessionTimeoutMs = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.session-timeout-ms", 60_000);
    int baseSleepMs = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.base-sleep-ms", 1_000);
    int maxRetries = reader.getPositiveInt("synthetic-read-lock.store.zookeeper.max-retries", 3);
    ProxyConfig.SyntheticReadLockStoreZooKeeperConfig zk =
        new ProxyConfig.SyntheticReadLockStoreZooKeeperConfig(
            reader.getOrNull("synthetic-read-lock.store.zookeeper.connect-string"),
            znode,
            connectionTimeoutMs,
            sessionTimeoutMs,
            baseSleepMs,
            maxRetries);
    if (mode == ProxyConfig.SyntheticReadLockStoreMode.ZOOKEEPER) {
      ConfigParsing.requireNonBlank(zk.connectString(), "synthetic-read-lock.store.zookeeper.connect-string");
    }
    return new ProxyConfig.SyntheticReadLockStoreConfig(mode, zk);
  }

  private static ProxyConfig.SyntheticReadLockStoreMode parseMode(String value, boolean zooKeeperConfigured) {
    if (value == null) {
      return zooKeeperConfigured
          ? ProxyConfig.SyntheticReadLockStoreMode.ZOOKEEPER
          : ProxyConfig.SyntheticReadLockStoreMode.IN_MEMORY;
    }
    try {
      return ProxyConfig.SyntheticReadLockStoreMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for synthetic-read-lock.store.mode: " + value
              + ". Expected one of: IN_MEMORY, ZOOKEEPER",
          e);
    }
  }
}
