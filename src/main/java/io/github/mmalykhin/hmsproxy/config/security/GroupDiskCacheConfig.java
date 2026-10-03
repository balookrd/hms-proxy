package io.github.mmalykhin.hmsproxy.config.security;

import java.util.Objects;

/**
 * Configuration for persistent on-disk user group caching.
 *
 * <p>Enables fast cold-start group resolution and resilience against transient Active Directory / LDAP outages
 * by persisting resolved user group memberships to disk across service restarts.
 */
public record GroupDiskCacheConfig(
    boolean enabled,
    String path,
    long entryTtlSeconds,
    long persistIntervalSeconds,
    boolean persistOnShutdown
) {
  public static final String DEFAULT_PATH = "/var/cache/hms-proxy/group-cache.json";
  public static final long DEFAULT_ENTRY_TTL_SECONDS = 86_400L; // 24 hours
  public static final long DEFAULT_PERSIST_INTERVAL_SECONDS = 60L; // 1 minute
  public static final boolean DEFAULT_PERSIST_ON_SHUTDOWN = true;

  public GroupDiskCacheConfig {
    if (enabled) {
      Objects.requireNonNull(path, "security.group-disk-cache.path must not be null when enabled");
      if (path.isBlank()) {
        throw new IllegalArgumentException("security.group-disk-cache.path must not be blank when enabled");
      }
    }
    if (entryTtlSeconds < 0L) {
      throw new IllegalArgumentException("security.group-disk-cache.entry-ttl-seconds must be >= 0, got: " + entryTtlSeconds);
    }
    if (persistIntervalSeconds < 0L) {
      throw new IllegalArgumentException("security.group-disk-cache.persist-interval-seconds must be >= 0, got: " + persistIntervalSeconds);
    }
  }

  public static GroupDiskCacheConfig disabled() {
    return new GroupDiskCacheConfig(false, DEFAULT_PATH, DEFAULT_ENTRY_TTL_SECONDS, DEFAULT_PERSIST_INTERVAL_SECONDS, DEFAULT_PERSIST_ON_SHUTDOWN);
  }
}
