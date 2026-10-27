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
    boolean persistOnShutdown,
    int maxEntries
) {
  public static final String DEFAULT_PATH = "/var/cache/hms-proxy/group-cache.json";
  public static final long DEFAULT_ENTRY_TTL_SECONDS = 86_400L; // 24 hours
  public static final long DEFAULT_PERSIST_INTERVAL_SECONDS = 60L; // 1 minute
  public static final boolean DEFAULT_PERSIST_ON_SHUTDOWN = true;
  // Entries past their TTL are kept on purpose (they back the serve-stale fallback during a
  // directory outage), so the user count is what bounds the cache.
  public static final int DEFAULT_MAX_ENTRIES = 50_000;

  public GroupDiskCacheConfig(
      boolean enabled,
      String path,
      long entryTtlSeconds,
      long persistIntervalSeconds,
      boolean persistOnShutdown
  ) {
    this(enabled, path, entryTtlSeconds, persistIntervalSeconds, persistOnShutdown, DEFAULT_MAX_ENTRIES);
  }

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
    if (maxEntries < 1) {
      throw new IllegalArgumentException("security.group-disk-cache.max-entries must be >= 1, got: " + maxEntries);
    }
  }

  public static GroupDiskCacheConfig disabled() {
    return new GroupDiskCacheConfig(false, DEFAULT_PATH, DEFAULT_ENTRY_TTL_SECONDS, DEFAULT_PERSIST_INTERVAL_SECONDS, DEFAULT_PERSIST_ON_SHUTDOWN,
        DEFAULT_MAX_ENTRIES);
  }
}
