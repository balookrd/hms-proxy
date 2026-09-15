package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Configuration for proactive background refresh of database caches while clients are active.
 *
 * @param enabled whether background refresh is enabled
 * @param intervalMs interval between background refreshes (default 60,000 ms = 1 minute)
 * @param activityWindowMs window of client activity after which background refresh pauses (default 3,600,000 ms = 1 hour)
 */
public record DatabaseCacheBackgroundRefreshConfig(
    boolean enabled,
    long intervalMs,
    long activityWindowMs
) {
  public DatabaseCacheBackgroundRefreshConfig {
    intervalMs = intervalMs <= 0L ? 60_000L : intervalMs;
    activityWindowMs = activityWindowMs <= 0L ? 3_600_000L : activityWindowMs;
  }

  public static DatabaseCacheBackgroundRefreshConfig disabled() {
    return new DatabaseCacheBackgroundRefreshConfig(false, 60_000L, 3_600_000L);
  }
}
