package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.routing.DatabaseCacheBackgroundRefreshConfig;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background refresher coordinating periodic warming of active {@link DatabaseListCache}
 * and {@link DatabaseMetadataCache} entries while client requests are active.
 */
public final class DatabaseCacheRefresher implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseCacheRefresher.class);

  private final DatabaseListCache databaseListCache;
  private final DatabaseMetadataCache databaseMetadataCache;
  private final DatabaseCacheBackgroundRefreshConfig listRefreshConfig;
  private final DatabaseCacheBackgroundRefreshConfig metaRefreshConfig;
  private final LongSupplier clock;
  private final ScheduledExecutorService executor;

  public DatabaseCacheRefresher(
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      DatabaseCacheBackgroundRefreshConfig listRefreshConfig,
      DatabaseCacheBackgroundRefreshConfig metaRefreshConfig
  ) {
    this(databaseListCache, databaseMetadataCache, listRefreshConfig, metaRefreshConfig, System::currentTimeMillis, true);
  }

  public DatabaseCacheRefresher(
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      DatabaseCacheBackgroundRefreshConfig listRefreshConfig,
      DatabaseCacheBackgroundRefreshConfig metaRefreshConfig,
      LongSupplier clock,
      boolean startScheduledTask
  ) {
    this.databaseListCache = databaseListCache;
    this.databaseMetadataCache = databaseMetadataCache;
    this.listRefreshConfig = listRefreshConfig == null ? DatabaseCacheBackgroundRefreshConfig.disabled() : listRefreshConfig;
    this.metaRefreshConfig = metaRefreshConfig == null ? DatabaseCacheBackgroundRefreshConfig.disabled() : metaRefreshConfig;
    this.clock = clock == null ? System::currentTimeMillis : clock;

    boolean anyEnabled = this.listRefreshConfig.enabled() || this.metaRefreshConfig.enabled();
    if (anyEnabled && startScheduledTask) {
      long intervalMs = Long.MAX_VALUE;
      if (this.listRefreshConfig.enabled()) {
        intervalMs = Math.min(intervalMs, this.listRefreshConfig.intervalMs());
      }
      if (this.metaRefreshConfig.enabled()) {
        intervalMs = Math.min(intervalMs, this.metaRefreshConfig.intervalMs());
      }
      this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "hms-proxy-db-cache-refresh");
        t.setDaemon(true);
        return t;
      });
      this.executor.scheduleWithFixedDelay(this::refreshTick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    } else {
      this.executor = null;
    }
  }

  void refreshTick() {
    long nowMs = clock.getAsLong();
    try {
      if (listRefreshConfig.enabled() && databaseListCache != null) {
        databaseListCache.refreshActiveEntries(nowMs, listRefreshConfig.activityWindowMs());
      }
      if (metaRefreshConfig.enabled() && databaseMetadataCache != null) {
        databaseMetadataCache.refreshActiveEntries(nowMs, metaRefreshConfig.activityWindowMs());
      }
    } catch (Throwable t) {
      LOG.warn("Unexpected error during database cache refresh tick: {}", t.toString(), t);
    }
  }

  @Override
  public void close() {
    if (executor != null) {
      executor.shutdownNow();
      try {
        executor.awaitTermination(3, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
