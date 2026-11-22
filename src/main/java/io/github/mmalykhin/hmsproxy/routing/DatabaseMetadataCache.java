package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.hadoop.hive.metastore.api.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DatabaseMetadataCache {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseMetadataCache.class);

  private final long ttlMs;
  private final int maxEntries;
  private final boolean sharedAcrossUsers;
  private final DatabaseListCache databaseListCache;
  private final boolean serveStaleOnError;
  private final long staleGracePeriodMs;
  private final io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Key, CompletableFuture<Database>> inFlight = new ConcurrentHashMap<>();
  private final AtomicLong generation = new AtomicLong();

  DatabaseMetadataCache(DatabaseMetadataCacheConfig config) {
    this(config, null, null, System::currentTimeMillis);
  }

  DatabaseMetadataCache(DatabaseMetadataCacheConfig config, DatabaseListCache databaseListCache) {
    this(config, databaseListCache, null, System::currentTimeMillis);
  }

  DatabaseMetadataCache(
      DatabaseMetadataCacheConfig config,
      DatabaseListCache databaseListCache,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics
  ) {
    this(config, databaseListCache, metrics, System::currentTimeMillis);
  }

  DatabaseMetadataCache(
      DatabaseMetadataCacheConfig config,
      DatabaseListCache databaseListCache,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics,
      LongSupplier clock
  ) {
    this(config, databaseListCache, false, 0L, metrics, clock);
  }

  DatabaseMetadataCache(
      DatabaseMetadataCacheConfig config,
      DatabaseListCache databaseListCache,
      boolean serveStaleOnError,
      long staleGracePeriodMs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics
  ) {
    this(config, databaseListCache, serveStaleOnError, staleGracePeriodMs, metrics, System::currentTimeMillis);
  }

  DatabaseMetadataCache(
      DatabaseMetadataCacheConfig config,
      DatabaseListCache databaseListCache,
      boolean serveStaleOnError,
      long staleGracePeriodMs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics,
      LongSupplier clock
  ) {
    this.ttlMs = config.ttlMs();
    this.maxEntries = config.maxEntries();
    this.sharedAcrossUsers = config.sharedAcrossUsers();
    this.databaseListCache = databaseListCache;
    this.serveStaleOnError = serveStaleOnError;
    this.staleGracePeriodMs = Math.max(0L, staleGracePeriodMs);
    this.metrics = metrics;
    this.clock = clock == null ? System::currentTimeMillis : clock;
  }

  Database get(
      String catalogName,
      String backendDbName,
      ImpersonationContext impersonation,
      Loader loader
  ) throws Throwable {
    return get(catalogName, backendDbName, impersonation, (ignoredImp, ignoredBg) -> loader.load());
  }

  Database get(
      String catalogName,
      String backendDbName,
      ImpersonationContext impersonation,
      RefreshableLoader loader
  ) throws Throwable {
    ImpersonationContext refreshImpersonation = sharedAcrossUsers ? null : impersonation;
    if (ttlMs == 0L) {
      return loader.load(impersonation, false);
    }
    long nowMs = clock.getAsLong();
    Key key = Key.of(catalogName, backendDbName, impersonation, sharedAcrossUsers);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      cached.recordAccess(nowMs, loader, refreshImpersonation);
      if (metrics != null) {
        metrics.recordCacheRequest("database_metadata", catalogName, "hit");
      }
      return new Database(cached.database());
    }

    CompletableFuture<Database> future = new CompletableFuture<>();
    CompletableFuture<Database> existing = inFlight.putIfAbsent(key, future);
    if (existing != null) {
      try {
        Database result = existing.get();
        cached = entries.get(key);
        if (cached != null) {
          cached.recordAccess(nowMs, loader, refreshImpersonation);
        }
        if (metrics != null) {
          metrics.recordCacheRequest("database_metadata", catalogName, "hit");
        }
        return result == null ? null : new Database(result);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(key);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        cached.recordAccess(nowMs, loader, refreshImpersonation);
        if (metrics != null) {
          metrics.recordCacheRequest("database_metadata", catalogName, "hit");
        }
        future.complete(new Database(cached.database()));
        return new Database(cached.database());
      }
      if (metrics != null) {
        metrics.recordCacheRequest("database_metadata", catalogName, "miss");
      }
      long loadEpoch = generation.get();
      Database loaded = loader.load(refreshImpersonation, false);
      if (loaded != null) {
        put(key, loaded, clock.getAsLong() + ttlMs, nowMs, loader, refreshImpersonation, loadEpoch);
      }
      future.complete(loaded == null ? null : new Database(loaded));
      return loaded == null ? null : new Database(loaded);
    } catch (Throwable t) {
      if (serveStaleOnError && cached != null && (nowMs - cached.expiresAtMs() <= staleGracePeriodMs)) {
        LOG.warn("Backend catalog '{}' failed for get_database '{}', serving stale cached metadata (age={} ms)",
            catalogName, backendDbName, nowMs - cached.expiresAtMs(), t);
        if (metrics != null) {
          metrics.recordCacheRequest("database_metadata", catalogName, "stale_hit");
        }
        future.complete(new Database(cached.database()));
        return new Database(cached.database());
      }
      future.completeExceptionally(t);
      throw t;
    } finally {
      inFlight.remove(key, future);
    }
  }

  void invalidate(String catalogName, String backendDbName) {
    if (catalogName == null || backendDbName == null) {
      return;
    }
    generation.incrementAndGet();
    boolean removed = entries.keySet().removeIf(k -> k.catalogName().equals(catalogName) && k.backendDbName().equalsIgnoreCase(backendDbName));
    if (metrics != null) {
      if (removed) {
        metrics.recordCacheInvalidation("database_metadata", catalogName, "write");
      }
      metrics.setCacheEntries("database_metadata", catalogName, countEntries(catalogName));
    }
  }

  void invalidateCatalog(String catalogName) {
    if (catalogName == null) {
      return;
    }
    generation.incrementAndGet();
    int removed = 0;
    for (var it = entries.keySet().iterator(); it.hasNext(); ) {
      if (it.next().catalogName().equals(catalogName)) {
        it.remove();
        removed++;
      }
    }
    if (metrics != null) {
      if (removed > 0) {
        metrics.recordCacheInvalidation("database_metadata", catalogName, "catalog", removed);
      }
      metrics.setCacheEntries("database_metadata", catalogName, countEntries(catalogName));
    }
  }

  void invalidateAll() {
    generation.incrementAndGet();
    int total = entries.size();
    entries.clear();
    if (metrics != null && total > 0) {
      metrics.recordCacheInvalidation("database_metadata", "all", "all", total);
    }
  }

  int size() {
    return entries.size();
  }

  private void put(
      Key key,
      Database database,
      long expiresAtMs,
      long lastAccessedAtMs,
      RefreshableLoader loader,
      ImpersonationContext refreshImpersonation,
      long loadEpoch
  ) {
    if (generation.get() != loadEpoch) {
      return;
    }
    pruneIfFull();
    Entry inserted = new Entry(new Database(database), expiresAtMs, lastAccessedAtMs, loader, refreshImpersonation);
    entries.put(key, inserted);
    if (generation.get() != loadEpoch) {
      entries.remove(key, inserted);
      return;
    }
    for (var entry : entries.entrySet()) {
      if (entry.getKey().catalogName().equals(key.catalogName())
          && (sharedAcrossUsers || entry.getKey().userName().equals(key.userName()))) {
        entry.getValue().extendExpiration(expiresAtMs);
      }
    }
    if (databaseListCache != null) {
      databaseListCache.extendCatalogExpiration(key.catalogName(), key.userName(), expiresAtMs);
    }
    if (metrics != null) {
      metrics.setCacheEntries("database_metadata", key.catalogName(), countEntries(key.catalogName()));
    }
  }

  int refreshActiveEntries(long nowMs, long activityWindowMs) {
    if (ttlMs == 0L) {
      return 0;
    }
    int refreshed = 0;
    for (var mapEntry : entries.entrySet()) {
      Key key = mapEntry.getKey();
      Entry entry = mapEntry.getValue();
      if (nowMs - entry.lastAccessedAtMs() > activityWindowMs) {
        continue;
      }
      RefreshableLoader loader = entry.loader();
      if (loader == null) {
        continue;
      }
      CompletableFuture<Database> future = new CompletableFuture<>();
      CompletableFuture<Database> existing = inFlight.putIfAbsent(key, future);
      if (existing != null) {
        continue;
      }
      try {
        long loadEpoch = generation.get();
        Database loaded = loader.load(entry.refreshImpersonation(), true);
        if (loaded != null && generation.get() == loadEpoch && entries.get(key) == entry) {
          long refreshedExpiresAtMs = clock.getAsLong() + ttlMs;
          entry.update(loaded, refreshedExpiresAtMs);
          if (generation.get() != loadEpoch) {
            entries.remove(key, entry);
          } else {
            if (databaseListCache != null) {
              databaseListCache.extendCatalogExpiration(key.catalogName(), key.userName(), refreshedExpiresAtMs);
            }
            if (metrics != null) {
              metrics.recordCacheRefresh("database_metadata", key.catalogName(), "success");
              metrics.setCacheEntries("database_metadata", key.catalogName(), countEntries(key.catalogName()));
            }
            refreshed++;
          }
        }
        future.complete(loaded == null ? null : new Database(loaded));
      } catch (Throwable t) {
        future.completeExceptionally(t);
        LOG.warn("Background refresh failed for database metadata {}/{}: {}",
            key.catalogName(), key.backendDbName(), t.toString());
        if (metrics != null) {
          metrics.recordCacheRefresh("database_metadata", key.catalogName(), "failure");
        }
      } finally {
        inFlight.remove(key, future);
      }
    }
    return refreshed;
  }

  long lastAccessedAtMs(String catalogName, String backendDbName) {
    long latest = 0L;
    for (var entry : entries.entrySet()) {
      if (entry.getKey().catalogName().equals(catalogName)
          && entry.getKey().backendDbName().equalsIgnoreCase(backendDbName)) {
        latest = Math.max(latest, entry.getValue().lastAccessedAtMs());
      }
    }
    return latest;
  }

  private void pruneIfFull() {
    if (entries.size() < maxEntries) {
      return;
    }
    long nowMs = clock.getAsLong();
    int before = entries.size();
    entries.entrySet().removeIf(entry -> entry.getValue().expiresAtMs() <= nowMs);
    int pruned = before - entries.size();
    if (metrics != null && pruned > 0) {
      metrics.recordCacheInvalidation("database_metadata", "all", "prune", pruned);
    }
    // Still full of live entries: drop the least recently used ones instead of the whole cache, so
    // one burst of new keys does not send every hot key back to the backend at once.
    while (entries.size() >= maxEntries && evictLeastRecentlyUsed()) {
      if (metrics != null) {
        metrics.recordCacheInvalidation("database_metadata", "all", "lru");
      }
    }
  }

  private boolean evictLeastRecentlyUsed() {
    Key oldestKey = null;
    long oldestAccess = Long.MAX_VALUE;
    for (var entry : entries.entrySet()) {
      long lastAccess = entry.getValue().lastAccessedAtMs();
      if (lastAccess < oldestAccess) {
        oldestAccess = lastAccess;
        oldestKey = entry.getKey();
      }
    }
    return oldestKey != null && entries.remove(oldestKey) != null;
  }

  private long countEntries(String catalogName) {
    long count = 0;
    for (Key k : entries.keySet()) {
      if (k.catalogName().equals(catalogName)) {
        count++;
      }
    }
    return count;
  }

  @FunctionalInterface
  interface Loader {
    Database load() throws Throwable;
  }

  @FunctionalInterface
  interface RefreshableLoader {
    Database load(ImpersonationContext impersonation, boolean background) throws Throwable;
  }

  private record Key(
      String catalogName,
      String backendDbName,
      String userName,
      List<String> groupNames
  ) {
    private static Key of(
        String catalogName,
        String backendDbName,
        ImpersonationContext impersonation,
        boolean sharedAcrossUsers
    ) {
      return new Key(
          catalogName,
          backendDbName == null ? "" : backendDbName,
          sharedAcrossUsers ? "" : (impersonation == null ? "" : Objects.requireNonNullElse(impersonation.userName(), "")),
          sharedAcrossUsers || impersonation == null || impersonation.groupNames() == null
              ? List.of()
              : List.copyOf(impersonation.groupNames()));
    }
  }

  private static final class Entry {
    private volatile Database database;
    private volatile long expiresAtMs;
    private volatile long lastAccessedAtMs;
    private volatile RefreshableLoader loader;
    private volatile ImpersonationContext refreshImpersonation;

    Entry(
        Database database,
        long expiresAtMs,
        long lastAccessedAtMs,
        RefreshableLoader loader,
        ImpersonationContext refreshImpersonation
    ) {
      this.database = database;
      this.expiresAtMs = expiresAtMs;
      this.lastAccessedAtMs = lastAccessedAtMs;
      this.loader = loader;
      this.refreshImpersonation = refreshImpersonation;
    }

    Database database() {
      return database;
    }

    long expiresAtMs() {
      return expiresAtMs;
    }

    long lastAccessedAtMs() {
      return lastAccessedAtMs;
    }

    RefreshableLoader loader() {
      return loader;
    }

    ImpersonationContext refreshImpersonation() {
      return refreshImpersonation;
    }

    void recordAccess(long accessedAtMs, RefreshableLoader currentLoader, ImpersonationContext currentImpersonation) {
      this.lastAccessedAtMs = accessedAtMs;
      if (currentLoader != null) {
        this.loader = currentLoader;
      }
      this.refreshImpersonation = currentImpersonation;
    }

    void update(Database newDatabase, long newExpiresAtMs) {
      this.database = new Database(newDatabase);
      this.expiresAtMs = newExpiresAtMs;
    }

    void extendExpiration(long newExpiresAtMs) {
      if (newExpiresAtMs > this.expiresAtMs) {
        this.expiresAtMs = newExpiresAtMs;
      }
    }
  }
}
