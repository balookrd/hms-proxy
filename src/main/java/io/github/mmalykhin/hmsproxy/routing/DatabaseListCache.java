package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DatabaseListCache {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseListCache.class);

  private final long ttlMs;
  private final int maxEntries;
  private final boolean sharedAcrossUsers;
  private final boolean serveStaleOnError;
  private final long staleGracePeriodMs;
  private final io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Key, CompletableFuture<List<String>>> inFlight = new ConcurrentHashMap<>();

  DatabaseListCache(DatabaseListCacheConfig config) {
    this(config, null, System::currentTimeMillis);
  }

  DatabaseListCache(DatabaseListCacheConfig config, io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics) {
    this(config, false, 0L, metrics, System::currentTimeMillis);
  }

  DatabaseListCache(
      DatabaseListCacheConfig config,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics,
      LongSupplier clock
  ) {
    this(config, false, 0L, metrics, clock);
  }

  DatabaseListCache(
      DatabaseListCacheConfig config,
      boolean serveStaleOnError,
      long staleGracePeriodMs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics
  ) {
    this(config, serveStaleOnError, staleGracePeriodMs, metrics, System::currentTimeMillis);
  }

  DatabaseListCache(
      DatabaseListCacheConfig config,
      boolean serveStaleOnError,
      long staleGracePeriodMs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics,
      LongSupplier clock
  ) {
    this.ttlMs = config.ttlMs();
    this.maxEntries = config.maxEntries();
    this.sharedAcrossUsers = config.sharedAcrossUsers();
    this.serveStaleOnError = serveStaleOnError;
    this.staleGracePeriodMs = Math.max(0L, staleGracePeriodMs);
    this.metrics = metrics;
    this.clock = clock == null ? System::currentTimeMillis : clock;
  }

  List<String> get(
      String methodName,
      String catalogName,
      String pattern,
      ImpersonationContext impersonation,
      Loader loader
  ) throws Throwable {
    if (ttlMs == 0L) {
      return loader.load();
    }
    long nowMs = clock.getAsLong();
    Key key = Key.of(methodName, catalogName, pattern, impersonation, sharedAcrossUsers);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      cached.recordAccess(nowMs, loader);
      if (metrics != null) {
        metrics.recordCacheRequest("database_list", catalogName, "hit");
      }
      return new ArrayList<>(cached.databases());
    }

    CompletableFuture<List<String>> future = new CompletableFuture<>();
    CompletableFuture<List<String>> existing = inFlight.putIfAbsent(key, future);
    if (existing != null) {
      try {
        List<String> result = existing.get();
        cached = entries.get(key);
        if (cached != null) {
          cached.recordAccess(nowMs, loader);
        }
        if (metrics != null) {
          metrics.recordCacheRequest("database_list", catalogName, "hit");
        }
        return result == null ? null : new ArrayList<>(result);
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(key);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        cached.recordAccess(nowMs, loader);
        if (metrics != null) {
          metrics.recordCacheRequest("database_list", catalogName, "hit");
        }
        List<String> result = new ArrayList<>(cached.databases());
        future.complete(result);
        return result;
      }
      if (metrics != null) {
        metrics.recordCacheRequest("database_list", catalogName, "miss");
      }
      List<String> loaded = loader.load();
      if (loaded != null) {
        put(key, loaded, clock.getAsLong() + ttlMs, nowMs, loader);
      }
      future.complete(loaded);
      return loaded == null ? null : new ArrayList<>(loaded);
    } catch (Throwable t) {
      if (serveStaleOnError && cached != null && (nowMs - cached.expiresAtMs() <= staleGracePeriodMs)) {
        LOG.warn("Backend catalog '{}' failed for method '{}' pattern '{}', serving stale cached database list (age={} ms)",
            catalogName, methodName, pattern, nowMs - cached.expiresAtMs(), t);
        if (metrics != null) {
          metrics.recordCacheRequest("database_list", catalogName, "stale_hit");
        }
        List<String> staleResult = new ArrayList<>(cached.databases());
        future.complete(staleResult);
        return staleResult;
      }
      future.completeExceptionally(t);
      throw t;
    } finally {
      inFlight.remove(key, future);
    }
  }

  void extendCatalogExpiration(String catalogName, String userName, long newExpiresAtMs) {
    if (catalogName == null) {
      return;
    }
    String effectiveUser = sharedAcrossUsers ? "" : (userName == null ? "" : userName);
    for (var entry : entries.entrySet()) {
      if (entry.getKey().catalogName().equals(catalogName)
          && (sharedAcrossUsers || entry.getKey().userName().equals(effectiveUser))) {
        entry.getValue().extendExpiration(newExpiresAtMs);
      }
    }
  }

  void invalidate(String catalogName) {
    if (catalogName == null) {
      return;
    }
    int removed = 0;
    for (var it = entries.keySet().iterator(); it.hasNext(); ) {
      if (it.next().catalogName().equals(catalogName)) {
        it.remove();
        removed++;
      }
    }
    if (metrics != null) {
      if (removed > 0) {
        metrics.recordCacheInvalidation("database_list", catalogName, "write", removed);
      }
      metrics.setCacheEntries("database_list", catalogName, countEntries(catalogName));
    }
  }

  void invalidateAll() {
    int total = entries.size();
    entries.clear();
    if (metrics != null && total > 0) {
      metrics.recordCacheInvalidation("database_list", "all", "all", total);
    }
  }

  private void put(Key key, List<String> databases, long expiresAtMs, long lastAccessedAtMs, Loader loader) {
    pruneIfFull();
    entries.put(key, new Entry(List.copyOf(databases), expiresAtMs, lastAccessedAtMs, loader));
    for (var entry : entries.entrySet()) {
      if (entry.getKey().catalogName().equals(key.catalogName())
          && (sharedAcrossUsers || entry.getKey().userName().equals(key.userName()))) {
        entry.getValue().extendExpiration(expiresAtMs);
      }
    }
    if (metrics != null) {
      metrics.setCacheEntries("database_list", key.catalogName(), countEntries(key.catalogName()));
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
      Loader loader = entry.loader();
      if (loader == null) {
        continue;
      }
      CompletableFuture<List<String>> future = new CompletableFuture<>();
      CompletableFuture<List<String>> existing = inFlight.putIfAbsent(key, future);
      if (existing != null) {
        continue;
      }
      try {
        List<String> loaded = loader.load();
        if (loaded != null) {
          entry.update(loaded, nowMs + ttlMs);
          if (metrics != null) {
            metrics.recordCacheRefresh("database_list", key.catalogName(), "success");
            metrics.setCacheEntries("database_list", key.catalogName(), countEntries(key.catalogName()));
          }
          refreshed++;
        }
        future.complete(loaded);
      } catch (Throwable t) {
        future.completeExceptionally(t);
        LOG.warn("Background refresh failed for database list of catalog {}: {}", key.catalogName(), t.toString());
        if (metrics != null) {
          metrics.recordCacheRefresh("database_list", key.catalogName(), "failure");
        }
      } finally {
        inFlight.remove(key, future);
      }
    }
    return refreshed;
  }

  long lastAccessedAtMs(String catalogName) {
    long latest = 0L;
    for (var entry : entries.entrySet()) {
      if (entry.getKey().catalogName().equals(catalogName)) {
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
    if (entries.size() >= maxEntries) {
      pruned += entries.size();
      entries.clear();
    }
    if (metrics != null && pruned > 0) {
      metrics.recordCacheInvalidation("database_list", "all", "prune", pruned);
    }
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
    List<String> load() throws Throwable;
  }

  private record Key(
      String methodName,
      String catalogName,
      String pattern,
      String userName,
      List<String> groupNames
  ) {
    private static Key of(
        String methodName,
        String catalogName,
        String pattern,
        ImpersonationContext impersonation,
        boolean sharedAcrossUsers
    ) {
      return new Key(
          methodName,
          catalogName,
          pattern == null ? "" : pattern,
          sharedAcrossUsers ? "" : (impersonation == null ? "" : Objects.requireNonNullElse(impersonation.userName(), "")),
          sharedAcrossUsers || impersonation == null || impersonation.groupNames() == null
              ? List.of()
              : List.copyOf(impersonation.groupNames()));
    }
  }

  private static final class Entry {
    private volatile List<String> databases;
    private volatile long expiresAtMs;
    private volatile long lastAccessedAtMs;
    private volatile Loader loader;

    Entry(List<String> databases, long expiresAtMs, long lastAccessedAtMs, Loader loader) {
      this.databases = databases;
      this.expiresAtMs = expiresAtMs;
      this.lastAccessedAtMs = lastAccessedAtMs;
      this.loader = loader;
    }

    List<String> databases() {
      return databases;
    }

    long expiresAtMs() {
      return expiresAtMs;
    }

    long lastAccessedAtMs() {
      return lastAccessedAtMs;
    }

    Loader loader() {
      return loader;
    }

    void recordAccess(long accessedAtMs, Loader currentLoader) {
      this.lastAccessedAtMs = accessedAtMs;
      if (currentLoader != null) {
        this.loader = currentLoader;
      }
    }

    void update(List<String> newDatabases, long newExpiresAtMs) {
      this.databases = List.copyOf(newDatabases);
      this.expiresAtMs = newExpiresAtMs;
    }

    void extendExpiration(long newExpiresAtMs) {
      if (newExpiresAtMs > this.expiresAtMs) {
        this.expiresAtMs = newExpiresAtMs;
      }
    }
  }
}
