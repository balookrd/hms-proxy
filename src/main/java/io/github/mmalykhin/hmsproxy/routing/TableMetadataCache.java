package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.TableMetadataCacheConfig;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TableMetadataCache {
  private static final Logger LOG = LoggerFactory.getLogger(TableMetadataCache.class);

  private final long ttlMs;
  private final int maxEntries;
  private final boolean sharedAcrossUsers;
  private final boolean serveStaleOnError;
  private final long staleGracePeriodMs;
  private final io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Key, CompletableFuture<Table>> inFlight = new ConcurrentHashMap<>();

  TableMetadataCache(TableMetadataCacheConfig config) {
    this(config, false, 0L, null, System::currentTimeMillis);
  }

  TableMetadataCache(
      TableMetadataCacheConfig config,
      boolean serveStaleOnError,
      long staleGracePeriodMs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics
  ) {
    this(config, serveStaleOnError, staleGracePeriodMs, metrics, System::currentTimeMillis);
  }

  TableMetadataCache(
      TableMetadataCacheConfig config,
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

  Table get(
      String catalogName,
      String backendDbName,
      String tableName,
      ImpersonationContext impersonation,
      Loader loader
  ) throws Throwable {
    if (ttlMs == 0L) {
      return loader.load();
    }
    long nowMs = clock.getAsLong();
    Key key = Key.of(catalogName, backendDbName, tableName, impersonation, sharedAcrossUsers);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      cached.recordAccess(nowMs);
      if (metrics != null) {
        metrics.recordCacheRequest("table_metadata", catalogName, "hit");
      }
      return new Table(cached.table());
    }

    CompletableFuture<Table> future = new CompletableFuture<>();
    CompletableFuture<Table> existing = inFlight.putIfAbsent(key, future);
    if (existing != null) {
      try {
        Table result = existing.get();
        cached = entries.get(key);
        if (cached != null) {
          cached.recordAccess(nowMs);
        }
        if (metrics != null) {
          metrics.recordCacheRequest("table_metadata", catalogName, "hit");
        }
        return result == null ? null : new Table(result);
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(key);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        cached.recordAccess(nowMs);
        if (metrics != null) {
          metrics.recordCacheRequest("table_metadata", catalogName, "hit");
        }
        Table result = new Table(cached.table());
        future.complete(result);
        return result;
      }
      if (metrics != null) {
        metrics.recordCacheRequest("table_metadata", catalogName, "miss");
      }
      Table loaded = loader.load();
      if (loaded != null) {
        put(key, loaded, clock.getAsLong() + ttlMs, nowMs);
      }
      future.complete(loaded);
      return loaded == null ? null : new Table(loaded);
    } catch (Throwable t) {
      if (serveStaleOnError && cached != null && (nowMs - cached.expiresAtMs() <= staleGracePeriodMs)) {
        LOG.warn("Backend catalog '{}' failed for table '{}.{}', serving stale cached metadata (age={} ms)",
            catalogName, backendDbName, tableName, nowMs - cached.expiresAtMs(), t);
        if (metrics != null) {
          metrics.recordCacheRequest("table_metadata", catalogName, "stale_hit");
        }
        Table staleResult = new Table(cached.table());
        future.complete(staleResult);
        return staleResult;
      }
      future.completeExceptionally(t);
      throw t;
    } finally {
      inFlight.remove(key, future);
    }
  }

  void putDirect(
      String catalogName,
      String backendDbName,
      String tableName,
      ImpersonationContext impersonation,
      Table table
  ) {
    if (ttlMs == 0L || table == null) {
      return;
    }
    long nowMs = clock.getAsLong();
    Key key = Key.of(catalogName, backendDbName, tableName, impersonation, sharedAcrossUsers);
    put(key, table, nowMs + ttlMs, nowMs);
  }

  void invalidateTable(String catalogName, String backendDbName, String tableName) {
    if (catalogName == null || backendDbName == null || tableName == null) {
      return;
    }
    boolean removed = entries.keySet().removeIf(k ->
        k.catalogName().equals(catalogName)
            && k.backendDbName().equalsIgnoreCase(backendDbName)
            && k.tableName().equalsIgnoreCase(tableName));
    if (metrics != null) {
      if (removed) {
        metrics.recordCacheInvalidation("table_metadata", catalogName, "write");
      }
      metrics.setCacheEntries("table_metadata", catalogName, countEntries(catalogName));
    }
  }

  void invalidateDatabase(String catalogName, String backendDbName) {
    if (catalogName == null || backendDbName == null) {
      return;
    }
    boolean removed = entries.keySet().removeIf(k ->
        k.catalogName().equals(catalogName) && k.backendDbName().equalsIgnoreCase(backendDbName));
    if (metrics != null) {
      if (removed) {
        metrics.recordCacheInvalidation("table_metadata", catalogName, "database_write");
      }
      metrics.setCacheEntries("table_metadata", catalogName, countEntries(catalogName));
    }
  }

  void invalidateCatalog(String catalogName) {
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
        metrics.recordCacheInvalidation("table_metadata", catalogName, "catalog", removed);
      }
      metrics.setCacheEntries("table_metadata", catalogName, 0);
    }
  }

  void clear() {
    entries.clear();
    inFlight.clear();
  }

  int size() {
    return entries.size();
  }

  private void put(Key key, Table table, long expiresAtMs, long nowMs) {
    if (entries.size() >= maxEntries && !entries.containsKey(key)) {
      evictOldest();
    }
    entries.compute(key, (k, existing) -> {
      if (existing == null) {
        return new Entry(table, expiresAtMs, nowMs);
      }
      existing.update(table, expiresAtMs);
      existing.recordAccess(nowMs);
      return existing;
    });
    if (metrics != null) {
      metrics.setCacheEntries("table_metadata", key.catalogName(), countEntries(key.catalogName()));
    }
  }

  private void evictOldest() {
    Key oldestKey = null;
    long oldestAccess = Long.MAX_VALUE;
    for (var entry : entries.entrySet()) {
      long lastAccess = entry.getValue().lastAccessedAtMs();
      if (lastAccess < oldestAccess) {
        oldestAccess = lastAccess;
        oldestKey = entry.getKey();
      }
    }
    if (oldestKey != null) {
      entries.remove(oldestKey);
      if (metrics != null) {
        metrics.recordCacheInvalidation("table_metadata", oldestKey.catalogName(), "lru");
      }
    }
  }

  private int countEntries(String catalogName) {
    int count = 0;
    for (Key key : entries.keySet()) {
      if (key.catalogName().equals(catalogName)) {
        count++;
      }
    }
    return count;
  }

  @FunctionalInterface
  interface Loader {
    Table load() throws Throwable;
  }

  private record Key(
      String catalogName,
      String backendDbName,
      String tableName,
      String userName,
      List<String> groupNames
  ) {
    private static Key of(
        String catalogName,
        String backendDbName,
        String tableName,
        ImpersonationContext impersonation,
        boolean sharedAcrossUsers
    ) {
      return new Key(
          catalogName,
          backendDbName == null ? "" : backendDbName,
          tableName == null ? "" : tableName,
          sharedAcrossUsers ? "" : (impersonation == null ? "" : Objects.requireNonNullElse(impersonation.userName(), "")),
          sharedAcrossUsers || impersonation == null || impersonation.groupNames() == null
              ? List.of()
              : List.copyOf(impersonation.groupNames()));
    }
  }

  private static final class Entry {
    private volatile Table table;
    private volatile long expiresAtMs;
    private volatile long lastAccessedAtMs;

    Entry(Table table, long expiresAtMs, long lastAccessedAtMs) {
      this.table = new Table(table);
      this.expiresAtMs = expiresAtMs;
      this.lastAccessedAtMs = lastAccessedAtMs;
    }

    Table table() {
      return table;
    }

    long expiresAtMs() {
      return expiresAtMs;
    }

    long lastAccessedAtMs() {
      return lastAccessedAtMs;
    }

    void recordAccess(long accessedAtMs) {
      this.lastAccessedAtMs = accessedAtMs;
    }

    void update(Table newTable, long newExpiresAtMs) {
      this.table = new Table(newTable);
      this.expiresAtMs = newExpiresAtMs;
    }
  }
}
