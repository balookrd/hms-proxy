package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PartitionMetadataCache {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionMetadataCache.class);

  private final long ttlMs;
  private final int maxEntries;
  private final boolean sharedAcrossUsers;
  private final boolean serveStaleOnError;
  private final long staleGracePeriodMs;
  private final io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics;
  private final LongSupplier clock;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Key, CompletableFuture<Object>> inFlight = new ConcurrentHashMap<>();

  PartitionMetadataCache(PartitionMetadataCacheConfig config) {
    this(config, false, 0L, null, System::currentTimeMillis);
  }

  PartitionMetadataCache(
      PartitionMetadataCacheConfig config,
      boolean serveStaleOnError,
      long staleGracePeriodMs,
      io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics
  ) {
    this(config, serveStaleOnError, staleGracePeriodMs, metrics, System::currentTimeMillis);
  }

  PartitionMetadataCache(
      PartitionMetadataCacheConfig config,
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

  @SuppressWarnings("unchecked")
  <T> T get(
      String catalogName,
      String backendDbName,
      String tableName,
      String querySignature,
      ImpersonationContext impersonation,
      Loader<T> loader
  ) throws Throwable {
    if (ttlMs == 0L) {
      return loader.load();
    }
    long nowMs = clock.getAsLong();
    Key key = Key.of(catalogName, backendDbName, tableName, querySignature, impersonation, sharedAcrossUsers);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      cached.recordAccess(nowMs);
      if (metrics != null) {
        metrics.recordCacheRequest("partition_metadata", catalogName, "hit");
      }
      return (T) deepCopy(cached.value());
    }

    CompletableFuture<Object> future = new CompletableFuture<>();
    CompletableFuture<Object> existing = inFlight.putIfAbsent(key, future);
    if (existing != null) {
      try {
        Object result = existing.get();
        cached = entries.get(key);
        if (cached != null) {
          cached.recordAccess(nowMs);
        }
        if (metrics != null) {
          metrics.recordCacheRequest("partition_metadata", catalogName, "hit");
        }
        return (T) deepCopy(result);
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(key);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        cached.recordAccess(nowMs);
        if (metrics != null) {
          metrics.recordCacheRequest("partition_metadata", catalogName, "hit");
        }
        Object result = deepCopy(cached.value());
        future.complete(result);
        return (T) result;
      }
      if (metrics != null) {
        metrics.recordCacheRequest("partition_metadata", catalogName, "miss");
      }
      T loaded = loader.load();
      if (loaded != null) {
        put(key, loaded, clock.getAsLong() + ttlMs, nowMs);
      }
      future.complete(loaded);
      return (T) deepCopy(loaded);
    } catch (Throwable t) {
      if (serveStaleOnError && cached != null && (nowMs - cached.expiresAtMs() <= staleGracePeriodMs)) {
        LOG.warn("Backend catalog '{}' failed for partitions of '{}.{}', serving stale cached metadata (age={} ms)",
            catalogName, backendDbName, tableName, nowMs - cached.expiresAtMs(), t);
        if (metrics != null) {
          metrics.recordCacheRequest("partition_metadata", catalogName, "stale_hit");
        }
        Object staleResult = deepCopy(cached.value());
        future.complete(staleResult);
        return (T) staleResult;
      }
      future.completeExceptionally(t);
      throw t;
    } finally {
      inFlight.remove(key, future);
    }
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
        metrics.recordCacheInvalidation("partition_metadata", catalogName, "write");
      }
      metrics.setCacheEntries("partition_metadata", catalogName, countEntries(catalogName));
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
        metrics.recordCacheInvalidation("partition_metadata", catalogName, "database_write");
      }
      metrics.setCacheEntries("partition_metadata", catalogName, countEntries(catalogName));
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
        metrics.recordCacheInvalidation("partition_metadata", catalogName, "catalog", removed);
      }
      metrics.setCacheEntries("partition_metadata", catalogName, 0);
    }
  }

  void clear() {
    entries.clear();
    inFlight.clear();
  }

  int size() {
    return entries.size();
  }

  private void put(Key key, Object value, long expiresAtMs, long nowMs) {
    if (entries.size() >= maxEntries && !entries.containsKey(key)) {
      evictOldest();
    }
    entries.compute(key, (k, existing) -> {
      if (existing == null) {
        return new Entry(value, expiresAtMs, nowMs);
      }
      existing.update(value, expiresAtMs);
      existing.recordAccess(nowMs);
      return existing;
    });
    if (metrics != null) {
      metrics.setCacheEntries("partition_metadata", key.catalogName(), countEntries(key.catalogName()));
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
        metrics.recordCacheInvalidation("partition_metadata", oldestKey.catalogName(), "lru");
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

  @SuppressWarnings("unchecked")
  private static Object deepCopy(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Partition p) {
      return new Partition(p);
    }
    if (value instanceof List<?> list) {
      if (list.isEmpty()) {
        return new ArrayList<>();
      }
      Object first = list.get(0);
      if (first instanceof Partition) {
        List<Partition> copy = new ArrayList<>(list.size());
        for (Object item : list) {
          copy.add(new Partition((Partition) item));
        }
        return copy;
      }
      if (first instanceof PartitionSpec) {
        List<PartitionSpec> copy = new ArrayList<>(list.size());
        for (Object item : list) {
          copy.add(new PartitionSpec((PartitionSpec) item));
        }
        return copy;
      }
      if (first instanceof String) {
        return new ArrayList<>((List<String>) list);
      }
      return new ArrayList<>(list);
    }
    return value;
  }

  @FunctionalInterface
  interface Loader<T> {
    T load() throws Throwable;
  }

  private record Key(
      String catalogName,
      String backendDbName,
      String tableName,
      String querySignature,
      String userName,
      List<String> groupNames
  ) {
    private static Key of(
        String catalogName,
        String backendDbName,
        String tableName,
        String querySignature,
        ImpersonationContext impersonation,
        boolean sharedAcrossUsers
    ) {
      return new Key(
          catalogName,
          backendDbName == null ? "" : backendDbName,
          tableName == null ? "" : tableName,
          querySignature == null ? "" : querySignature,
          sharedAcrossUsers ? "" : (impersonation == null ? "" : Objects.requireNonNullElse(impersonation.userName(), "")),
          sharedAcrossUsers || impersonation == null || impersonation.groupNames() == null
              ? List.of()
              : List.copyOf(impersonation.groupNames()));
    }
  }

  private static final class Entry {
    private volatile Object value;
    private volatile long expiresAtMs;
    private volatile long lastAccessedAtMs;

    Entry(Object value, long expiresAtMs, long lastAccessedAtMs) {
      this.value = deepCopy(value);
      this.expiresAtMs = expiresAtMs;
      this.lastAccessedAtMs = lastAccessedAtMs;
    }

    Object value() {
      return value;
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

    void update(Object newValue, long newExpiresAtMs) {
      this.value = deepCopy(newValue);
      this.expiresAtMs = newExpiresAtMs;
    }
  }
}
