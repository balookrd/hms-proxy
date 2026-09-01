package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hive.metastore.api.Database;

final class DatabaseMetadataCache {
  private final long ttlMs;
  private final int maxEntries;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Key, CompletableFuture<Database>> inFlight = new ConcurrentHashMap<>();

  DatabaseMetadataCache(DatabaseMetadataCacheConfig config) {
    this.ttlMs = config.ttlMs();
    this.maxEntries = config.maxEntries();
  }

  Database get(
      String catalogName,
      String backendDbName,
      ImpersonationContext impersonation,
      Loader loader
  ) throws Throwable {
    if (ttlMs == 0L) {
      return loader.load();
    }
    long nowMs = System.currentTimeMillis();
    Key key = Key.of(catalogName, backendDbName, impersonation);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      return new Database(cached.database());
    }

    CompletableFuture<Database> future = new CompletableFuture<>();
    CompletableFuture<Database> existing = inFlight.putIfAbsent(key, future);
    if (existing != null) {
      try {
        Database result = existing.get();
        return result == null ? null : new Database(result);
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(key);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        Database result = new Database(cached.database());
        future.complete(result);
        return result;
      }
      Database loaded = loader.load();
      if (loaded != null) {
        put(key, loaded, System.currentTimeMillis() + ttlMs);
      }
      future.complete(loaded);
      return loaded == null ? null : new Database(loaded);
    } catch (Throwable t) {
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
    entries.keySet().removeIf(k -> k.catalogName().equals(catalogName) && k.backendDbName().equalsIgnoreCase(backendDbName));
  }

  void invalidateCatalog(String catalogName) {
    if (catalogName == null) {
      return;
    }
    entries.keySet().removeIf(k -> k.catalogName().equals(catalogName));
  }

  void invalidateAll() {
    entries.clear();
  }

  private void put(Key key, Database database, long expiresAtMs) {
    pruneIfFull();
    entries.put(key, new Entry(new Database(database), expiresAtMs));
  }

  private void pruneIfFull() {
    if (entries.size() < maxEntries) {
      return;
    }
    long nowMs = System.currentTimeMillis();
    entries.entrySet().removeIf(entry -> entry.getValue().expiresAtMs() <= nowMs);
    if (entries.size() >= maxEntries) {
      entries.clear();
    }
  }

  @FunctionalInterface
  interface Loader {
    Database load() throws Throwable;
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
        ImpersonationContext impersonation
    ) {
      return new Key(
          catalogName,
          backendDbName == null ? "" : backendDbName,
          impersonation == null ? "" : Objects.requireNonNullElse(impersonation.userName(), ""),
          impersonation == null || impersonation.groupNames() == null
              ? List.of()
              : List.copyOf(impersonation.groupNames()));
    }
  }

  private record Entry(Database database, long expiresAtMs) {
  }
}
