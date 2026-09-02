package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

final class DatabaseListCache {
  private final long ttlMs;
  private final int maxEntries;
  private final boolean sharedAcrossUsers;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Key, CompletableFuture<List<String>>> inFlight = new ConcurrentHashMap<>();

  DatabaseListCache(DatabaseListCacheConfig config) {
    this.ttlMs = config.ttlMs();
    this.maxEntries = config.maxEntries();
    this.sharedAcrossUsers = config.sharedAcrossUsers();
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
    long nowMs = System.currentTimeMillis();
    Key key = Key.of(methodName, catalogName, pattern, impersonation, sharedAcrossUsers);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      return new ArrayList<>(cached.databases());
    }

    CompletableFuture<List<String>> future = new CompletableFuture<>();
    CompletableFuture<List<String>> existing = inFlight.putIfAbsent(key, future);
    if (existing != null) {
      try {
        List<String> result = existing.get();
        return result == null ? null : new ArrayList<>(result);
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(key);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        List<String> result = new ArrayList<>(cached.databases());
        future.complete(result);
        return result;
      }
      List<String> loaded = loader.load();
      if (loaded != null) {
        put(key, loaded, System.currentTimeMillis() + ttlMs);
      }
      future.complete(loaded);
      return loaded == null ? null : new ArrayList<>(loaded);
    } catch (Throwable t) {
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
    entries.keySet().removeIf(k -> k.catalogName().equals(catalogName));
  }

  void invalidateAll() {
    entries.clear();
  }

  private void put(Key key, List<String> databases, long expiresAtMs) {
    pruneIfFull();
    entries.put(key, new Entry(List.copyOf(databases), expiresAtMs));
    for (var entry : entries.entrySet()) {
      if (entry.getKey().catalogName().equals(key.catalogName())
          && (sharedAcrossUsers || entry.getKey().userName().equals(key.userName()))) {
        entry.getValue().extendExpiration(expiresAtMs);
      }
    }
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
    private final List<String> databases;
    private volatile long expiresAtMs;

    Entry(List<String> databases, long expiresAtMs) {
      this.databases = databases;
      this.expiresAtMs = expiresAtMs;
    }

    List<String> databases() {
      return databases;
    }

    long expiresAtMs() {
      return expiresAtMs;
    }

    void extendExpiration(long newExpiresAtMs) {
      if (newExpiresAtMs > this.expiresAtMs) {
        this.expiresAtMs = newExpiresAtMs;
      }
    }
  }
}
