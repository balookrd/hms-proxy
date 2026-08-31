package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

final class DatabaseListCache {
  private final long ttlMs;
  private final int maxEntries;
  private final ConcurrentHashMap<Key, Entry> entries = new ConcurrentHashMap<>();

  DatabaseListCache(DatabaseListCacheConfig config) {
    this.ttlMs = config.ttlMs();
    this.maxEntries = config.maxEntries();
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
    Key key = Key.of(methodName, catalogName, pattern, impersonation);
    Entry cached = entries.get(key);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      return new ArrayList<>(cached.databases());
    }
    List<String> loaded = loader.load();
    put(key, loaded, nowMs + ttlMs);
    return loaded;
  }

  private void put(Key key, List<String> databases, long expiresAtMs) {
    pruneIfFull();
    entries.put(key, new Entry(List.copyOf(databases), expiresAtMs));
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
        ImpersonationContext impersonation
    ) {
      return new Key(
          methodName,
          catalogName,
          pattern == null ? "" : pattern,
          impersonation == null ? "" : Objects.requireNonNullElse(impersonation.userName(), ""),
          impersonation == null || impersonation.groupNames() == null
              ? List.of()
              : List.copyOf(impersonation.groupNames()));
    }
  }

  private record Entry(List<String> databases, long expiresAtMs) {
  }
}
