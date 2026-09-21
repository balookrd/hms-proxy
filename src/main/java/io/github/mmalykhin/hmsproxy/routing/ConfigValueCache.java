package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.routing.ConfigValueCacheConfig;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.LongSupplier;

/**
 * Thread-safe in-memory cache for metastore configuration parameters fetched via {@code get_config_value}.
 *
 * <p>Caches both present values and missing keys (negative caching) to avoid repeated backend
 * invocations when clients query properties with different default values.
 * Uses SingleFlight deduplication to collapse concurrent queries for the same unpopulated key into
 * a single backend call.
 */
final class ConfigValueCache {
  public static final String SENTINEL = "\0__HMS_PROXY_MISSING_CONFIG__\0";

  @FunctionalInterface
  interface BackendFetcher {
    String fetch(String name, String sentinel) throws Throwable;
  }

  private final long ttlMs;
  private final int maxEntries;
  private final LongSupplier clock;
  private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CompletableFuture<Optional<String>>> inFlight = new ConcurrentHashMap<>();

  ConfigValueCache(ConfigValueCacheConfig config) {
    this(config, System::currentTimeMillis);
  }

  ConfigValueCache(ConfigValueCacheConfig config, LongSupplier clock) {
    this.ttlMs = config.ttlMs();
    this.maxEntries = config.maxEntries();
    this.clock = clock == null ? System::currentTimeMillis : clock;
  }

  String getOrFetch(String name, String defaultValue, BackendFetcher fetcher) throws Throwable {
    if (name == null) {
      return defaultValue;
    }
    if (ttlMs == 0L) {
      return fetcher.fetch(name, defaultValue);
    }

    long nowMs = clock.getAsLong();
    Entry cached = entries.get(name);
    if (cached != null && cached.expiresAtMs() > nowMs) {
      return cached.value().orElse(defaultValue);
    }

    CompletableFuture<Optional<String>> future = new CompletableFuture<>();
    CompletableFuture<Optional<String>> existing = inFlight.putIfAbsent(name, future);
    if (existing != null) {
      try {
        Optional<String> result = existing.get();
        return result.orElse(defaultValue);
      } catch (ExecutionException e) {
        throw e.getCause() != null ? e.getCause() : e;
      }
    }

    try {
      cached = entries.get(name);
      if (cached != null && cached.expiresAtMs() > nowMs) {
        future.complete(cached.value());
        return cached.value().orElse(defaultValue);
      }

      String raw = fetcher.fetch(name, SENTINEL);
      Optional<String> resolved = SENTINEL.equals(raw) ? Optional.empty() : Optional.ofNullable(raw);
      put(name, resolved, nowMs + ttlMs);
      future.complete(resolved);
      return resolved.orElse(defaultValue);
    } catch (Throwable t) {
      future.completeExceptionally(t);
      throw t;
    } finally {
      inFlight.remove(name, future);
    }
  }

  private void put(String name, Optional<String> value, long expiresAtMs) {
    pruneIfFull();
    entries.put(name, new Entry(value, expiresAtMs));
  }

  private void pruneIfFull() {
    if (entries.size() < maxEntries) {
      return;
    }
    long nowMs = clock.getAsLong();
    entries.entrySet().removeIf(e -> e.getValue().expiresAtMs() <= nowMs);
    if (entries.size() >= maxEntries) {
      entries.clear();
    }
  }

  int size() {
    return entries.size();
  }

  void invalidateAll() {
    entries.clear();
  }

  private record Entry(Optional<String> value, long expiresAtMs) {
  }
}
