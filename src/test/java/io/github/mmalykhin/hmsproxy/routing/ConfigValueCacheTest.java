package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.routing.ConfigValueCacheConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class ConfigValueCacheTest {

  @Test
  public void returnsDefaultWhenKeyIsNull() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(ConfigValueCacheConfig.defaultConfig());
    AtomicInteger callCount = new AtomicInteger();
    String result = cache.getOrFetch(null, "fallback", (name, sentinel) -> {
      callCount.incrementAndGet();
      return "backendValue";
    });
    Assert.assertEquals("fallback", result);
    Assert.assertEquals(0, callCount.get());
  }

  @Test
  public void cachesPresentBackendValue() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(ConfigValueCacheConfig.defaultConfig());
    AtomicInteger callCount = new AtomicInteger();

    String first = cache.getOrFetch("hive.metastore.try.direct.sql", "false", (name, sentinel) -> {
      callCount.incrementAndGet();
      Assert.assertEquals(ConfigValueCache.SENTINEL, sentinel);
      return "true";
    });
    Assert.assertEquals("true", first);
    Assert.assertEquals(1, callCount.get());

    String second = cache.getOrFetch("hive.metastore.try.direct.sql", "false", (name, sentinel) -> {
      callCount.incrementAndGet();
      return "false";
    });
    Assert.assertEquals("true", second);
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void cachesMissingBackendKeyAndPreservesDifferentCallerDefaults() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(ConfigValueCacheConfig.defaultConfig());
    AtomicInteger callCount = new AtomicInteger();

    // First call: backend returns the sentinel, indicating key is not set on the backend.
    String first = cache.getOrFetch("custom.missing.key", "defaultA", (name, sentinel) -> {
      callCount.incrementAndGet();
      return sentinel;
    });
    Assert.assertEquals("defaultA", first);
    Assert.assertEquals(1, callCount.get());

    // Second call with different default: served from negative cache, returns new default without backend call.
    String second = cache.getOrFetch("custom.missing.key", "defaultB", (name, sentinel) -> {
      callCount.incrementAndGet();
      return "shouldNotBeCalled";
    });
    Assert.assertEquals("defaultB", second);
    Assert.assertEquals(1, callCount.get());
  }

  @Test
  public void expiresEntryAfterTtl() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    ConfigValueCache cache = new ConfigValueCache(new ConfigValueCacheConfig(5_000L, 100), clock::get);
    AtomicInteger callCount = new AtomicInteger();

    String first = cache.getOrFetch("test.key", "def", (name, sentinel) -> {
      callCount.incrementAndGet();
      return "val1";
    });
    Assert.assertEquals("val1", first);
    Assert.assertEquals(1, callCount.get());

    // Advance clock within TTL
    clock.addAndGet(3_000L);
    String cached = cache.getOrFetch("test.key", "def", (name, sentinel) -> {
      callCount.incrementAndGet();
      return "val2";
    });
    Assert.assertEquals("val1", cached);
    Assert.assertEquals(1, callCount.get());

    // Advance clock past TTL
    clock.addAndGet(3_000L);
    String refreshed = cache.getOrFetch("test.key", "def", (name, sentinel) -> {
      callCount.incrementAndGet();
      return "val2";
    });
    Assert.assertEquals("val2", refreshed);
    Assert.assertEquals(2, callCount.get());
  }

  @Test
  public void singleFlightDeduplicatesConcurrentRequests() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(ConfigValueCacheConfig.defaultConfig());
    int threads = 16;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch startGate = new CountDownLatch(1);
    CountDownLatch backendEntered = new CountDownLatch(1);
    AtomicInteger backendCalls = new AtomicInteger();

    try {
      List<Future<String>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        futures.add(executor.submit(() -> {
          startGate.await();
          try {
            return cache.getOrFetch("concurrent.key", "default", (name, sentinel) -> {
              backendCalls.incrementAndGet();
              backendEntered.countDown();
              Thread.sleep(50);
              return "sharedBackendValue";
            });
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
        }));
      }

      startGate.countDown();
      Assert.assertTrue(backendEntered.await(5, TimeUnit.SECONDS));

      for (Future<String> future : futures) {
        Assert.assertEquals("sharedBackendValue", future.get(5, TimeUnit.SECONDS));
      }
      Assert.assertEquals(1, backendCalls.get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void backendExceptionsAreNotCached() {
    ConfigValueCache cache = new ConfigValueCache(ConfigValueCacheConfig.defaultConfig());
    AtomicInteger calls = new AtomicInteger();

    try {
      cache.getOrFetch("failing.key", "def", (name, sentinel) -> {
        calls.incrementAndGet();
        throw new RuntimeException("Backend transport error");
      });
      Assert.fail("Expected exception");
    } catch (Throwable expected) {
      Assert.assertTrue(expected.getMessage().contains("Backend transport error"));
    }
    Assert.assertEquals(1, calls.get());

    // Next attempt retries the backend instead of returning a cached failure
    try {
      String success = cache.getOrFetch("failing.key", "def", (name, sentinel) -> {
        calls.incrementAndGet();
        return "recoveredValue";
      });
      Assert.assertEquals("recoveredValue", success);
      Assert.assertEquals(2, calls.get());
    } catch (Throwable t) {
      Assert.fail("Unexpected exception on retry: " + t);
    }
  }

  @Test
  public void disabledCacheAlwaysInvokesFetcherDirectly() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(ConfigValueCacheConfig.disabled());
    AtomicInteger calls = new AtomicInteger();

    String first = cache.getOrFetch("key", "def1", (name, defaultVal) -> {
      calls.incrementAndGet();
      Assert.assertEquals("def1", defaultVal);
      return "v1";
    });
    Assert.assertEquals("v1", first);
    Assert.assertEquals(1, calls.get());

    String second = cache.getOrFetch("key", "def2", (name, defaultVal) -> {
      calls.incrementAndGet();
      Assert.assertEquals("def2", defaultVal);
      return "v2";
    });
    Assert.assertEquals("v2", second);
    Assert.assertEquals(2, calls.get());
    Assert.assertEquals(0, cache.size());
  }

  @Test
  public void prunesExpiredEntriesWhenFull() throws Throwable {
    AtomicLong clock = new AtomicLong(100L);
    ConfigValueCache cache = new ConfigValueCache(new ConfigValueCacheConfig(50L, 2), clock::get);

    cache.getOrFetch("k1", "d", (n, s) -> "v1");
    cache.getOrFetch("k2", "d", (n, s) -> "v2");
    Assert.assertEquals(2, cache.size());

    // Advance clock past expiration
    clock.set(200L);

    // Adding third key triggers prune of expired entries
    cache.getOrFetch("k3", "d", (n, s) -> "v3");
    Assert.assertTrue(cache.size() <= 2);
  }

  @Test
  public void aFullCacheDropsTheOldestValueInsteadOfEverything() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000L);
    ConfigValueCache cache = new ConfigValueCache(new ConfigValueCacheConfig(60_000L, 3), clock::get);
    AtomicInteger fetches = new AtomicInteger();
    for (String key : List.of("k1", "k2", "k3", "k4")) {
      clock.addAndGet(10L);
      cache.getOrFetch(key, "d", (name, sentinel) -> {
        fetches.incrementAndGet();
        return name + "_value";
      });
    }
    Assert.assertEquals(3, cache.size());
    int before = fetches.get();
    Assert.assertEquals("k4_value", cache.getOrFetch("k4", "d", (name, sentinel) -> {
      fetches.incrementAndGet();
      return "refetched";
    }));
    Assert.assertEquals("k2_value", cache.getOrFetch("k2", "d", (name, sentinel) -> {
      fetches.incrementAndGet();
      return "refetched";
    }));
    Assert.assertEquals("recent values survive", before, fetches.get());
  }

  @Test
  public void inFlightFetchDoesNotResurrectStaleValueAcrossInvalidateAll() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(new ConfigValueCacheConfig(60_000L, 100));
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);

    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<String> stale = pool.submit(() -> {
        try {
          return cache.getOrFetch("k", "def", (name, sentinel) -> {
            entered.countDown();
            release.await(5L, TimeUnit.SECONDS);
            return "stale";
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      });

      Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));
      cache.invalidateAll();
      release.countDown();
      Assert.assertEquals("stale", stale.get(5L, TimeUnit.SECONDS));

      AtomicInteger loads = new AtomicInteger();
      String fresh = cache.getOrFetch("k", "def", (name, sentinel) -> {
        loads.incrementAndGet();
        return "fresh";
      });
      Assert.assertEquals("fresh", fresh);
      Assert.assertEquals(1, loads.get());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void waiterRestoresInterruptFlagWhenInterruptedDuringInFlightFetch() throws Throwable {
    ConfigValueCache cache = new ConfigValueCache(new ConfigValueCacheConfig(60_000L, 100));
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<String> owner = pool.submit(() -> {
        try {
          return cache.getOrFetch("k", "def", (name, sentinel) -> {
            entered.countDown();
            release.await(5L, TimeUnit.SECONDS);
            return "v1";
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      });
      Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));

      java.util.concurrent.atomic.AtomicBoolean interruptedFlagAfterCatch =
          new java.util.concurrent.atomic.AtomicBoolean();
      java.util.concurrent.atomic.AtomicBoolean threwInterrupted =
          new java.util.concurrent.atomic.AtomicBoolean();
      Thread waiter = new Thread(() -> {
        try {
          cache.getOrFetch("k", "def", (name, sentinel) -> "unexpected");
        } catch (InterruptedException e) {
          threwInterrupted.set(true);
          interruptedFlagAfterCatch.set(Thread.currentThread().isInterrupted());
        } catch (Throwable ignored) {
        }
      });
      waiter.start();
      Thread.sleep(50L);
      waiter.interrupt();
      waiter.join(5_000L);
      release.countDown();
      owner.get(5L, TimeUnit.SECONDS);

      Assert.assertTrue(threwInterrupted.get());
      Assert.assertTrue(interruptedFlagAfterCatch.get());
    } finally {
      pool.shutdownNow();
    }
  }
}
