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
}
