package io.github.mmalykhin.hmsproxy.app;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class SingleFlightCacheTest {
  @Test
  public void reusesCachedValueWithinTtl() {
    AtomicLong clock = new AtomicLong();
    AtomicInteger loads = new AtomicInteger();
    SingleFlightCache<Integer> cache = new SingleFlightCache<>(1_000L, clock::get);

    Assert.assertEquals(Integer.valueOf(1), cache.get(loads::incrementAndGet));
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(999L));
    Assert.assertEquals(Integer.valueOf(1), cache.get(loads::incrementAndGet));
    Assert.assertEquals(1, loads.get());
  }

  @Test
  public void refreshesAfterTtlElapses() {
    AtomicLong clock = new AtomicLong();
    AtomicInteger loads = new AtomicInteger();
    SingleFlightCache<Integer> cache = new SingleFlightCache<>(1_000L, clock::get);

    Assert.assertEquals(Integer.valueOf(1), cache.get(loads::incrementAndGet));
    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1_000L));
    Assert.assertEquals(Integer.valueOf(2), cache.get(loads::incrementAndGet));
    Assert.assertEquals(2, loads.get());
  }

  @Test
  public void zeroTtlRecomputesEveryCall() {
    AtomicInteger loads = new AtomicInteger();
    SingleFlightCache<Integer> cache = new SingleFlightCache<>(0L, () -> 0L);

    Assert.assertEquals(Integer.valueOf(1), cache.get(loads::incrementAndGet));
    Assert.assertEquals(Integer.valueOf(2), cache.get(loads::incrementAndGet));
  }

  @Test
  public void concurrentCallerServesStaleValueInsteadOfWaitingForRefresh() throws Exception {
    AtomicLong clock = new AtomicLong();
    SingleFlightCache<String> cache = new SingleFlightCache<>(1_000L, clock::get);
    Assert.assertEquals("v1", cache.get(() -> "v1"));

    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(2_000L));
    CountDownLatch refreshStarted = new CountDownLatch(1);
    CountDownLatch releaseRefresh = new CountDownLatch(1);
    Thread refresher = new Thread(() -> cache.get(() -> {
      refreshStarted.countDown();
      try {
        releaseRefresh.await(5L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "v2";
    }));
    refresher.setDaemon(true);
    refresher.start();
    Assert.assertTrue(refreshStarted.await(5L, TimeUnit.SECONDS));

    // The second caller must not block behind the in-flight refresh, and must not start its own.
    Assert.assertEquals("v1", cache.get(() -> {
      throw new AssertionError("stale reader must not run the loader");
    }));

    releaseRefresh.countDown();
    refresher.join(5_000L);
    Assert.assertFalse(refresher.isAlive());
    Assert.assertEquals("v2", cache.get(() -> "v3"));
  }

  @Test
  public void failedRefreshDoesNotPoisonTheCache() {
    AtomicLong clock = new AtomicLong();
    SingleFlightCache<String> cache = new SingleFlightCache<>(1_000L, clock::get);
    Assert.assertEquals("v1", cache.get(() -> "v1"));

    clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(2_000L));
    try {
      cache.get(() -> {
        throw new IllegalStateException("probe failed");
      });
      Assert.fail("expected the loader failure to propagate");
    } catch (IllegalStateException expected) {
      Assert.assertEquals("probe failed", expected.getMessage());
    }

    Assert.assertEquals("v2", cache.get(() -> "v2"));
  }

  @Test
  public void tryGetReturnsEmptyQuicklyWhenColdStartRefreshBlocks() throws Exception {
    SingleFlightCache<String> cache = new SingleFlightCache<>(1_000L);
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread initializer = new Thread(() -> cache.get(() -> {
      started.countDown();
      try {
        release.await(5L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return "ready";
    }));
    initializer.setDaemon(true);
    initializer.start();
    Assert.assertTrue(started.await(5L, TimeUnit.SECONDS));

    long startNanos = System.nanoTime();
    Optional<String> fast = cache.tryGet(() -> "unused", 40L);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    Assert.assertTrue("cold-start tryGet must return empty while first probe is still running",
        fast.isEmpty());
    Assert.assertTrue("cold-start wait should remain bounded, took " + elapsedMs + "ms",
        elapsedMs < 1_000L);

    release.countDown();
    initializer.join(5_000L);
    Assert.assertEquals(Optional.of("ready"), cache.tryGet(() -> "unused", 40L));
  }
}
