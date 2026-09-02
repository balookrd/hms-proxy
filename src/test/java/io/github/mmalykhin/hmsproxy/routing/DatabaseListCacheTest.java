package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseListCacheTest {

  @Test
  public void returnsCachedListWhileValid() throws Throwable {
    DatabaseListCache cache = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    List<String> first = cache.get("get_all_databases", "cat1", null, user, () -> {
      loads.incrementAndGet();
      return List.of("db1", "db2");
    });

    List<String> second = cache.get("get_all_databases", "cat1", null, user, () -> {
      loads.incrementAndGet();
      return List.of("other");
    });

    Assert.assertEquals(1, loads.get());
    Assert.assertEquals(List.of("db1", "db2"), first);
    Assert.assertEquals(List.of("db1", "db2"), second);
    Assert.assertNotSame(first, second);
  }

  @Test
  public void invalidatesCatalog() throws Throwable {
    DatabaseListCache cache = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100));
    AtomicInteger cat1Loads = new AtomicInteger();
    AtomicInteger cat2Loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("get_all_databases", "cat1", null, user, () -> {
      cat1Loads.incrementAndGet();
      return List.of("db1");
    });
    cache.get("get_all_databases", "cat2", null, user, () -> {
      cat2Loads.incrementAndGet();
      return List.of("db2");
    });

    cache.invalidate("cat1");

    cache.get("get_all_databases", "cat1", null, user, () -> {
      cat1Loads.incrementAndGet();
      return List.of("db1_new");
    });
    cache.get("get_all_databases", "cat2", null, user, () -> {
      cat2Loads.incrementAndGet();
      return List.of("db2");
    });

    Assert.assertEquals(2, cat1Loads.get());
    Assert.assertEquals(1, cat2Loads.get());
  }

  @Test
  public void singleFlightCoalescesConcurrentLoads() throws Exception {
    DatabaseListCache cache = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    int threads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<List<String>>> futures = new ArrayList<>();

    for (int i = 0; i < threads; i++) {
      futures.add(executor.submit(() -> {
        startLatch.await();
        try {
          return cache.get("get_all_databases", "cat1", null, user, () -> {
            loads.incrementAndGet();
            Thread.sleep(50L);
            return List.of("db1", "db2");
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }));
    }

    startLatch.countDown();
    for (Future<List<String>> f : futures) {
      List<String> dbs = f.get(5L, TimeUnit.SECONDS);
      Assert.assertEquals(List.of("db1", "db2"), dbs);
    }
    executor.shutdown();

    Assert.assertEquals(1, loads.get());
  }

  @Test
  public void cachesPerUserWhenSharedDisabled() throws Throwable {
    DatabaseListCache cache = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, false));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext alice = new ImpersonationContext("alice", List.of("group1"));
    ImpersonationContext bob = new ImpersonationContext("bob", List.of("group2"));

    cache.get("get_all_databases", "cat1", null, alice, () -> {
      loads.incrementAndGet();
      return List.of("db1", "db2");
    });
    cache.get("get_all_databases", "cat1", null, bob, () -> {
      loads.incrementAndGet();
      return List.of("db1", "db2");
    });

    Assert.assertEquals(2, loads.get());
  }

  @Test
  public void sharesCacheAcrossUsersWhenSharedEnabled() throws Throwable {
    DatabaseListCache cache = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext alice = new ImpersonationContext("alice", List.of("group1"));
    ImpersonationContext bob = new ImpersonationContext("bob", List.of("group2"));

    List<String> aliceList = cache.get("get_all_databases", "cat1", null, alice, () -> {
      loads.incrementAndGet();
      return List.of("db1", "db2");
    });
    List<String> bobList = cache.get("get_all_databases", "cat1", null, bob, () -> {
      loads.incrementAndGet();
      return List.of("unexpected");
    });

    Assert.assertEquals(1, loads.get());
    Assert.assertEquals(List.of("db1", "db2"), aliceList);
    Assert.assertEquals(List.of("db1", "db2"), bobList);
    Assert.assertNotSame(aliceList, bobList);
  }

  @Test
  public void recordsMetricsOnHitsMissesAndInvalidation() throws Throwable {
    io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics metrics =
        new io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics();
    DatabaseListCache cache = new DatabaseListCache(new DatabaseListCacheConfig(60_000L, 100, true), metrics);
    ImpersonationContext alice = new ImpersonationContext("alice", List.of());

    // Miss
    cache.get("get_all_databases", "cat1", null, alice, () -> List.of("db1", "db2"));
    // Hit
    cache.get("get_all_databases", "cat1", null, alice, () -> List.of("should_not_load"));

    String rendered = metrics.render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_requests_total{cache=\"database_list\",catalog=\"cat1\",result=\"miss\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_requests_total{cache=\"database_list\",catalog=\"cat1\",result=\"hit\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_entries{cache=\"database_list\",catalog=\"cat1\"} 1.0"));

    // Invalidation
    cache.invalidate("cat1");
    rendered = metrics.render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_invalidations_total{cache=\"database_list\",catalog=\"cat1\",reason=\"write\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_entries{cache=\"database_list\",catalog=\"cat1\"} 0.0"));
  }
}
