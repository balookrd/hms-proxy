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
}
