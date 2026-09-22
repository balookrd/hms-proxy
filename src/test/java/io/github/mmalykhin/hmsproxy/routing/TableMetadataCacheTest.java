package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.TableMetadataCacheConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

public class TableMetadataCacheTest {

  @Test
  public void returnsCachedTableWhileValid() throws Throwable {
    TableMetadataCache cache = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of("analysts"));

    Table first = cache.get("cat1", "sales", "orders", user, () -> {
      loads.incrementAndGet();
      Table tbl = new Table();
      tbl.setDbName("sales");
      tbl.setTableName("orders");
      tbl.setOwner("alice");
      return tbl;
    });

    Table second = cache.get("cat1", "sales", "orders", user, () -> {
      loads.incrementAndGet();
      return new Table();
    });

    Assert.assertEquals(1, loads.get());
    Assert.assertEquals("orders", first.getTableName());
    Assert.assertEquals("orders", second.getTableName());
    Assert.assertEquals("alice", second.getOwner());
    Assert.assertNotSame(first, second);
  }

  @Test
  public void defensiveCopyProtectsCachedInstance() throws Throwable {
    TableMetadataCache cache = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    Table original = cache.get("cat1", "sales", "orders", user, () -> {
      Table tbl = new Table();
      tbl.setDbName("sales");
      tbl.setTableName("orders");
      tbl.setParameters(new HashMap<>());
      tbl.putToParameters("k1", "v1");
      return tbl;
    });

    original.setTableName("mutated");
    original.putToParameters("k1", "mutated");

    Table fresh = cache.get("cat1", "sales", "orders", user, () -> {
      Assert.fail("should be served from cache");
      return null;
    });
    Assert.assertEquals("orders", fresh.getTableName());
    Assert.assertEquals("v1", fresh.getParameters().get("k1"));
  }

  @Test
  public void servesStaleOnErrorWithinGracePeriod() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    TableMetadataCache cache = new TableMetadataCache(
        new TableMetadataCacheConfig(10_000L, 100, true),
        true,
        60_000L,
        null,
        clock::get);

    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // 1. Initial successful load at t=1_000_000
    Table initial = cache.get("cat1", "sales", "orders", user, () -> {
      Table tbl = new Table();
      tbl.setDbName("sales");
      tbl.setTableName("orders");
      return tbl;
    });
    Assert.assertNotNull(initial);

    // 2. Advance time past TTL (t=1_015_000), but within grace period (grace expires at 1_070_000)
    clock.set(1_015_000L);

    // Backend fails on reload
    Table stale = cache.get("cat1", "sales", "orders", user, () -> {
      throw new MetaException("Remote metastore down!");
    });
    Assert.assertNotNull(stale);
    Assert.assertEquals("orders", stale.getTableName());
  }

  @Test
  public void throwsWhenStaleGracePeriodExpired() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    TableMetadataCache cache = new TableMetadataCache(
        new TableMetadataCacheConfig(10_000L, 100, true),
        true,
        20_000L,
        null,
        clock::get);

    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // 1. Initial load
    cache.get("cat1", "sales", "orders", user, () -> {
      Table tbl = new Table();
      tbl.setDbName("sales");
      tbl.setTableName("orders");
      return tbl;
    });

    // 2. Advance time past grace period: TTL=10s, grace=20s -> expired after 30s
    clock.set(1_035_000L);

    try {
      cache.get("cat1", "sales", "orders", user, () -> {
        throw new MetaException("Remote metastore down!");
      });
      Assert.fail("Expected exception when grace period expired");
    } catch (MetaException e) {
      Assert.assertTrue(e.getMessage().contains("Remote metastore down!"));
    }
  }

  @Test
  public void invalidatesSpecificTable() throws Throwable {
    TableMetadataCache cache = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger ordersLoads = new AtomicInteger();
    AtomicInteger itemsLoads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("cat1", "sales", "orders", user, () -> {
      ordersLoads.incrementAndGet();
      Table t = new Table();
      t.setTableName("orders");
      return t;
    });
    cache.get("cat1", "sales", "items", user, () -> {
      itemsLoads.incrementAndGet();
      Table t = new Table();
      t.setTableName("items");
      return t;
    });

    Assert.assertEquals(1, ordersLoads.get());
    Assert.assertEquals(1, itemsLoads.get());

    // Invalidate orders only
    cache.invalidateTable("cat1", "sales", "orders");

    cache.get("cat1", "sales", "orders", user, () -> {
      ordersLoads.incrementAndGet();
      Table t = new Table();
      t.setTableName("orders");
      return t;
    });
    cache.get("cat1", "sales", "items", user, () -> {
      itemsLoads.incrementAndGet();
      Table t = new Table();
      t.setTableName("items");
      return t;
    });

    Assert.assertEquals(2, ordersLoads.get());
    Assert.assertEquals(1, itemsLoads.get());
  }

  @Test
  public void evictsLruWhenCapacityExceeded() throws Throwable {
    TableMetadataCache cache = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 2, true));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("cat1", "sales", "t1", user, () -> {
      Table t = new Table();
      t.setTableName("t1");
      return t;
    });
    cache.get("cat1", "sales", "t2", user, () -> {
      Table t = new Table();
      t.setTableName("t2");
      return t;
    });

    // Adding t3 should evict t1 (LRU)
    cache.get("cat1", "sales", "t3", user, () -> {
      Table t = new Table();
      t.setTableName("t3");
      return t;
    });

    AtomicInteger t1Reload = new AtomicInteger();
    cache.get("cat1", "sales", "t1", user, () -> {
      t1Reload.incrementAndGet();
      Table t = new Table();
      t.setTableName("t1");
      return t;
    });
    Assert.assertEquals(1, t1Reload.get());
  }

  @Test
  public void singleFlightDeduplicatesConcurrentLoads() throws Throwable {
    TableMetadataCache cache = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    int threadCount = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startGate = new CountDownLatch(1);
    List<Future<Table>> futures = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      futures.add(pool.submit(() -> {
        startGate.await();
        try {
          return cache.get("cat1", "sales", "orders", user, () -> {
            loads.incrementAndGet();
            Thread.sleep(30L);
            Table t = new Table();
            t.setTableName("orders");
            return t;
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }));
    }

    startGate.countDown();
    pool.shutdown();
    Assert.assertTrue(pool.awaitTermination(5L, TimeUnit.SECONDS));

    for (Future<Table> f : futures) {
      Table t = f.get();
      Assert.assertNotNull(t);
      Assert.assertEquals("orders", t.getTableName());
    }
    Assert.assertEquals(1, loads.get());
  }
}
