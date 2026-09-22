package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig;
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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Assert;
import org.junit.Test;

public class PartitionMetadataCacheTest {

  @Test
  public void returnsCachedPartitionsWhileValid() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    List<String> first = cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () -> {
      loads.incrementAndGet();
      return List.of("dt=2026-09-01", "dt=2026-09-02");
    });

    List<String> second = cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () -> {
      loads.incrementAndGet();
      return List.of();
    });

    Assert.assertEquals(1, loads.get());
    Assert.assertEquals(List.of("dt=2026-09-01", "dt=2026-09-02"), first);
    Assert.assertEquals(List.of("dt=2026-09-01", "dt=2026-09-02"), second);
    Assert.assertNotSame(first, second);
  }

  @Test
  public void differentiatesByQuerySignature() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    List<String> allParts = cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () -> {
      loads.incrementAndGet();
      return List.of("dt=2026-09-01", "dt=2026-09-02");
    });

    List<String> onePart = cache.get("cat1", "sales", "orders", "get_partition_names:[1]", user, () -> {
      loads.incrementAndGet();
      return List.of("dt=2026-09-01");
    });

    Assert.assertEquals(2, loads.get());
    Assert.assertEquals(2, allParts.size());
    Assert.assertEquals(1, onePart.size());
  }

  @Test
  public void defensiveCopyProtectsPartitionObjects() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    Partition initial = new Partition();
    initial.setDbName("sales");
    initial.setTableName("orders");
    initial.setValues(List.of("2026-09-01"));
    initial.setParameters(new HashMap<>());
    initial.putToParameters("k1", "v1");

    Partition loaded = cache.get("cat1", "sales", "orders", "get_partition:[2026-09-01]", user, () -> initial);
    loaded.putToParameters("k1", "mutated");

    Partition fresh = cache.get("cat1", "sales", "orders", "get_partition:[2026-09-01]", user, () -> {
      Assert.fail("should be served from cache");
      return null;
    });

    Assert.assertEquals("v1", fresh.getParameters().get("k1"));
  }

  @Test
  public void servesStaleOnErrorWithinGracePeriod() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    PartitionMetadataCache cache = new PartitionMetadataCache(
        new PartitionMetadataCacheConfig(10_000L, 100, true),
        true,
        60_000L,
        null,
        clock::get);

    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // 1. Initial successful load at t=1_000_000
    List<String> initial = cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () ->
        List.of("dt=2026-09-01"));
    Assert.assertEquals(1, initial.size());

    // 2. Advance time past TTL (t=1_015_000), within grace period
    clock.set(1_015_000L);

    // Backend fails on reload
    List<String> stale = cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () -> {
      throw new MetaException("Remote metastore unavailable!");
    });
    Assert.assertEquals(List.of("dt=2026-09-01"), stale);
  }

  @Test
  public void throwsWhenStaleGracePeriodExpired() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    PartitionMetadataCache cache = new PartitionMetadataCache(
        new PartitionMetadataCacheConfig(10_000L, 100, true),
        true,
        20_000L,
        null,
        clock::get);

    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () ->
        List.of("dt=2026-09-01"));

    // Advance time past grace period: TTL=10s, grace=20s -> expired after 30s
    clock.set(1_035_000L);

    try {
      cache.get("cat1", "sales", "orders", "get_partition_names:[-1]", user, () -> {
        throw new MetaException("Remote metastore unavailable!");
      });
      Assert.fail("Expected exception when grace period expired");
    } catch (MetaException e) {
      Assert.assertTrue(e.getMessage().contains("Remote metastore unavailable!"));
    }
  }

  @Test
  public void invalidatesSpecificTablePartitions() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger ordersLoads = new AtomicInteger();
    AtomicInteger itemsLoads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("cat1", "sales", "orders", "sig1", user, () -> {
      ordersLoads.incrementAndGet();
      return List.of("p1");
    });
    cache.get("cat1", "sales", "items", "sig1", user, () -> {
      itemsLoads.incrementAndGet();
      return List.of("p2");
    });

    Assert.assertEquals(1, ordersLoads.get());
    Assert.assertEquals(1, itemsLoads.get());

    cache.invalidateTable("cat1", "sales", "orders");

    cache.get("cat1", "sales", "orders", "sig1", user, () -> {
      ordersLoads.incrementAndGet();
      return List.of("p1");
    });
    cache.get("cat1", "sales", "items", "sig1", user, () -> {
      itemsLoads.incrementAndGet();
      return List.of("p2");
    });

    Assert.assertEquals(2, ordersLoads.get());
    Assert.assertEquals(1, itemsLoads.get());
  }

  @Test
  public void singleFlightDeduplicatesConcurrentLoads() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(new PartitionMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    int threadCount = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch startGate = new CountDownLatch(1);
    List<Future<List<String>>> futures = new ArrayList<>();

    for (int i = 0; i < threadCount; i++) {
      futures.add(pool.submit(() -> {
        startGate.await();
        try {
          return cache.get("cat1", "sales", "orders", "names", user, () -> {
            loads.incrementAndGet();
            Thread.sleep(30L);
            return List.of("dt=2026-09-01");
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }));
    }

    startGate.countDown();
    pool.shutdown();
    Assert.assertTrue(pool.awaitTermination(5L, TimeUnit.SECONDS));

    for (Future<List<String>> f : futures) {
      List<String> parts = f.get();
      Assert.assertEquals(List.of("dt=2026-09-01"), parts);
    }
    Assert.assertEquals(1, loads.get());
  }
}
