package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseMetadataCacheTest {

  @Test
  public void returnsCachedDatabaseWhileValid() throws Throwable {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of("analysts"));

    Database first = cache.get("cat1", "sales", user, () -> {
      loads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      db.setDescription("sales db");
      return db;
    });

    Database second = cache.get("cat1", "sales", user, () -> {
      loads.incrementAndGet();
      return new Database();
    });

    Assert.assertEquals(1, loads.get());
    Assert.assertEquals("sales", first.getName());
    Assert.assertEquals("sales", second.getName());
    Assert.assertEquals("sales db", second.getDescription());
    Assert.assertNotSame(first, second);
  }

  @Test
  public void defensiveCopyProtectsCachedInstance() throws Throwable {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    Database original = cache.get("cat1", "sales", user, () -> {
      Database db = new Database();
      db.setName("sales");
      db.setParameters(new HashMap<>());
      db.putToParameters("k1", "v1");
      return db;
    });

    original.setName("mutated");
    original.putToParameters("k1", "mutated");

    Database fresh = cache.get("cat1", "sales", user, () -> {
      Assert.fail("should be cached");
      return null;
    });
    Assert.assertEquals("sales", fresh.getName());
    Assert.assertEquals("v1", fresh.getParameters().get("k1"));
  }

  @Test
  public void expiresAfterTtl() throws Throwable {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(30L, 100));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("cat1", "sales", user, () -> {
      loads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      return db;
    });

    Thread.sleep(60L);

    cache.get("cat1", "sales", user, () -> {
      loads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      return db;
    });

    Assert.assertEquals(2, loads.get());
  }

  @Test
  public void invalidatesSpecificDatabase() throws Throwable {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100));
    AtomicInteger salesLoads = new AtomicInteger();
    AtomicInteger analyticsLoads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    cache.get("cat1", "sales", user, () -> {
      salesLoads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      return db;
    });
    cache.get("cat1", "analytics", user, () -> {
      analyticsLoads.incrementAndGet();
      Database db = new Database();
      db.setName("analytics");
      return db;
    });

    cache.invalidate("cat1", "sales");

    cache.get("cat1", "sales", user, () -> {
      salesLoads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      return db;
    });
    cache.get("cat1", "analytics", user, () -> {
      analyticsLoads.incrementAndGet();
      Database db = new Database();
      db.setName("analytics");
      return db;
    });

    Assert.assertEquals(2, salesLoads.get());
    Assert.assertEquals(1, analyticsLoads.get());
  }

  @Test
  public void singleFlightCoalescesConcurrentLoads() throws Exception {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    int threads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    CountDownLatch startLatch = new CountDownLatch(1);
    List<Future<Database>> futures = new ArrayList<>();

    for (int i = 0; i < threads; i++) {
      futures.add(executor.submit(() -> {
        startLatch.await();
        try {
          return cache.get("cat1", "sales", user, () -> {
            loads.incrementAndGet();
            Thread.sleep(50L);
            Database db = new Database();
            db.setName("sales");
            return db;
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      }));
    }

    startLatch.countDown();
    for (Future<Database> f : futures) {
      Database db = f.get(5L, TimeUnit.SECONDS);
      Assert.assertEquals("sales", db.getName());
    }
    executor.shutdown();

    Assert.assertEquals(1, loads.get());
  }

  @Test
  public void batchScanProgressExtendsExpirationForEarlierDatabases() throws Throwable {
    DatabaseListCache listCache = new DatabaseListCache(
        new io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig(50L, 100));
    DatabaseMetadataCache cache = new DatabaseMetadataCache(
        new DatabaseMetadataCacheConfig(50L, 100), listCache);
    AtomicInteger db1Loads = new AtomicInteger();
    AtomicInteger db2Loads = new AtomicInteger();
    AtomicInteger listLoads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // Step 1: Initial list load
    listCache.get("get_all_databases", "cat1", null, user, () -> {
      listLoads.incrementAndGet();
      return List.of("db1", "db2");
    });

    // Step 2: Load db1 at t=0
    cache.get("cat1", "db1", user, () -> {
      db1Loads.incrementAndGet();
      Database db = new Database();
      db.setName("db1");
      return db;
    });

    // Step 3: Wait 30ms, then load db2 at t=30ms (simulating progressive batch scan)
    Thread.sleep(30L);
    cache.get("cat1", "db2", user, () -> {
      db2Loads.incrementAndGet();
      Database db = new Database();
      db.setName("db2");
      return db;
    });

    // Step 4: Wait another 30ms (total 60ms elapsed since db1 and listCache were first loaded)
    // 60ms > 50ms TTL, but db2's load extended db1 and listCache expiration to 30ms + 50ms = 80ms!
    Thread.sleep(30L);

    // Reading db1 at t=60ms should be a cache hit (0 extra loads)
    Database cachedDb1 = cache.get("cat1", "db1", user, () -> {
      db1Loads.incrementAndGet();
      return new Database();
    });
    Assert.assertEquals("db1", cachedDb1.getName());
    Assert.assertEquals(1, db1Loads.get());

    // Reading listCache at t=60ms should also be a cache hit (0 extra loads)
    List<String> cachedList = listCache.get("get_all_databases", "cat1", null, user, () -> {
      listLoads.incrementAndGet();
      return List.of();
    });
    Assert.assertEquals(List.of("db1", "db2"), cachedList);
    Assert.assertEquals(1, listLoads.get());
  }

  @Test
  public void cachesPerUserWhenSharedDisabled() throws Throwable {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100, false));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext alice = new ImpersonationContext("alice", List.of("group1"));
    ImpersonationContext bob = new ImpersonationContext("bob", List.of("group2"));

    cache.get("cat1", "sales", alice, () -> {
      loads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      return db;
    });
    cache.get("cat1", "sales", bob, () -> {
      loads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      return db;
    });

    Assert.assertEquals(2, loads.get());
  }

  @Test
  public void sharesCacheAcrossUsersWhenSharedEnabled() throws Throwable {
    DatabaseMetadataCache cache = new DatabaseMetadataCache(new DatabaseMetadataCacheConfig(60_000L, 100, true));
    AtomicInteger loads = new AtomicInteger();
    ImpersonationContext alice = new ImpersonationContext("alice", List.of("group1"));
    ImpersonationContext bob = new ImpersonationContext("bob", List.of("group2"));

    Database aliceDb = cache.get("cat1", "sales", alice, () -> {
      loads.incrementAndGet();
      Database db = new Database();
      db.setName("sales");
      db.setDescription("sales db");
      return db;
    });
    Database bobDb = cache.get("cat1", "sales", bob, () -> {
      loads.incrementAndGet();
      return new Database();
    });

    Assert.assertEquals(1, loads.get());
    Assert.assertEquals("sales", aliceDb.getName());
    Assert.assertEquals("sales", bobDb.getName());
    Assert.assertEquals("sales db", bobDb.getDescription());
    Assert.assertNotSame(aliceDb, bobDb);
  }
}
