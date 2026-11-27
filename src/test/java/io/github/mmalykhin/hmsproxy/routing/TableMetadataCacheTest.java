package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
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

  @Test
  public void singleFlightDoesNotResurrectStaleTableAfterClearAndRestoresInterrupt() throws Throwable {
    TableMetadataCache cache = new TableMetadataCache(new TableMetadataCacheConfig(60_000L, 100, true));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<Table> leader = pool.submit(() -> {
        try {
          return cache.get("cat1", "sales", "orders", user, () -> {
            entered.countDown();
            release.await(5L, TimeUnit.SECONDS);
            Table t = new Table();
            t.setTableName("orders");
            t.putToParameters("ver", "stale");
            return t;
          });
        } catch (Throwable t) {
          throw new RuntimeException(t);
        }
      });
      Assert.assertTrue(entered.await(5L, TimeUnit.SECONDS));

      java.util.concurrent.atomic.AtomicBoolean interruptedFlag = new java.util.concurrent.atomic.AtomicBoolean();
      Thread waiter = new Thread(() -> {
        try {
          cache.get("cat1", "sales", "orders", user, () -> new Table());
        } catch (InterruptedException e) {
          interruptedFlag.set(Thread.currentThread().isInterrupted());
        } catch (Throwable ignored) {
        }
      });
      waiter.start();
      Thread.sleep(50L);
      waiter.interrupt();
      waiter.join(5_000L);
      Assert.assertTrue(interruptedFlag.get());

      cache.clear();
      release.countDown();
      leader.get(5L, TimeUnit.SECONDS);

      AtomicInteger loads = new AtomicInteger();
      Table fresh = cache.get("cat1", "sales", "orders", user, () -> {
        loads.incrementAndGet();
        Table t = new Table();
        t.setTableName("orders");
        t.putToParameters("ver", "fresh");
        return t;
      });
      Assert.assertEquals(1, loads.get());
      Assert.assertEquals("fresh", fresh.getParameters().get("ver"));
      fresh.putToParameters("ver", "caller_mutated");

      Table cached = cache.get("cat1", "sales", "orders", user, () -> new Table());
      Assert.assertEquals("fresh", cached.getParameters().get("ver"));
    } finally {
      pool.shutdownNow();
    }
  }

  public static final class ForeignPartitionResponse {
    private String state;

    public ForeignPartitionResponse(String state) {
      this.state = state;
    }

    public ForeignPartitionResponse deepCopy() {
      return new ForeignPartitionResponse(this.state);
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }
  }

  @Test
  public void partitionCacheDeepCopiesReflectiveThriftObjectsAndLists() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(
        new io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig(60_000L, 100, true));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    String sig = RoutingSupport.encodeQuerySignature("get_partitions_req", new Object[] {(short) -1}, 0);

    ForeignPartitionResponse first = cache.get("hdp", "beemetrics", "events", sig, user,
        () -> new ForeignPartitionResponse("original"));
    first.setState("mutated");

    ForeignPartitionResponse second = cache.get("hdp", "beemetrics", "events", sig, user,
        () -> new ForeignPartitionResponse("reloaded"));
    Assert.assertEquals("original", second.getState());
  }

  @Test
  public void requestWrapperReadsUseTableAndPartitionCachesAndServeStaleOnOutage() throws Throwable {
    io.github.mmalykhin.hmsproxy.config.ProxyConfig config =
        io.github.mmalykhin.hmsproxy.config.ProxyConfig.builder()
            .server(new io.github.mmalykhin.hmsproxy.config.server.ServerConfig("test", "127.0.0.1", 9083, 1, 4))
            .security(new io.github.mmalykhin.hmsproxy.config.security.SecurityConfig(
                io.github.mmalykhin.hmsproxy.config.security.SecurityMode.NONE, null, null, null, null, false, java.util.Map.of()))
            .catalogDbSeparator("__")
            .defaultCatalog("janus")
            .catalogs(java.util.Map.of(
                "janus", new io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig(
                    "janus", "janus", "file:///janus", false,
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE, List.of(),
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL, List.of(), java.util.Map.of(),
                    List.of(), false, null, null, java.util.Map.of("hive.metastore.uris", "thrift://janus")),
                "hdp", new io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig(
                    "hdp", "hdp", "file:///hdp", false,
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE, List.of(),
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL, List.of(), java.util.Map.of(),
                    List.of(), true, null, null, java.util.Map.of("hive.metastore.uris", "thrift://hdp"))))
            .syntheticReadLockStore(
                io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig.inMemory())
            .latencyRouting(new io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig(
                null, null, null, null,
                io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS,
                null, null, null, false,
                new TableMetadataCacheConfig(60_000L, 1000, true),
                new io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig(60_000L, 1000, true),
                true, 300_000L,
                io.github.mmalykhin.hmsproxy.config.routing.DistributedCacheInvalidationConfig.disabled(),
                io.github.mmalykhin.hmsproxy.config.routing.MergedDatabaseResolutionConfig.defaultConfig()))
            .build();

    AtomicInteger hdpTableCalls = new AtomicInteger();
    AtomicInteger hdpPartCalls = new AtomicInteger();
    io.github.mmalykhin.hmsproxy.backend.CatalogBackend janus =
        RoutingMetaStoreProxyTestSupport.newBackend(
            config, config.catalogs().get("janus"),
            new io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter(),
            RoutingMetaStoreProxyTestSupport.newBackendRuntime(
                config, config.catalogs().get("janus"),
                RoutingMetaStoreProxyTestSupport.newSession((proxy, method, args) -> {
                  throw new org.apache.hadoop.hive.metastore.api.NoSuchObjectException("not in janus");
                })));
    io.github.mmalykhin.hmsproxy.backend.CatalogBackend hdp =
        RoutingMetaStoreProxyTestSupport.newBackend(
            config, config.catalogs().get("hdp"),
            new io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter(),
            RoutingMetaStoreProxyTestSupport.newBackendRuntime(
                config, config.catalogs().get("hdp"),
                RoutingMetaStoreProxyTestSupport.newSession((proxy, method, args) -> {
                  if ("get_database".equals(method.getName())) {
                    org.apache.hadoop.hive.metastore.api.Database db =
                        new org.apache.hadoop.hive.metastore.api.Database();
                    db.setName((String) args[0]);
                    return db;
                  }
                  if ("get_table_req".equals(method.getName())) {
                    hdpTableCalls.incrementAndGet();
                    Table t = new Table();
                    t.setDbName("Beemetrics");
                    t.setTableName("events");
                    return new org.apache.hadoop.hive.metastore.api.GetTableResult(t);
                  }
                  if ("get_partitions_by_names".equals(method.getName())) {
                    hdpPartCalls.incrementAndGet();
                    org.apache.hadoop.hive.metastore.api.Partition p =
                        new org.apache.hadoop.hive.metastore.api.Partition();
                    p.setDbName("Beemetrics");
                    p.setTableName("events");
                    p.setValues(List.of("2026-11-27"));
                    return List.of(p);
                  }
                  throw new UnsupportedOperationException(method.getName());
                })));

    java.util.LinkedHashMap<String, io.github.mmalykhin.hmsproxy.backend.CatalogBackend> backends =
        new java.util.LinkedHashMap<>();
    backends.put("janus", janus);
    backends.put("hdp", hdp);
    CatalogRouter router = new CatalogRouter(config, backends);
    router.updateDatabases("janus", List.of("default"));
    router.updateDatabases("hdp", List.of("Beemetrics"));
    io.github.mmalykhin.hmsproxy.federation.FederationLayer federation =
        new io.github.mmalykhin.hmsproxy.federation.FederationLayer(config, router);
    RoutingMetaStoreProxy proxy = new RoutingMetaStoreProxy(
        config, router, federation, null, new io.github.mmalykhin.hmsproxy.observability.ProxyObservability(config));

    org.apache.hadoop.hive.metastore.api.GetTableRequest tblReq =
        new org.apache.hadoop.hive.metastore.api.GetTableRequest("Beemetrics", "events");
    org.apache.hadoop.hive.metastore.api.GetTableResult r1 =
        (org.apache.hadoop.hive.metastore.api.GetTableResult) proxy.get_table_req(tblReq);
    org.apache.hadoop.hive.metastore.api.GetTableResult r2 =
        (org.apache.hadoop.hive.metastore.api.GetTableResult) proxy.get_table_req(tblReq);
    Assert.assertEquals("events", r1.getTable().getTableName());
    Assert.assertEquals("events", r2.getTable().getTableName());
    Assert.assertEquals("get_table_req must hit TableMetadataCache on second call", 1, hdpTableCalls.get());

    org.junit.Assume.assumeTrue(java.nio.file.Files.isReadable(RoutingMetaStoreProxyTestSupport.HIVE_4_JAR));
    ClassLoader hdpCl = new io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader(
        io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader.buildIsolatedRuntimeUrls(
            RoutingMetaStoreProxyTestSupport.HIVE_4_JAR),
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> byNamesReqClass = hdpCl.loadClass("org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest");
    Object partReq = byNamesReqClass.getConstructor().newInstance();
    byNamesReqClass.getMethod("setDb_name", String.class).invoke(partReq, "Beemetrics");
    byNamesReqClass.getMethod("setTbl_name", String.class).invoke(partReq, "events");
    byNamesReqClass.getMethod("setNames", List.class).invoke(partReq, List.of("dt=2026-11-27"));

    Object p1 = proxy.get_partitions_by_names_req(partReq);
    Object p2 = proxy.get_partitions_by_names_req(partReq);
    List<?> parts1 = (List<?>) p1.getClass().getMethod("getPartitions").invoke(p1);
    List<?> parts2 = (List<?>) p2.getClass().getMethod("getPartitions").invoke(p2);
    Assert.assertEquals(1, parts1.size());
    Assert.assertEquals(1, parts2.size());
    Assert.assertEquals("get_partitions_by_names_req must hit PartitionMetadataCache on second call",
        1, hdpPartCalls.get());
  }
}
