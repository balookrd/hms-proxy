package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseCacheBackgroundRefreshConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseCacheRefresherTest {

  @Test
  public void refreshesBothCachesWhileActiveWithinWindow() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    PrometheusMetrics metrics = new PrometheusMetrics();

    DatabaseCacheBackgroundRefreshConfig refreshConfig =
        new DatabaseCacheBackgroundRefreshConfig(true, 60_000L, 3_600_000L);
    DatabaseListCache listCache = new DatabaseListCache(
        new DatabaseListCacheConfig(120_000L, 100, true, refreshConfig), metrics, clock::get);
    DatabaseMetadataCache metaCache = new DatabaseMetadataCache(
        new DatabaseMetadataCacheConfig(120_000L, 100, true, refreshConfig), listCache, metrics, clock::get);

    DatabaseCacheRefresher refresher = new DatabaseCacheRefresher(
        listCache, metaCache, refreshConfig, refreshConfig, clock::get, false);

    AtomicInteger listLoads = new AtomicInteger();
    AtomicInteger metaLoads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // 1. Initial client queries (miss -> load)
    List<String> dbs = listCache.get("get_all_databases", "cat1", null, user, () -> {
      listLoads.incrementAndGet();
      return List.of("db1", "db2");
    });
    Assert.assertEquals(1, listLoads.get());
    Assert.assertEquals(List.of("db1", "db2"), dbs);

    Database db = metaCache.get("cat1", "db1", user, () -> {
      metaLoads.incrementAndGet();
      return new Database("db1", "desc1", "hdfs://ns/db1", null);
    });
    Assert.assertEquals(1, metaLoads.get());
    Assert.assertEquals("desc1", db.getDescription());

    // 2. Advance time by 30 seconds (within activity window of 1 hour)
    clock.addAndGet(30_000L);

    // 3. Trigger refresh tick
    refresher.refreshTick();

    // Both should have reloaded
    Assert.assertEquals(2, listLoads.get());
    Assert.assertEquals(2, metaLoads.get());

    // 4. Client reads again: should be cache hits, no extra sync loads
    List<String> dbs2 = listCache.get("get_all_databases", "cat1", null, user, () -> {
      listLoads.incrementAndGet();
      return List.of("db1_new");
    });
    Assert.assertEquals(2, listLoads.get());
    Assert.assertEquals(List.of("db1", "db2"), dbs2);

    Database db2 = metaCache.get("cat1", "db1", user, () -> {
      metaLoads.incrementAndGet();
      return new Database("db1", "unexpected", "hdfs://ns/db1", null);
    });
    Assert.assertEquals(2, metaLoads.get());
    Assert.assertEquals("desc1", db2.getDescription());

    // Verify metrics
    String rendered = metrics.render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_refreshes_total{cache=\"database_list\",catalog=\"cat1\",result=\"success\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_refreshes_total{cache=\"database_metadata\",catalog=\"cat1\",result=\"success\"} 1"));

    refresher.close();
  }

  @Test
  public void pausesRefreshWhenActivityWindowExceeded() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    PrometheusMetrics metrics = new PrometheusMetrics();

    // Activity window: 10 minutes (600,000 ms)
    DatabaseCacheBackgroundRefreshConfig refreshConfig =
        new DatabaseCacheBackgroundRefreshConfig(true, 60_000L, 600_000L);
    DatabaseListCache listCache = new DatabaseListCache(
        new DatabaseListCacheConfig(1_200_000L, 100, true, refreshConfig), metrics, clock::get);
    DatabaseMetadataCache metaCache = new DatabaseMetadataCache(
        new DatabaseMetadataCacheConfig(1_200_000L, 100, true, refreshConfig), listCache, metrics, clock::get);

    DatabaseCacheRefresher refresher = new DatabaseCacheRefresher(
        listCache, metaCache, refreshConfig, refreshConfig, clock::get, false);

    AtomicInteger listLoads = new AtomicInteger();
    AtomicInteger metaLoads = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // Initial query
    listCache.get("get_all_databases", "cat1", null, user, () -> {
      listLoads.incrementAndGet();
      return List.of("db1");
    });
    metaCache.get("cat1", "db1", user, () -> {
      metaLoads.incrementAndGet();
      return new Database("db1", "desc", "hdfs://ns/db1", null);
    });

    Assert.assertEquals(1, listLoads.get());
    Assert.assertEquals(1, metaLoads.get());

    // Advance clock by 15 minutes (> 10 minutes activity window)
    clock.addAndGet(15 * 60_000L);

    // Refresh tick: should skip because client was idle
    refresher.refreshTick();

    Assert.assertEquals(1, listLoads.get());
    Assert.assertEquals(1, metaLoads.get());

    refresher.close();
  }

  @Test
  public void preservesExistingCacheWhenBackgroundRefreshFails() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    PrometheusMetrics metrics = new PrometheusMetrics();

    DatabaseCacheBackgroundRefreshConfig refreshConfig =
        new DatabaseCacheBackgroundRefreshConfig(true, 60_000L, 3_600_000L);
    DatabaseListCache listCache = new DatabaseListCache(
        new DatabaseListCacheConfig(120_000L, 100, true, refreshConfig), metrics, clock::get);
    DatabaseMetadataCache metaCache = new DatabaseMetadataCache(
        new DatabaseMetadataCacheConfig(120_000L, 100, true, refreshConfig), listCache, metrics, clock::get);

    DatabaseCacheRefresher refresher = new DatabaseCacheRefresher(
        listCache, metaCache, refreshConfig, refreshConfig, clock::get, false);

    AtomicInteger attempt = new AtomicInteger();
    ImpersonationContext user = new ImpersonationContext("alice", List.of());

    // Initial load succeeds
    listCache.get("get_all_databases", "cat1", null, user, () -> {
      if (attempt.incrementAndGet() == 1) {
        return List.of("db_safe");
      }
      throw new RuntimeException("Metastore connection refused");
    });

    // Advance 10s
    clock.addAndGet(10_000L);

    // Refresh tick: loader throws exception
    refresher.refreshTick();

    // Verify error metric recorded
    String rendered = metrics.render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_cache_refreshes_total{cache=\"database_list\",catalog=\"cat1\",result=\"failure\"} 1"));

    // Existing data is still intact!
    List<String> dbs = listCache.get("get_all_databases", "cat1", null, user, () -> {
      attempt.incrementAndGet();
      return List.of("fallback");
    });
    Assert.assertEquals(List.of("db_safe"), dbs);

    refresher.close();
  }
}
