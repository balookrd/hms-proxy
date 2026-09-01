package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class RoutingMetaStoreProxyDatabaseMetadataCacheTest {

  @Test
  public void getDatabaseCachesMetadata() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 1, 200L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT,
            new DatabaseListCacheConfig(60_000L, 100),
            new DatabaseMetadataCacheConfig(60_000L, 100)));

    AtomicInteger cat1GetDb = new AtomicInteger();
    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_database".equals(method.getName())) {
                cat1GetDb.incrementAndGet();
                Database db = new Database();
                db.setName((String) args[0]);
                db.setDescription("sales description");
                return db;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    AtomicInteger cat2GetDb = new AtomicInteger();
    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_database".equals(method.getName())) {
                cat2GetDb.incrementAndGet();
                Database db = new Database();
                db.setName((String) args[0]);
                db.setDescription("reports description");
                return db;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);

    Method getDatabaseMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    Database db1 = (Database) handler.invoke(null, getDatabaseMethod, new Object[]{"sales"});
    Database db2 = (Database) handler.invoke(null, getDatabaseMethod, new Object[]{"sales"});

    Assert.assertEquals("sales", db1.getName());
    Assert.assertEquals("sales description", db1.getDescription());
    Assert.assertEquals("sales", db2.getName());
    Assert.assertEquals(1, cat1GetDb.get());

    Database cat2Db1 = (Database) handler.invoke(null, getDatabaseMethod, new Object[]{"catalog2__reports"});
    Database cat2Db2 = (Database) handler.invoke(null, getDatabaseMethod, new Object[]{"catalog2__reports"});

    Assert.assertEquals("catalog2__reports", cat2Db1.getName());
    Assert.assertEquals(1, cat2GetDb.get());
  }

  @Test
  public void alterAndDropDatabaseInvalidatesCache() throws Throwable {
    ProxyConfig config = latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 1, 200L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT,
            new DatabaseListCacheConfig(60_000L, 100),
            new DatabaseMetadataCacheConfig(60_000L, 100)));

    AtomicInteger getDbCalls = new AtomicInteger();
    AtomicInteger alterDbCalls = new AtomicInteger();
    AtomicInteger dropDbCalls = new AtomicInteger();

    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_database".equals(method.getName())) {
                getDbCalls.incrementAndGet();
                Database db = new Database();
                db.setName((String) args[0]);
                return db;
              }
              if ("alter_database".equals(method.getName())) {
                alterDbCalls.incrementAndGet();
                return null;
              }
              if ("drop_database".equals(method.getName())) {
                dropDbCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);

    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
    Method alterDbMethod = ThriftHiveMetastore.Iface.class.getMethod("alter_database", String.class, Database.class);
    Method dropDbMethod = ThriftHiveMetastore.Iface.class.getMethod("drop_database", String.class, boolean.class, boolean.class);

    // Initial get - cached
    handler.invoke(null, getDbMethod, new Object[]{"sales"});
    handler.invoke(null, getDbMethod, new Object[]{"sales"});
    Assert.assertEquals(1, getDbCalls.get());

    // Alter database - invalidates
    Database updated = new Database();
    updated.setName("sales");
    updated.setDescription("new desc");
    handler.invoke(null, alterDbMethod, new Object[]{"sales", updated});
    Assert.assertEquals(1, alterDbCalls.get());

    handler.invoke(null, getDbMethod, new Object[]{"sales"});
    Assert.assertEquals(2, getDbCalls.get());

    // Drop database - invalidates
    handler.invoke(null, dropDbMethod, new Object[]{"sales", true, true});
    Assert.assertEquals(1, dropDbCalls.get());

    handler.invoke(null, getDbMethod, new Object[]{"sales"});
    Assert.assertEquals(3, getDbCalls.get());
  }
}
