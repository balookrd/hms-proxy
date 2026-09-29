package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newBackend;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newBackendRuntime;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.newSession;

public class UnprefixedDatabasesRoutingTest {

  private ProxyConfig createConfig(List<String> unprefixed) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", new CatalogConfig(
                "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), null, null,
                Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", new CatalogConfig(
                "catalog2", "c2", "file:///c2", false, CatalogAccessMode.READ_WRITE, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), unprefixed, null, null,
                Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  @Test
  public void unprefixedDatabaseShadowsDefaultCatalogDatabaseAndRoutesCorrectly() throws Throwable {
    ProxyConfig config = createConfig(List.of("beemetrics"));

    AtomicInteger cat1GetAllDbs = new AtomicInteger();
    AtomicInteger cat2GetAllDbs = new AtomicInteger();
    AtomicReference<String> cat2GetDbArg = new AtomicReference<>();
    AtomicReference<String> cat1GetDbArg = new AtomicReference<>();
    AtomicReference<String> cat2GetTableDbArg = new AtomicReference<>();

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                cat1GetAllDbs.incrementAndGet();
                return List.of("default", "beemetrics", "local_only");
              }
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat1GetDbArg.set(dbName);
                Database db = new Database();
                db.setName(dbName);
                db.setDescription("cat1-" + dbName);
                return db;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                cat2GetAllDbs.incrementAndGet();
                return List.of("beemetrics", "remote_only");
              }
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat2GetDbArg.set(dbName);
                Database db = new Database();
                db.setName(dbName);
                db.setDescription("cat2-" + dbName);
                return db;
              }
              if ("get_table".equals(method.getName())) {
                String dbName = (String) args[0];
                String tblName = (String) args[1];
                cat2GetTableDbArg.set(dbName);
                Table tbl = new Table();
                tbl.setDbName(dbName);
                tbl.setTableName(tblName);
                return tbl;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);

    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federationLayer = new FederationLayer(config, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, federationLayer, null, observability);

    Method getAllDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    @SuppressWarnings("unchecked")
    List<String> dbs = (List<String>) handler.invoke(null, getAllDbsMethod, new Object[0]);

    // beemetrics from catalog1 must be shadowed and not appear twice!
    // remote_only should have catalog2__ prefix.
    // beemetrics should be unprefixed!
    Assert.assertTrue("Should contain default", dbs.contains("default"));
    Assert.assertTrue("Should contain local_only", dbs.contains("local_only"));
    Assert.assertTrue("Should contain beemetrics", dbs.contains("beemetrics"));
    Assert.assertTrue("Should contain catalog2__remote_only", dbs.contains("catalog2__remote_only"));
    Assert.assertEquals(1, dbs.stream().filter("beemetrics"::equals).count());
    Assert.assertEquals(4, dbs.size());

    // Calling get_database("beemetrics") must route to catalog2!
    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
    Database db = (Database) handler.invoke(null, getDbMethod, new Object[] {"beemetrics"});
    Assert.assertEquals("beemetrics", db.getName());
    Assert.assertEquals("cat2-beemetrics", db.getDescription());
    Assert.assertEquals("beemetrics", cat2GetDbArg.get());
    Assert.assertNull(cat1GetDbArg.get());

    // Calling get_database("local_only") must route to catalog1!
    Database localDb = (Database) handler.invoke(null, getDbMethod, new Object[] {"local_only"});
    Assert.assertEquals("local_only", localDb.getName());
    Assert.assertEquals("cat1-local_only", localDb.getDescription());
    Assert.assertEquals("local_only", cat1GetDbArg.get());

    // Calling get_table("beemetrics", "tbl1") must route to catalog2!
    Method getTblMethod = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);
    Table table = (Table) handler.invoke(null, getTblMethod, new Object[] {"beemetrics", "tbl1"});
    Assert.assertEquals("tbl1", table.getTableName());
    Assert.assertEquals("beemetrics", cat2GetTableDbArg.get());
  }

  @Test
  public void getDatabasesPatternWithUnprefixedDatabase() throws Throwable {
    ProxyConfig config = createConfig(List.of("beemetrics"));

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_databases".equals(method.getName())) {
                return List.of("default");
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    CatalogBackend backend2 = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_databases".equals(method.getName())) {
                return List.of("beemetrics");
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);

    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federationLayer = new FederationLayer(config, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, federationLayer, null, observability);

    Method getDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_databases", String.class);
    @SuppressWarnings("unchecked")
    List<String> dbs = (List<String>) handler.invoke(null, getDbsMethod, new Object[] {"beemetrics"});

    Assert.assertEquals(List.of("beemetrics"), dbs);
  }

  @Test
  public void reconfigureUpdatesRoutingAndClearsCachesOnTheFly() throws Throwable {
    ProxyConfig initialConfig = createConfig(List.of());

    AtomicReference<String> cat1GetDb = new AtomicReference<>();
    AtomicReference<String> cat2GetDb = new AtomicReference<>();

    CatalogBackend backend1 = newBackend(
        initialConfig,
        initialConfig.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            initialConfig,
            initialConfig.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat1GetDb.set(dbName);
                Database db = new Database();
                db.setName(dbName);
                db.setDescription("cat1");
                return db;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    CatalogBackend backend2 = newBackend(
        initialConfig,
        initialConfig.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            initialConfig,
            initialConfig.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat2GetDb.set(dbName);
                Database db = new Database();
                db.setName(dbName);
                db.setDescription("cat2");
                return db;
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);

    ProxyObservability observability = new ProxyObservability(initialConfig);
    CatalogRouter router = new CatalogRouter(initialConfig, backends);
    FederationLayer federationLayer = new FederationLayer(initialConfig, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(initialConfig, router, federationLayer, null, observability);

    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    // Initially beemetrics is not unprefixed, so it routes to catalog1
    Database db1 = (Database) handler.invoke(null, getDbMethod, new Object[] {"beemetrics"});
    Assert.assertEquals("cat1", db1.getDescription());
    Assert.assertEquals("beemetrics", cat1GetDb.get());

    // Reconfigure with beemetrics unprefixed
    ProxyConfig updatedConfig = createConfig(List.of("beemetrics"));
    router.reconfigure(updatedConfig);
    federationLayer.reconfigure(updatedConfig);
    handler.reconfigure(updatedConfig);

    cat1GetDb.set(null);
    cat2GetDb.set(null);

    // Now beemetrics routes to catalog2!
    Database db2 = (Database) handler.invoke(null, getDbMethod, new Object[] {"beemetrics"});
    Assert.assertEquals("cat2", db2.getDescription());
    Assert.assertEquals("beemetrics", cat2GetDb.get());
    Assert.assertNull(cat1GetDb.get());
  }
}
