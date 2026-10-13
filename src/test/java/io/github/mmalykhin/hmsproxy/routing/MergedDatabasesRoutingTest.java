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

public class MergedDatabasesRoutingTest {

  private ProxyConfig createConfig(boolean mergeWithDefault) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", new CatalogConfig(
                "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
                Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", new CatalogConfig(
                "catalog2", "c2", "file:///c2", false, CatalogAccessMode.READ_WRITE, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), mergeWithDefault, null, null,
                Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  @Test
  public void testGetAllDatabasesWithMergeNoShadowingAndPrefixedOnConflict() throws Throwable {
    ProxyConfig config = createConfig(true);

    AtomicInteger cat1GetAllDbs = new AtomicInteger();
    AtomicInteger cat2GetAllDbs = new AtomicInteger();
    AtomicReference<String> cat1GetDbArg = new AtomicReference<>();
    AtomicReference<String> cat2GetDbArg = new AtomicReference<>();
    AtomicReference<String> cat2GetTableDbArg = new AtomicReference<>();
    AtomicReference<String> cat1GetTableDbArg = new AtomicReference<>();

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
                return List.of("default", "finance", "local_only");
              }
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat1GetDbArg.set(dbName);
                if ("default".equals(dbName) || "finance".equals(dbName) || "local_only".equals(dbName)) {
                  Database db = new Database();
                  db.setName(dbName);
                  db.setDescription("cat1-" + dbName);
                  return db;
                }
                throw new NoSuchObjectException("No such db in cat1: " + dbName);
              }
              if ("get_table".equals(method.getName())) {
                String dbName = (String) args[0];
                String tblName = (String) args[1];
                cat1GetTableDbArg.set(dbName);
                Table tbl = new Table();
                tbl.setDbName(dbName);
                tbl.setTableName(tblName);
                return tbl;
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
                return List.of("finance", "remote_only");
              }
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat2GetDbArg.set(dbName);
                if ("finance".equals(dbName) || "remote_only".equals(dbName)) {
                  Database db = new Database();
                  db.setName(dbName);
                  db.setDescription("cat2-" + dbName);
                  return db;
                }
                throw new NoSuchObjectException("No such db in cat2: " + dbName);
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

    // Expectations:
    // 1. default catalog schemas visible without prefix: default, finance, local_only
    // 2. unique remote catalog schema visible without prefix: remote_only
    // 3. conflicting remote schema finance NOT shadowed, but remains with prefix: catalog2__finance!
    Assert.assertTrue("Should contain default", dbs.contains("default"));
    Assert.assertTrue("Should contain finance", dbs.contains("finance"));
    Assert.assertTrue("Should contain local_only", dbs.contains("local_only"));
    Assert.assertTrue("Should contain remote_only", dbs.contains("remote_only"));
    Assert.assertTrue("Should contain catalog2__finance", dbs.contains("catalog2__finance"));
    Assert.assertEquals(1, dbs.stream().filter("finance"::equals).count());
    Assert.assertEquals(1, dbs.stream().filter("catalog2__finance"::equals).count());
    Assert.assertEquals(5, dbs.size());

    // Calling get_database("finance") MUST route to catalog1 (no shadowing)!
    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
    Database dbFinance = (Database) handler.invoke(null, getDbMethod, new Object[] {"finance"});
    Assert.assertEquals("finance", dbFinance.getName());
    Assert.assertEquals("cat1-finance", dbFinance.getDescription());
    Assert.assertEquals("finance", cat1GetDbArg.get());

    // Calling get_database("catalog2__finance") with prefix MUST route to catalog2!
    cat1GetDbArg.set(null);
    cat2GetDbArg.set(null);
    Database dbRemoteFinance = (Database) handler.invoke(null, getDbMethod, new Object[] {"catalog2__finance"});
    Assert.assertEquals("cat2-finance", dbRemoteFinance.getDescription());
    Assert.assertEquals("finance", cat2GetDbArg.get());
    Assert.assertNull(cat1GetDbArg.get());

    // Calling get_database("remote_only") without prefix MUST route to catalog2!
    cat1GetDbArg.set(null);
    cat2GetDbArg.set(null);
    Database dbRemoteOnly = (Database) handler.invoke(null, getDbMethod, new Object[] {"remote_only"});
    Assert.assertEquals("remote_only", dbRemoteOnly.getName());
    Assert.assertEquals("cat2-remote_only", dbRemoteOnly.getDescription());
    Assert.assertEquals("remote_only", cat2GetDbArg.get());
    Assert.assertNull(cat1GetDbArg.get());

    // Calling get_table("remote_only", "tbl1") MUST route to catalog2!
    Method getTblMethod = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);
    Table tableRemote = (Table) handler.invoke(null, getTblMethod, new Object[] {"remote_only", "tbl1"});
    Assert.assertEquals("tbl1", tableRemote.getTableName());
    Assert.assertEquals("remote_only", cat2GetTableDbArg.get());

    // Calling get_table("finance", "tbl1") MUST route to catalog1!
    Table tableLocal = (Table) handler.invoke(null, getTblMethod, new Object[] {"finance", "tbl1"});
    Assert.assertEquals("tbl1", tableLocal.getTableName());
    Assert.assertEquals("finance", cat1GetTableDbArg.get());
  }

  @Test
  public void testCreateDatabaseRoutesToDefaultCatalogWhenUnprefixed() throws Throwable {
    ProxyConfig config = createConfig(true);

    AtomicReference<String> cat1CreatedDb = new AtomicReference<>();
    AtomicReference<String> cat2CreatedDb = new AtomicReference<>();

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("create_database".equals(method.getName())) {
                Database db = (Database) args[0];
                cat1CreatedDb.set(db.getName());
                return null;
              }
              if ("get_database".equals(method.getName())) {
                Database db = new Database();
                db.setName((String) args[0]);
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
              if ("create_database".equals(method.getName())) {
                Database db = (Database) args[0];
                cat2CreatedDb.set(db.getName());
                return null;
              }
              if ("get_database".equals(method.getName())) {
                Database db = new Database();
                db.setName((String) args[0]);
                return db;
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

    Method createDbMethod = ThriftHiveMetastore.Iface.class.getMethod("create_database", Database.class);

    // 1. Creating unprefixed database: MUST go to catalog1 (default catalog)
    Database newDb = new Database();
    newDb.setName("new_schema");
    handler.invoke(null, createDbMethod, new Object[] {newDb});

    Assert.assertEquals("new_schema", cat1CreatedDb.get());
    Assert.assertNull(cat2CreatedDb.get());

    // 2. Creating explicitly prefixed database: MUST go to catalog2
    cat1CreatedDb.set(null);
    cat2CreatedDb.set(null);
    Database remoteDb = new Database();
    remoteDb.setName("catalog2__remote_schema");
    handler.invoke(null, createDbMethod, new Object[] {remoteDb});

    Assert.assertEquals("remote_schema", cat2CreatedDb.get());
    Assert.assertNull(cat1CreatedDb.get());
  }

  @Test
  public void testGetDatabasesPatternWithMerge() throws Throwable {
    ProxyConfig config = createConfig(true);

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_databases".equals(method.getName())) {
                return List.of("default", "finance");
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
                return List.of("finance", "findata");
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

    // Pattern *fin* matches finance, catalog2__finance and findata:
    @SuppressWarnings("unchecked")
    List<String> dbsStar = (List<String>) handler.invoke(null, getDbsMethod, new Object[] {"*fin*"});
    Assert.assertTrue("Should contain finance for *fin*", dbsStar.contains("finance"));
    Assert.assertTrue("Should contain catalog2__finance for *fin*", dbsStar.contains("catalog2__finance"));
    Assert.assertTrue("Should contain findata for *fin*", dbsStar.contains("findata"));

    // Pattern fin* matches finance and findata (without prefix), but not catalog2__finance (which starts with prefix):
    @SuppressWarnings("unchecked")
    List<String> dbsPrefix = (List<String>) handler.invoke(null, getDbsMethod, new Object[] {"fin*"});
    Assert.assertTrue("Should contain finance for fin*", dbsPrefix.contains("finance"));
    Assert.assertTrue("Should contain findata for fin*", dbsPrefix.contains("findata"));
    Assert.assertFalse("Should NOT contain catalog2__finance for fin*", dbsPrefix.contains("catalog2__finance"));

    // Pattern catalog2__fin* matches catalog2__finance:
    @SuppressWarnings("unchecked")
    List<String> dbsCat = (List<String>) handler.invoke(null, getDbsMethod, new Object[] {"catalog2__fin*"});
    Assert.assertTrue("Should contain catalog2__finance for catalog2__fin*", dbsCat.contains("catalog2__finance"));
  }

  @Test
  public void testReconfigureEnablesMergeOnTheFly() throws Throwable {
    ProxyConfig initialConfig = createConfig(false);

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

    // Initially mergeWithDefault is false, so remote_only routes to catalog1
    Database db1 = (Database) handler.invoke(null, getDbMethod, new Object[] {"remote_only"});
    Assert.assertEquals("cat1", db1.getDescription());
    Assert.assertEquals("remote_only", cat1GetDb.get());

    // Reconfigure with mergeWithDefault=true
    ProxyConfig updatedConfig = createConfig(true);
    router.reconfigure(updatedConfig);
    federationLayer.reconfigure(updatedConfig);
    handler.reconfigure(updatedConfig);

    cat1GetDb.set(null);
    cat2GetDb.set(null);

    // Tell router about databases: catalog1 has [default], catalog2 has [remote_only]
    router.updateDatabases("catalog1", List.of("default"));
    router.updateDatabases("catalog2", List.of("remote_only"));

    // Now remote_only routes to catalog2!
    Database db2 = (Database) handler.invoke(null, getDbMethod, new Object[] {"remote_only"});
    Assert.assertEquals("cat2", db2.getDescription());
    Assert.assertEquals("remote_only", cat2GetDb.get());
    Assert.assertNull(cat1GetDb.get());
  }

  @Test
  public void testDirectDatabaseLookupWithoutPriorGetAllDatabases() throws Throwable {
    ProxyConfig config = createConfig(true);

    AtomicReference<String> cat1GetDb = new AtomicReference<>();
    AtomicReference<String> cat2GetDb = new AtomicReference<>();

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat1GetDb.set(dbName);
                if ("default".equals(dbName) || "local_only".equals(dbName)) {
                  Database db = new Database();
                  db.setName(dbName);
                  db.setDescription("cat1-" + dbName);
                  return db;
                }
                throw new NoSuchObjectException("No such db in cat1: " + dbName);
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
              if ("get_database".equals(method.getName())) {
                String dbName = (String) args[0];
                cat2GetDb.set(dbName);
                if ("remote_only".equals(dbName)) {
                  Database db = new Database();
                  db.setName(dbName);
                  db.setDescription("cat2-" + dbName);
                  return db;
                }
                throw new NoSuchObjectException("No such db in cat2: " + dbName);
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

    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    // Direct call to remote_only without get_all_databases:
    // On-demand lookup should check catalog1 (not found), then catalog2 (found!)
    Database db = (Database) handler.invoke(null, getDbMethod, new Object[] {"remote_only"});
    Assert.assertEquals("cat2-remote_only", db.getDescription());
    Assert.assertEquals("remote_only", cat2GetDb.get());
    Assert.assertEquals("remote_only", cat1GetDb.get());
  }
}
