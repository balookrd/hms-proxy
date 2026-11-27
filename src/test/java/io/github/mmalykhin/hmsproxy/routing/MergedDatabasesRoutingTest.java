package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.MergedDatabaseResolutionConfig;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.security.UserGroupInformation;
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

  @Test
  public void theNegativeCacheIsCappedAgainstAFloodOfUnknownNames() throws Throwable {
    ProbeRouter probe = probeRouter(new MergedDatabaseResolutionConfig(60_000L, 50), (name, calls) -> {
      throw new NoSuchObjectException(name);
    });
    for (int i = 0; i < 500; i++) {
      probe.router.resolveDatabase("ghost_" + i);
    }
    Assert.assertTrue("cache size " + probe.router.negativeResolutionCacheSize(),
        probe.router.negativeResolutionCacheSize() <= 50);
  }

  @Test
  public void expiredMissesMakeRoomForNewOnes() throws Throwable {
    ProbeRouter probe = probeRouter(new MergedDatabaseResolutionConfig(20L, 10), (name, calls) -> {
      throw new NoSuchObjectException(name);
    });
    for (int i = 0; i < 10; i++) {
      probe.router.resolveDatabase("old_" + i);
    }
    Thread.sleep(60L);
    for (int i = 0; i < 10; i++) {
      probe.router.resolveDatabase("new_" + i);
    }
    Assert.assertEquals(10, probe.router.negativeResolutionCacheSize());
    int before = probe.remoteCalls.get();
    probe.router.resolveDatabase("new_3");
    Assert.assertEquals("a remembered miss is answered without a probe", before, probe.remoteCalls.get());
  }

  @Test
  public void concurrentMissesOnOneNameShareOneProbe() throws Throwable {
    CountDownLatch release = new CountDownLatch(1);
    ProbeRouter probe = probeRouter(MergedDatabaseResolutionConfig.defaultConfig(), (name, calls) -> {
      if (!release.await(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("never released");
      }
      throw new NoSuchObjectException(name);
    });
    ExecutorService pool = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> lookups = new ArrayList<>();
      for (int i = 0; i < 8; i++) {
        lookups.add(pool.submit(() -> {
          probe.router.resolveDatabase("ghost");
          return null;
        }));
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (probe.remoteCalls.get() == 0 && System.nanoTime() < deadline) {
        Thread.sleep(5L);
      }
      Thread.sleep(200L);
      release.countDown();
      for (Future<?> lookup : lookups) {
        lookup.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    Assert.assertEquals("eight concurrent misses must reach the remote backend once", 1, probe.remoteCalls.get());
  }

  @Test
  public void aBackendThatFailedToAnswerIsNotRememberedAsMissingTheDatabase() throws Throwable {
    ProbeRouter probe = probeRouter(MergedDatabaseResolutionConfig.defaultConfig(), (name, calls) -> {
      if (calls == 1) {
        throw new MetaException("Could not connect to meta store using any of the URIs provided");
      }
      Database db = new Database();
      db.setName(name);
      return db;
    });
    CatalogRouter.ResolvedNamespace first = probe.router.resolveDatabase("remote_only");
    Assert.assertEquals("the failed lookup falls back to the default catalog", "catalog1", first.catalogName());
    Assert.assertEquals(0, probe.router.negativeResolutionCacheSize());

    CatalogRouter.ResolvedNamespace second = probe.router.resolveDatabase("remote_only");
    Assert.assertEquals("the next lookup must probe again and find the remote database",
        "catalog2", second.catalogName());
  }

  // A name present in two merged catalogs goes to the one declared first in catalogs=. The walk
  // used to follow Map.copyOf order, salted per JVM start, so the same name could route to a
  // different catalog after a restart. Both declaration orders are checked: within one JVM the old
  // salted order is fixed, so one of the two failed on every run.
  @Test
  public void aNameInTwoMergedCatalogsGoesToTheOneDeclaredFirst() throws Throwable {
    Assert.assertEquals("remoteB", resolveSharedName(List.of("remoteB", "remoteA")));
    Assert.assertEquals("remoteA", resolveSharedName(List.of("remoteA", "remoteB")));
  }

  private String resolveSharedName(List<String> mergedOrder) throws Throwable {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("catalog1", new CatalogConfig(
        "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
        Map.of("hive.metastore.uris", "thrift://one")));
    for (String name : mergedOrder) {
      catalogs.put(name, new CatalogConfig(
          name, name, "file:///" + name, false, CatalogAccessMode.READ_WRITE, List.of(),
          CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true, null, null,
          Map.of("hive.metastore.uris", "thrift://" + name)));
    }
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String name : config.catalogs().keySet()) {
      boolean holdsShared = !name.equals("catalog1");
      backends.put(name, newBackend(config, config.catalogs().get(name), new ApacheBackendAdapter(),
          newBackendRuntime(config, config.catalogs().get(name), newSession((proxy, method, args) -> {
            if ("get_database".equals(method.getName()) && holdsShared && "shared".equals(args[0])) {
              Database db = new Database();
              db.setName("shared");
              return db;
            }
            if ("get_database".equals(method.getName())) {
              throw new NoSuchObjectException("No such db in " + name + ": " + args[0]);
            }
            throw new UnsupportedOperationException(method.getName());
          }))));
    }
    return new CatalogRouter(config, backends).resolveDatabase("shared").catalogName();
  }

  @FunctionalInterface
  private interface RemoteGetDatabase {
    Object answer(String dbName, int callNumber) throws Exception;
  }

  private record ProbeRouter(CatalogRouter router, AtomicInteger remoteCalls) {
  }

  /** Default catalog knows only "default"; catalog2 (merged) answers get_database through {@code remote}. */
  private ProbeRouter probeRouter(MergedDatabaseResolutionConfig resolution, RemoteGetDatabase remote)
      throws Exception {
    ProxyConfig base = createConfig(true);
    ProxyConfig config = ProxyConfig.builder()
        .server(base.server())
        .security(base.security())
        .catalogDbSeparator(base.catalogDbSeparator())
        .defaultCatalog(base.defaultCatalog())
        .catalogs(base.catalogs())
        .syntheticReadLockStore(base.syntheticReadLockStore())
        .latencyRouting(new LatencyRoutingConfig(
            null, null, null, null, null, null, null, null, false, null, null, false, 0L, null, resolution))
        .build();
    AtomicInteger remoteCalls = new AtomicInteger();
    CatalogBackend backend1 = newBackend(config, config.catalogs().get("catalog1"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          if ("get_database".equals(method.getName()) && "default".equals(args[0])) {
            Database db = new Database();
            db.setName("default");
            return db;
          }
          if ("get_database".equals(method.getName())) {
            throw new NoSuchObjectException("No such db in cat1: " + args[0]);
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend backend2 = newBackend(config, config.catalogs().get("catalog2"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((proxy, method, args) -> {
          if ("get_database".equals(method.getName())) {
            return remote.answer((String) args[0], remoteCalls.incrementAndGet());
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    return new ProbeRouter(new CatalogRouter(config, backends), remoteCalls);
  }

  @Test
  public void narrowedGetDatabasesPatternDoesNotEraseOtherDefaultCatalogDatabases() throws Throwable {
    ProxyConfig config = createConfig(true);
    AtomicReference<String> cat1GetDbArg = new AtomicReference<>();
    AtomicReference<String> cat2GetDbArg = new AtomicReference<>();

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                return List.of("default", "finance", "local_only");
              }
              if ("get_databases".equals(method.getName())) {
                return List.of("local_only");
              }
              if ("get_database".equals(method.getName())) {
                String db = (String) args[0];
                cat1GetDbArg.set(db);
                if ("default".equals(db) || "finance".equals(db) || "local_only".equals(db)) {
                  Database result = new Database();
                  result.setName(db);
                  result.setDescription("cat1-" + db);
                  return result;
                }
                throw new NoSuchObjectException(db);
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
                return List.of("finance", "remote_only");
              }
              if ("get_databases".equals(method.getName())) {
                return List.of();
              }
              if ("get_database".equals(method.getName())) {
                String db = (String) args[0];
                cat2GetDbArg.set(db);
                if ("finance".equals(db) || "remote_only".equals(db)) {
                  Database result = new Database();
                  result.setName(db);
                  result.setDescription("cat2-" + db);
                  return result;
                }
                throw new NoSuchObjectException(db);
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federationLayer = new FederationLayer(config, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, federationLayer, null, new ProxyObservability(config));

    Method getAllDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    handler.invoke(null, getAllDbsMethod, new Object[0]);

    Method getDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_databases", String.class);
    @SuppressWarnings("unchecked")
    List<String> filtered = (List<String>) handler.invoke(null, getDbsMethod, new Object[] {"local*"});
    Assert.assertEquals(List.of("local_only"), filtered);

    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
    Database financeDb = (Database) handler.invoke(null, getDbMethod, new Object[] {"finance"});
    Assert.assertEquals("cat1-finance", financeDb.getDescription());
    Assert.assertEquals("finance", cat1GetDbArg.get());
    Assert.assertNull("narrowed get_databases must not reroute default-catalog finance to catalog2",
        cat2GetDbArg.get());
  }

  @Test
  public void userScopedGetAllDatabasesDoesNotEraseUnseenDefaultCatalogDatabases() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", new CatalogConfig(
                "catalog1", "c1", "file:///c1", true, CatalogAccessMode.READ_WRITE, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
                Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", new CatalogConfig(
                "catalog2", "c2", "file:///c2", true, CatalogAccessMode.READ_WRITE, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true, null, null,
                Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicReference<String> cat1GetDbArg = new AtomicReference<>();
    AtomicReference<String> cat2GetDbArg = new AtomicReference<>();

    CatalogBackend backend1 = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                return List.of("default");
              }
              if ("get_database".equals(method.getName())) {
                String db = (String) args[0];
                cat1GetDbArg.set(db);
                if ("default".equals(db) || "finance".equals(db)) {
                  Database result = new Database();
                  result.setName(db);
                  result.setDescription("cat1-" + db);
                  return result;
                }
                throw new NoSuchObjectException(db);
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
                return List.of("finance", "remote_only");
              }
              if ("get_database".equals(method.getName())) {
                String db = (String) args[0];
                cat2GetDbArg.set(db);
                if ("finance".equals(db) || "remote_only".equals(db)) {
                  Database result = new Database();
                  result.setName(db);
                  result.setDescription("cat2-" + db);
                  return result;
                }
                throw new NoSuchObjectException(db);
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    router.updateDatabases("catalog1", List.of("default", "finance"));
    router.updateDatabases("catalog2", List.of("finance", "remote_only"));
    FederationLayer federationLayer = new FederationLayer(config, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, federationLayer, null, new ProxyObservability(config));

    UserGroupInformation alice = UserGroupInformation.createUserForTesting("alice", new String[] {"sales"});
    alice.doAs((PrivilegedExceptionAction<Void>) () -> {
      try {
        Method getAllDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
        handler.invoke(null, getAllDbsMethod, new Object[0]);

        Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
        Database financeDb = (Database) handler.invoke(null, getDbMethod, new Object[] {"finance"});
        Assert.assertEquals("cat1-finance", financeDb.getDescription());
        Assert.assertEquals("finance", cat1GetDbArg.get());
        Assert.assertNull("user-scoped get_all_databases must not erase unseen default-catalog finance",
            cat2GetDbArg.get());
        return null;
      } catch (Throwable t) {
        if (t instanceof Exception e) {
          throw e;
        }
        throw new RuntimeException(t);
      }
    });
  }

  @Test
  public void getAllDatabasesPreservesPriorityOrderAcrossMultipleMergedCatalogs() throws Throwable {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("janus", new CatalogConfig(
        "janus", "janus", "file:///janus", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
        Map.of("hive.metastore.uris", "thrift://janus")));
    catalogs.put("hdp", new CatalogConfig(
        "hdp", "hdp", "file:///hdp", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of("^(?!default$).*$"), Map.of(), List.of(), true, null, null,
        Map.of("hive.metastore.uris", "thrift://hdp")));
    catalogs.put("titanium", new CatalogConfig(
        "titanium", "titanium", "file:///titanium", false, CatalogAccessMode.READ_ONLY, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of("^(?!default$).*$"), Map.of(), List.of(), true, null, null,
        Map.of("hive.metastore.uris", "thrift://titanium")));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("janus")
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .latencyRouting(new io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig(
            null, null, null, null,
            io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS))
        .build();

    AtomicReference<String> lastGetDbCatalog = new AtomicReference<>();
    AtomicReference<String> lastGetTableCatalog = new AtomicReference<>();

    CatalogBackend janus = newBackend(config, config.catalogs().get("janus"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("janus"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName()) || "get_databases".equals(method.getName())) {
            return List.of("default", "janus_only");
          }
          if ("get_database".equals(method.getName())) {
            String db = (String) args[0];
            if ("default".equals(db) || "janus_only".equals(db)) {
              lastGetDbCatalog.set("janus");
              Database res = new Database();
              res.setName(db);
              res.setDescription("janus-" + db);
              return res;
            }
            throw new NoSuchObjectException(db);
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    CatalogBackend hdp = newBackend(config, config.catalogs().get("hdp"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName()) || "get_databases".equals(method.getName())) {
            return List.of("default", "Beemetrics", "hdp_only");
          }
          if ("get_database".equals(method.getName())) {
            String db = (String) args[0];
            if ("default".equals(db) || "Beemetrics".equalsIgnoreCase(db) || "hdp_only".equals(db)) {
              lastGetDbCatalog.set("hdp");
              Database res = new Database();
              res.setName(db);
              res.setDescription("hdp-" + db);
              return res;
            }
            throw new NoSuchObjectException(db);
          }
          if ("get_table".equals(method.getName())) {
            lastGetTableCatalog.set("hdp");
            Table tbl = new Table();
            tbl.setDbName((String) args[0]);
            tbl.setTableName((String) args[1]);
            return tbl;
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    CatalogBackend titanium = newBackend(config, config.catalogs().get("titanium"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("titanium"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName()) || "get_databases".equals(method.getName())) {
            return List.of("default", "Beemetrics", "titanium_only");
          }
          if ("get_database".equals(method.getName())) {
            String db = (String) args[0];
            if ("default".equals(db) || "Beemetrics".equalsIgnoreCase(db) || "titanium_only".equals(db)) {
              lastGetDbCatalog.set("titanium");
              Database res = new Database();
              res.setName(db);
              res.setDescription("titanium-" + db);
              return res;
            }
            throw new NoSuchObjectException(db);
          }
          if ("get_table".equals(method.getName())) {
            lastGetTableCatalog.set("titanium");
            Table tbl = new Table();
            tbl.setDbName((String) args[0]);
            tbl.setTableName((String) args[1]);
            return tbl;
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("janus", janus);
    backends.put("hdp", hdp);
    backends.put("titanium", titanium);

    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federationLayer = new FederationLayer(config, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, federationLayer, null, new ProxyObservability(config));

    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
    Method getTblMethod = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);
    Method getAllDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    Method getDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_databases", String.class);

    // 1. Before SHOW DATABASES: on-demand lookup resolves Beemetrics -> hdp
    Database dbBefore = (Database) handler.invoke(null, getDbMethod, new Object[] {"Beemetrics"});
    Assert.assertEquals("hdp-Beemetrics", dbBefore.getDescription());
    Assert.assertEquals("hdp", lastGetDbCatalog.get());

    // 2. SHOW DATABASES (get_all_databases): hdp must keep Beemetrics unprefixed, titanium gets prefix
    @SuppressWarnings("unchecked")
    List<String> allDbs = (List<String>) handler.invoke(null, getAllDbsMethod, new Object[0]);
    Assert.assertTrue("Beemetrics from hdp must be unprefixed: " + allDbs, allDbs.contains("Beemetrics"));
    Assert.assertTrue("Beemetrics from titanium must be prefixed: " + allDbs,
        allDbs.contains("titanium__Beemetrics"));
    Assert.assertFalse("Beemetrics from hdp must NOT be prefixed: " + allDbs,
        allDbs.contains("hdp__Beemetrics"));

    // 3. After SHOW DATABASES: Beemetrics MUST still route to hdp, NOT titanium!
    lastGetDbCatalog.set(null);
    lastGetTableCatalog.set(null);
    Database dbAfterAll = (Database) handler.invoke(null, getDbMethod, new Object[] {"Beemetrics"});
    Assert.assertEquals("hdp-Beemetrics", dbAfterAll.getDescription());
    Assert.assertEquals("hdp", lastGetDbCatalog.get());
    handler.invoke(null, getTblMethod, new Object[] {"Beemetrics", "events"});
    Assert.assertEquals("hdp", lastGetTableCatalog.get());

    // 4. Explicit get_databases("*"): still preserves priority order
    handler.invoke(null, getDbsMethod, new Object[] {"*"});
    lastGetTableCatalog.set(null);
    handler.invoke(null, getTblMethod, new Object[] {"Beemetrics", "events"});
    Assert.assertEquals("hdp", lastGetTableCatalog.get());

    // 5. Explicit titanium__Beemetrics still routes to titanium
    lastGetDbCatalog.set(null);
    Database dbTitanium = (Database) handler.invoke(null, getDbMethod, new Object[] {"titanium__Beemetrics"});
    Assert.assertEquals("titanium-Beemetrics", dbTitanium.getDescription());
    Assert.assertEquals("titanium", lastGetDbCatalog.get());
  }

  @Test
  public void onDemandProbeDoesNotCacheLowerPriorityCatalogWhenHigherPriorityFailed() throws Throwable {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("janus", new CatalogConfig(
        "janus", "janus", "file:///janus", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
        Map.of("hive.metastore.uris", "thrift://janus")));
    catalogs.put("hdp", new CatalogConfig(
        "hdp", "hdp", "file:///hdp", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true, null, null,
        Map.of("hive.metastore.uris", "thrift://hdp")));
    catalogs.put("titanium", new CatalogConfig(
        "titanium", "titanium", "file:///titanium", false, CatalogAccessMode.READ_ONLY, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true, null, null,
        Map.of("hive.metastore.uris", "thrift://titanium")));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("janus")
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .latencyRouting(new io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig(
            null, null, null, null,
            io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS))
        .build();

    AtomicInteger hdpCalls = new AtomicInteger();
    CatalogBackend janus = newBackend(config, config.catalogs().get("janus"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("janus"), newSession((proxy, method, args) -> {
          if ("get_database".equals(method.getName())) {
            throw new NoSuchObjectException((String) args[0]);
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend hdp = newBackend(config, config.catalogs().get("hdp"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          if ("get_database".equals(method.getName())) {
            if (hdpCalls.incrementAndGet() == 1) {
              throw new MetaException("Could not connect to meta store using any of the URIs provided");
            }
            Database db = new Database();
            db.setName((String) args[0]);
            return db;
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend titanium = newBackend(config, config.catalogs().get("titanium"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("titanium"), newSession((proxy, method, args) -> {
          if ("get_database".equals(method.getName())) {
            Database db = new Database();
            db.setName((String) args[0]);
            return db;
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("janus", janus);
    backends.put("hdp", hdp);
    backends.put("titanium", titanium);

    CatalogRouter router = new CatalogRouter(config, backends);
    // First call: hdp fails with transient MetaException -> must NOT permanently cache titanium
    router.resolveDatabase("Beemetrics");
    // Second call: hdp is healthy again -> must resolve to hdp!
    CatalogRouter.ResolvedNamespace resolved = router.resolveDatabase("Beemetrics");
    Assert.assertEquals("hdp", resolved.catalogName());
  }

  @Test
  public void degradedFanoutDoesNotHijackDatabaseToLowerPriorityCatalog() throws Throwable {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("janus", new CatalogConfig(
        "janus", "janus", "file:///janus", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), false, null, null,
        Map.of("hive.metastore.uris", "thrift://janus")));
    catalogs.put("hdp", new CatalogConfig(
        "hdp", "hdp", "file:///hdp", false, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true, null, null,
        Map.of("hive.metastore.uris", "thrift://hdp")));
    catalogs.put("titanium", new CatalogConfig(
        "titanium", "titanium", "file:///titanium", false, CatalogAccessMode.READ_ONLY, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true, null, null,
        Map.of("hive.metastore.uris", "thrift://titanium")));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("janus")
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .latencyRouting(new io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig(
            null, null, null, null,
            io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS))
        .build();

    AtomicInteger hdpFanoutCalls = new AtomicInteger();
    CatalogBackend janus = newBackend(config, config.catalogs().get("janus"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("janus"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            return List.of("default", "janus_only");
          }
          if ("get_database".equals(method.getName())) {
            throw new NoSuchObjectException((String) args[0]);
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend hdp = newBackend(config, config.catalogs().get("hdp"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            if (hdpFanoutCalls.incrementAndGet() == 1) {
              throw new MetaException("hdp temporarily unavailable");
            }
            return List.of("default", "Beemetrics");
          }
          if ("get_database".equals(method.getName())) {
            Database db = new Database();
            db.setName((String) args[0]);
            db.setDescription("hdp-" + args[0]);
            return db;
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend titanium = newBackend(config, config.catalogs().get("titanium"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("titanium"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            return List.of("default", "Beemetrics", "titanium_only");
          }
          if ("get_database".equals(method.getName())) {
            Database db = new Database();
            db.setName((String) args[0]);
            db.setDescription("titanium-" + args[0]);
            return db;
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("janus", janus);
    backends.put("hdp", hdp);
    backends.put("titanium", titanium);

    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federationLayer = new FederationLayer(config, router);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, federationLayer, null, new ProxyObservability(config));

    Method getAllDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    // 1. Degraded fanout: hdp fails during get_all_databases, titanium succeeds
    handler.invoke(null, getAllDbsMethod, new Object[0]);

    // 2. Subsequent get_database("Beemetrics") when hdp is healthy MUST resolve to hdp, not titanium
    Database db = (Database) handler.invoke(null, getDbMethod, new Object[] {"Beemetrics"});
    Assert.assertEquals("hdp-Beemetrics", db.getDescription());
  }

  @Test
  public void backgroundDatabaseListRefreshUpdatesMergedCatalogRoutingTable() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    io.github.mmalykhin.hmsproxy.config.routing.DatabaseCacheBackgroundRefreshConfig refreshCfg =
        new io.github.mmalykhin.hmsproxy.config.routing.DatabaseCacheBackgroundRefreshConfig(true, 10_000L, 3_600_000L);
    ProxyConfig base = createConfig(true);
    ProxyConfig config = ProxyConfig.builder()
        .server(base.server())
        .security(base.security())
        .catalogDbSeparator(base.catalogDbSeparator())
        .defaultCatalog(base.defaultCatalog())
        .catalogs(base.catalogs())
        .syntheticReadLockStore(base.syntheticReadLockStore())
        .latencyRouting(new LatencyRoutingConfig(
            null, null, null, null,
            io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS,
            new io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig(60_000L, 100, true, refreshCfg),
            new io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig(60_000L, 100, true, refreshCfg),
            null, false, null, null, false, 0L,
            io.github.mmalykhin.hmsproxy.config.routing.DistributedCacheInvalidationConfig.disabled(),
            MergedDatabaseResolutionConfig.defaultConfig()))
        .build();

    AtomicReference<List<String>> cat2Dbs = new AtomicReference<>(List.of("remote_v1"));
    AtomicInteger cat2GetDbCalls = new AtomicInteger();
    CatalogBackend backend1 = newBackend(config, config.catalogs().get("catalog1"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            return List.of("default");
          }
          if ("get_database".equals(method.getName())) {
            throw new NoSuchObjectException((String) args[0]);
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend backend2 = newBackend(config, config.catalogs().get("catalog2"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            return cat2Dbs.get();
          }
          if ("get_database".equals(method.getName())) {
            cat2GetDbCalls.incrementAndGet();
            Database db = new Database();
            db.setName((String) args[0]);
            return db;
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federation = new FederationLayer(config, router);
    ProxyObservability observability = new ProxyObservability(config);
    DatabaseListCache listCache = new DatabaseListCache(
        config.latencyRouting().databaseListCache(), false, 0L, observability.metrics(), clock::get);
    DatabaseMetadataCache metaCache = new DatabaseMetadataCache(
        config.latencyRouting().databaseMetadataCache(), listCache, false, 0L, observability.metrics(), clock::get);
    DatabaseCacheRefresher refresher = new DatabaseCacheRefresher(
        listCache, metaCache, refreshCfg, refreshCfg, clock::get, false);
    io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer compat =
        new io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer(config, null);
    BackendRoutingController brc = new BackendRoutingController(config, router, observability);
    AdmissionGate gate = new AdmissionGate(brc, new RequestRateLimiter(config, observability.metrics()));
    FanoutExecutor fanout = new FanoutExecutor(brc, router, gate);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(compat, gate, observability, fanout, router);
    RoutingHandler handler = new RoutingHandler(
        config, router, federation, compat, observability, dispatcher, new ImpersonationResolver(config),
        listCache, metaCache, new TableMetadataCache(config.latencyRouting().tableMetadataCache()),
        new PartitionMetadataCache(config.latencyRouting().partitionMetadataCache()),
        new FileSystemExternalTableDropPurger(config),
        io.github.mmalykhin.hmsproxy.security.ranger.NoOpMetadataAuthorizer.INSTANCE,
        new LocalCacheInvalidator(listCache, metaCache, null, null, router));

    Method getAllDbs = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    handler.invoke(null, getAllDbs, new Object[0]);

    // New database remote_v2 appears in catalog2 and is picked up by background refresh
    cat2Dbs.set(List.of("remote_v1", "remote_v2"));
    clock.addAndGet(15_000L);
    refresher.refreshTick();

    // Resolving remote_v2 must immediately route to catalog2 without an on-demand get_database probe!
    CatalogRouter.ResolvedNamespace ns = router.resolveDatabase("remote_v2");
    Assert.assertEquals("catalog2", ns.catalogName());
    Assert.assertEquals("Background refresh should have registered remote_v2 without on-demand probe",
        0, cat2GetDbCalls.get());
    brc.close();
    handler.close();
  }
}
