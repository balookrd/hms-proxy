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

  @Test
  public void partitionQuerySigIsInjectiveForCollidingArguments() throws Throwable {
    ProxyConfig config = fullCacheConfig();
    AtomicInteger partCalls = new AtomicInteger();

    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("get_partitions_ps".equals(method.getName())) {
                int call = partCalls.incrementAndGet();
                org.apache.hadoop.hive.metastore.api.Partition p =
                    new org.apache.hadoop.hive.metastore.api.Partition();
                p.setDbName("sales");
                p.setTableName("orders");
                p.setValues(List.of("call_" + call));
                return List.of(p);
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    RoutingMetaStoreProxy handler = proxyFor(config, backend);
    Method getPartsPs = ThriftHiveMetastore.Iface.class.getMethod(
        "get_partitions_ps", String.class, String.class, List.class, short.class);

    @SuppressWarnings("unchecked")
    List<org.apache.hadoop.hive.metastore.api.Partition> r1 =
        (List<org.apache.hadoop.hive.metastore.api.Partition>) handler.invoke(
            null, getPartsPs, new Object[] {"sales", "orders", List.of("a, b"), (short) -1});
    @SuppressWarnings("unchecked")
    List<org.apache.hadoop.hive.metastore.api.Partition> r2 =
        (List<org.apache.hadoop.hive.metastore.api.Partition>) handler.invoke(
            null, getPartsPs, new Object[] {"sales", "orders", List.of("a", "b"), (short) -1});
    @SuppressWarnings("unchecked")
    List<org.apache.hadoop.hive.metastore.api.Partition> r3 =
        (List<org.apache.hadoop.hive.metastore.api.Partition>) handler.invoke(
            null, getPartsPs, new Object[] {"sales", "orders", java.util.Arrays.asList("null"), (short) -1});
    @SuppressWarnings("unchecked")
    List<org.apache.hadoop.hive.metastore.api.Partition> r4 =
        (List<org.apache.hadoop.hive.metastore.api.Partition>) handler.invoke(
            null, getPartsPs, new Object[] {"sales", "orders", java.util.Collections.singletonList(null), (short) -1});

    Assert.assertEquals(4, partCalls.get());
    Assert.assertEquals(List.of("call_1"), r1.get(0).getValues());
    Assert.assertEquals(List.of("call_2"), r2.get(0).getValues());
    Assert.assertEquals(List.of("call_3"), r3.get(0).getValues());
    Assert.assertEquals(List.of("call_4"), r4.get(0).getValues());
  }

  @Test
  public void partitionReadMethodsDoNotInvalidateTableOrPartitionCache() throws Throwable {
    ProxyConfig config = fullCacheConfig();
    AtomicInteger getTableCalls = new AtomicInteger();
    AtomicInteger getPartNamesCalls = new AtomicInteger();

    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              switch (method.getName()) {
                case "get_table" -> {
                  getTableCalls.incrementAndGet();
                  org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
                  t.setDbName("sales");
                  t.setTableName((String) args[1]);
                  return t;
                }
                case "get_partition_names" -> {
                  getPartNamesCalls.incrementAndGet();
                  return List.of("p=1");
                }
                case "get_partitions_by_expr" -> {
                  return new org.apache.hadoop.hive.metastore.api.PartitionsByExprResult(List.of(), false);
                }
                case "get_partition_values" -> {
                  return new org.apache.hadoop.hive.metastore.api.PartitionValuesResponse(List.of());
                }
                default -> throw new UnsupportedOperationException(method.getName());
              }
            })));

    RoutingMetaStoreProxy handler = proxyFor(config, backend);
    Method getTable = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);
    Method getPartNames = ThriftHiveMetastore.Iface.class.getMethod(
        "get_partition_names", String.class, String.class, short.class);
    Method getByExpr = ThriftHiveMetastore.Iface.class.getMethod(
        "get_partitions_by_expr", org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest.class);
    Method getValues = ThriftHiveMetastore.Iface.class.getMethod(
        "get_partition_values", org.apache.hadoop.hive.metastore.api.PartitionValuesRequest.class);

    handler.invoke(null, getTable, new Object[] {"sales", "orders"});
    handler.invoke(null, getPartNames, new Object[] {"sales", "orders", (short) -1});
    Assert.assertEquals(1, getTableCalls.get());
    Assert.assertEquals(1, getPartNamesCalls.get());

    org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest exprReq =
        new org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest();
    exprReq.setDbName("sales");
    exprReq.setTblName("orders");
    exprReq.setExpr(new byte[0]);
    handler.invoke(null, getByExpr, new Object[] {exprReq});

    org.apache.hadoop.hive.metastore.api.PartitionValuesRequest valReq =
        new org.apache.hadoop.hive.metastore.api.PartitionValuesRequest();
    valReq.setDbName("sales");
    valReq.setTblName("orders");
    valReq.setPartitionKeys(List.of());
    handler.invoke(null, getValues, new Object[] {valReq});

    handler.invoke(null, getTable, new Object[] {"sales", "orders"});
    handler.invoke(null, getPartNames, new Object[] {"sales", "orders", (short) -1});
    Assert.assertEquals(1, getTableCalls.get());
    Assert.assertEquals(1, getPartNamesCalls.get());
  }

  @Test
  public void specialCaseGenericAndCrossDbRenameMutationsInvalidateTableAndPartitionCaches() throws Throwable {
    ProxyConfig config = fullCacheConfig();
    Map<String, AtomicInteger> getTableCounts = new java.util.concurrent.ConcurrentHashMap<>();
    AtomicInteger getPartNamesCount = new AtomicInteger();

    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              switch (method.getName()) {
                case "get_table" -> {
                  String key = args[0] + "." + args[1];
                  getTableCounts.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
                  org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
                  t.setDbName((String) args[0]);
                  t.setTableName((String) args[1]);
                  t.setParameters(Map.of());
                  return t;
                }
                case "get_partition_names" -> {
                  getPartNamesCount.incrementAndGet();
                  return List.of("p=1");
                }
                case "add_partitions_req" -> {
                  return new org.apache.hadoop.hive.metastore.api.AddPartitionsResult();
                }
                case "alter_partitions_req", "alter_partitions", "rename_partition", "create_table_req",
                    "create_table", "alter_table" -> {
                  return null;
                }
                case "exchange_partition" -> {
                  return new org.apache.hadoop.hive.metastore.api.Partition();
                }
                case "delete_column_statistics_req", "delete_table_column_statistics",
                    "set_aggr_stats_for", "update_table_column_statistics" -> {
                  return Boolean.TRUE;
                }
                default -> throw new UnsupportedOperationException(method.getName());
              }
            })));

    RoutingMetaStoreProxy handler = proxyFor(config, backend);
    Method getTable = ThriftHiveMetastore.Iface.class.getMethod("get_table", String.class, String.class);
    Method getPartNames = ThriftHiveMetastore.Iface.class.getMethod(
        "get_partition_names", String.class, String.class, short.class);

    // Warm up unrelated table to ensure targeted invalidations stay scoped
    handler.invoke(null, getTable, new Object[] {"sales", "unrelated"});
    Assert.assertEquals(1, getTableCounts.get("sales.unrelated").get());

    List<RunnableThrowing> mutations = new java.util.ArrayList<>();
    mutations.add(() -> {
      org.apache.hadoop.hive.metastore.api.AddPartitionsRequest req =
          new org.apache.hadoop.hive.metastore.api.AddPartitionsRequest("sales", "orders", List.of(), false);
      handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
          "add_partitions_req", org.apache.hadoop.hive.metastore.api.AddPartitionsRequest.class),
          new Object[] {req});
    });
    mutations.add(() -> handler.alter_partitions_req(new TestAlterPartitionsRequest("sales", "orders")));
    mutations.add(() -> {
      org.apache.hadoop.hive.metastore.api.Partition p = new org.apache.hadoop.hive.metastore.api.Partition();
      p.setDbName("sales");
      p.setTableName("orders");
      p.setValues(List.of("2"));
      handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
          "rename_partition", String.class, String.class, List.class, org.apache.hadoop.hive.metastore.api.Partition.class),
          new Object[] {"sales", "orders", List.of("1"), p});
    });
    mutations.add(() -> handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
        "exchange_partition", Map.class, String.class, String.class, String.class, String.class),
        new Object[] {Map.of("p", "1"), "sales", "orders", "sales", "dest_tbl"}));
    mutations.add(() -> {
      org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
      t.setDbName("sales");
      t.setTableName("orders");
      handler.create_table_req(new TestCreateTableRequest(t));
    });
    mutations.add(() -> handler.delete_column_statistics_req(new TestDeleteColumnStatisticsRequest("sales", "orders")));
    mutations.add(() -> handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
        "delete_table_column_statistics", String.class, String.class, String.class),
        new Object[] {"sales", "orders", "col1"}));
    mutations.add(() -> {
      org.apache.hadoop.hive.metastore.api.ColumnStatistics colStats =
          new org.apache.hadoop.hive.metastore.api.ColumnStatistics(
              new org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc(true, "sales", "orders"),
              List.of());
      org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest req =
          new org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest(List.of(colStats));
      handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
          "set_aggr_stats_for", org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest.class),
          new Object[] {req});
    });
    mutations.add(() -> {
      org.apache.hadoop.hive.metastore.api.ColumnStatistics colStats =
          new org.apache.hadoop.hive.metastore.api.ColumnStatistics(
              new org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc(true, "sales", "orders"),
              List.of());
      handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
          "update_table_column_statistics", org.apache.hadoop.hive.metastore.api.ColumnStatistics.class),
          new Object[] {colStats});
    });

    int expectedOrdersLoads = 0;
    int expectedPartLoads = 0;
    for (RunnableThrowing mutation : mutations) {
      handler.invoke(null, getTable, new Object[] {"sales", "orders"});
      expectedOrdersLoads++;
      handler.invoke(null, getPartNames, new Object[] {"sales", "orders", (short) -1});
      expectedPartLoads++;
      Assert.assertEquals(expectedOrdersLoads, getTableCounts.get("sales.orders").get());
      Assert.assertEquals(expectedPartLoads, getPartNamesCount.get());

      mutation.run();
    }
    handler.invoke(null, getTable, new Object[] {"sales", "orders"});
    Assert.assertEquals(expectedOrdersLoads + 1, getTableCounts.get("sales.orders").get());
    handler.invoke(null, getTable, new Object[] {"sales", "unrelated"});
    Assert.assertEquals(1, getTableCounts.get("sales.unrelated").get());

    // Cross-DB alter_table rename with the same table name invalidates both db1.same and db2.same
    handler.invoke(null, getTable, new Object[] {"db1", "same"});
    handler.invoke(null, getTable, new Object[] {"db2", "same"});
    org.apache.hadoop.hive.metastore.api.Table renamed = new org.apache.hadoop.hive.metastore.api.Table();
    renamed.setDbName("db2");
    renamed.setTableName("same");
    renamed.setParameters(Map.of());
    handler.invoke(null, ThriftHiveMetastore.Iface.class.getMethod(
        "alter_table", String.class, String.class, org.apache.hadoop.hive.metastore.api.Table.class),
        new Object[] {"db1", "same", renamed});
    int db1AfterAlter = getTableCounts.get("db1.same").get();
    int db2AfterAlter = getTableCounts.get("db2.same").get();
    handler.invoke(null, getTable, new Object[] {"db1", "same"});
    handler.invoke(null, getTable, new Object[] {"db2", "same"});
    Assert.assertEquals(db1AfterAlter + 1, getTableCounts.get("db1.same").get());
    Assert.assertEquals(db2AfterAlter + 1, getTableCounts.get("db2.same").get());
  }

  public static final class TestAlterPartitionsRequest {
    private String dbName;
    private final String tableName;

    public TestAlterPartitionsRequest(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    public String getDbName() { return dbName; }
    public void setDbName(String dbName) { this.dbName = dbName; }
    public String getTableName() { return tableName; }
    public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions() { return List.of(); }
  }

  public static final class TestCreateTableRequest {
    private final org.apache.hadoop.hive.metastore.api.Table table;

    public TestCreateTableRequest(org.apache.hadoop.hive.metastore.api.Table table) {
      this.table = table;
    }

    public org.apache.hadoop.hive.metastore.api.Table getTable() { return table; }
  }

  public static final class TestDeleteColumnStatisticsRequest {
    private String dbName;
    private final String tblName;

    public TestDeleteColumnStatisticsRequest(String dbName, String tblName) {
      this.dbName = dbName;
      this.tblName = tblName;
    }

    public String getDb_name() { return dbName; }
    public void setDb_name(String dbName) { this.dbName = dbName; }
    public String getTbl_name() { return tblName; }
  }

  @FunctionalInterface
  private interface RunnableThrowing {
    void run() throws Throwable;
  }

  private static ProxyConfig fullCacheConfig() {
    return latencyAwareConfig(
        Map.of("catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 1, 200L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT,
            new DatabaseListCacheConfig(60_000L, 100),
            new DatabaseMetadataCacheConfig(60_000L, 100),
            null,
            false,
            new io.github.mmalykhin.hmsproxy.config.routing.TableMetadataCacheConfig(60_000L, 100, true),
            new io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig(60_000L, 100, true),
            false,
            86_400_000L));
  }

  private static RoutingMetaStoreProxy proxyFor(ProxyConfig config, CatalogBackend backend) {
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);
  }
}
