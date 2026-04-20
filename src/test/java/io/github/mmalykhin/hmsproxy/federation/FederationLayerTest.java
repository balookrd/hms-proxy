package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;

public class FederationLayerTest {
  @Test
  public void internalizeArgumentRewritesViewTextForLocalAndRemoteCatalogs() throws Exception {
    FederationLayer layer = federationLayer(viewRewriteConfig(false));
    CatalogRouter.ResolvedNamespace namespace =
        new CatalogRouter.ResolvedNamespace(null, "catalog1", "catalog1__sales", "sales");
    Table table = new Table();
    table.setTableType("VIRTUAL_VIEW");
    table.setDbName("catalog1__sales");
    table.setTableName("v_orders");
    table.setViewOriginalText(
        "select * from `catalog1__sales`.`orders` o join catalog2__dim.customers c on o.id = c.id");
    table.setViewExpandedText(
        "select * from catalog1__sales.orders o join `catalog2__dim`.customers c on o.id = c.id");

    Table routed = (Table) layer.internalizeArgument(table, namespace);

    Assert.assertEquals(
        "select * from `sales`.`orders` o join dim.customers c on o.id = c.id",
        routed.getViewOriginalText());
    Assert.assertEquals(
        "select * from sales.orders o join `dim`.customers c on o.id = c.id",
        routed.getViewExpandedText());
  }

  @Test
  public void internalizeArgumentCanPreserveOriginalViewText() throws Exception {
    FederationLayer layer = federationLayer(viewRewriteConfig(true));
    CatalogRouter.ResolvedNamespace namespace =
        new CatalogRouter.ResolvedNamespace(null, "catalog1", "catalog1__sales", "sales");
    Table table = new Table();
    table.setTableType("VIRTUAL_VIEW");
    table.setDbName("catalog1__sales");
    table.setTableName("v_orders");
    table.setViewOriginalText("select * from catalog1__sales.orders");
    table.setViewExpandedText("select * from catalog1__sales.orders");

    Table routed = (Table) layer.internalizeArgument(table, namespace);

    Assert.assertEquals("select * from catalog1__sales.orders", routed.getViewOriginalText());
    Assert.assertEquals("select * from sales.orders", routed.getViewExpandedText());
  }

  @Test
  public void externalizeResultRewritesMaterializedViewTextForClientNamespace() throws Exception {
    FederationLayer layer = federationLayer(viewRewriteConfig(false));
    CatalogRouter.ResolvedNamespace namespace =
        new CatalogRouter.ResolvedNamespace(null, "catalog2", "catalog2__sales", "sales");
    Table table = new Table();
    table.setTableType("MATERIALIZED_VIEW");
    table.setDbName("sales");
    table.setTableName("mv_orders");
    table.setViewOriginalText("select * from sales.orders");
    table.setViewExpandedText("select * from hive.sales.orders");

    Table routed = (Table) layer.externalizeResult(table, namespace);

    Assert.assertEquals("select * from catalog2__sales.orders", routed.getViewOriginalText());
    Assert.assertEquals("select * from catalog2__sales.orders", routed.getViewExpandedText());
  }

  @Test
  public void internalizeArgumentHandlesUnionBackedStatsRequests() throws Exception {
    FederationLayer layer = federationLayer(viewRewriteConfig(false));
    CatalogRouter.ResolvedNamespace namespace =
        new CatalogRouter.ResolvedNamespace(null, "catalog1", "catalog1__sales", "sales");

    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, "catalog1__sales", "events");
    statsDesc.setCatName("hive");

    LongColumnStatsData longStats = new LongColumnStatsData();
    longStats.setLowValue(2L);
    longStats.setHighValue(2L);
    longStats.setNumNulls(0L);
    longStats.setNumDVs(1L);

    ColumnStatisticsData statsData = new ColumnStatisticsData();
    statsData.setLongStats(longStats);

    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColName("id");
    statsObj.setColType("int");
    statsObj.setStatsData(statsData);

    ColumnStatistics statistics = new ColumnStatistics();
    statistics.setStatsDesc(statsDesc);
    statistics.setStatsObj(List.of(statsObj));

    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
    request.setColStats(List.of(statistics));
    request.setNeedMerge(true);

    SetPartitionsStatsRequest routed = (SetPartitionsStatsRequest) layer.internalizeArgument(request, namespace);

    Assert.assertEquals("sales", routed.getColStats().get(0).getStatsDesc().getDbName());
    Assert.assertEquals("events", routed.getColStats().get(0).getStatsDesc().getTableName());
    Assert.assertNull(routed.getColStats().get(0).getStatsDesc().getCatName());
    Assert.assertTrue(routed.getColStats().get(0).getStatsObj().get(0).getStatsData().isSetLongStats());
  }

  @Test
  public void exposurePolicyMatchesDatabaseAndTableRegexCaseInsensitively() throws Exception {
    FederationLayer layer = federationLayer(exposureConfig(
        CatalogExposureMode.DENY_BY_DEFAULT,
        java.util.List.of("sales", "finance"),
        Map.of("sales", java.util.List.of("orders_.*"))));
    CatalogRouter.ResolvedNamespace salesNamespace =
        new CatalogRouter.ResolvedNamespace(null, "catalog1", "sales", "sales");

    Assert.assertTrue(layer.isDatabaseExposed("catalog1", "Finance"));
    Assert.assertFalse(layer.isDatabaseExposed("catalog1", "hidden"));
    Assert.assertTrue(layer.isTableExposed(salesNamespace, "Orders_2024"));
    Assert.assertFalse(layer.isTableExposed(salesNamespace, "secret"));
  }

  @Test
  public void tableRulesCanExposeDatabaseWhenDenyByDefaultIsEnabled() throws Exception {
    FederationLayer layer = federationLayer(exposureConfig(
        CatalogExposureMode.DENY_BY_DEFAULT,
        java.util.List.of(),
        Map.of("sales", java.util.List.of("orders"))));
    CatalogRouter.ResolvedNamespace salesNamespace =
        new CatalogRouter.ResolvedNamespace(null, "catalog1", "sales", "sales");

    Assert.assertTrue(layer.isDatabaseExposed("catalog1", "sales"));
    Assert.assertFalse(layer.isDatabaseExposed("catalog1", "finance"));
    Assert.assertTrue(layer.isTableExposed(salesNamespace, "orders"));
    Assert.assertFalse(layer.isTableExposed(salesNamespace, "events"));
  }

  @SuppressWarnings("unchecked")
  private static FederationLayer federationLayer(ProxyConfig config) throws Exception {
    Constructor<CatalogRouter> constructor =
        CatalogRouter.class.getDeclaredConstructor(ProxyConfig.class, Map.class);
    constructor.setAccessible(true);
    Map<String, Object> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", null);
    CatalogRouter router = constructor.newInstance(config, backends);
    return new FederationLayer(config, router);
  }

  private static ProxyConfig viewRewriteConfig(boolean preserveOriginalViewText) {
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy", "127.0.0.1", 9083, 16, 64))
        .security(new SecurityConfig(
            SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            new CatalogConfig(
                "catalog1",
                "catalog1",
                "file:///warehouse/catalog1",
                false,
                CatalogAccessMode.READ_WRITE,
                java.util.List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms1:9083")),
            "catalog2",
            new CatalogConfig(
                "catalog2",
                "catalog2",
                "file:///warehouse/catalog2",
                false,
                CatalogAccessMode.READ_WRITE,
                java.util.List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms2:9083"))))
        .backend(new BackendConfig(Map.of()))
        .compatibility(new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false))
        .federation(new FederationConfig(
            false,
            ViewTextRewriteMode.REWRITE,
            preserveOriginalViewText))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(
            TransactionalDdlGuardMode.DISABLED,
            java.util.List.of()))
        .management(new ManagementConfig(false, "127.0.0.1", 10083))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static ProxyConfig exposureConfig(
      CatalogExposureMode exposeMode,
      java.util.List<String> exposeDbPatterns,
      Map<String, java.util.List<String>> exposeTablePatterns
  ) {
    return ProxyConfig.builder()
        .server(new ServerConfig("hms-proxy", "127.0.0.1", 9083, 16, 64))
        .security(new SecurityConfig(
            SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            new CatalogConfig(
                "catalog1",
                "catalog1",
                "file:///warehouse/catalog1",
                false,
                CatalogAccessMode.READ_WRITE,
                java.util.List.of(),
                exposeMode,
                exposeDbPatterns,
                exposeTablePatterns,
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms1:9083"))))
        .backend(new BackendConfig(Map.of()))
        .compatibility(new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false))
        .federation(new FederationConfig(
            false,
            ViewTextRewriteMode.DISABLED,
            false))
        .transactionalDdlGuard(new TransactionalDdlGuardConfig(
            TransactionalDdlGuardMode.DISABLED,
            java.util.List.of()))
        .management(new ManagementConfig(false, "127.0.0.1", 10083))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
