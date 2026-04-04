package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

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
  public void exposurePolicyMatchesDatabaseAndTableRegexCaseInsensitively() throws Exception {
    FederationLayer layer = federationLayer(exposureConfig(
        ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
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
        ProxyConfig.CatalogExposureMode.DENY_BY_DEFAULT,
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
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("hms-proxy", "127.0.0.1", 9083, 16, 64),
        new ProxyConfig.SecurityConfig(
            ProxyConfig.SecurityMode.NONE,
            null,
            null,
            null,
            null,
            false,
            Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            new ProxyConfig.CatalogConfig(
                "catalog1",
                "catalog1",
                "file:///warehouse/catalog1",
                false,
                ProxyConfig.CatalogAccessMode.READ_WRITE,
                java.util.List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms1:9083")),
            "catalog2",
            new ProxyConfig.CatalogConfig(
                "catalog2",
                "catalog2",
                "file:///warehouse/catalog2",
                false,
                ProxyConfig.CatalogAccessMode.READ_WRITE,
                java.util.List.of(),
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms2:9083"))),
        new ProxyConfig.BackendConfig(Map.of()),
        new ProxyConfig.CompatibilityConfig(ProxyConfig.FrontendProfile.APACHE_3_1_3, null, null, false),
        new ProxyConfig.FederationConfig(
            false,
            ProxyConfig.ViewTextRewriteMode.REWRITE,
            preserveOriginalViewText),
        new ProxyConfig.TransactionalDdlGuardConfig(
            ProxyConfig.TransactionalDdlGuardMode.DISABLED,
            java.util.List.of()),
        new ProxyConfig.ManagementConfig(false, "127.0.0.1", 10083));
  }

  private static ProxyConfig exposureConfig(
      ProxyConfig.CatalogExposureMode exposeMode,
      java.util.List<String> exposeDbPatterns,
      Map<String, java.util.List<String>> exposeTablePatterns
  ) {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("hms-proxy", "127.0.0.1", 9083, 16, 64),
        new ProxyConfig.SecurityConfig(
            ProxyConfig.SecurityMode.NONE,
            null,
            null,
            null,
            null,
            false,
            Map.of()),
        "__",
        "catalog1",
        Map.of(
            "catalog1",
            new ProxyConfig.CatalogConfig(
                "catalog1",
                "catalog1",
                "file:///warehouse/catalog1",
                false,
                ProxyConfig.CatalogAccessMode.READ_WRITE,
                java.util.List.of(),
                exposeMode,
                exposeDbPatterns,
                exposeTablePatterns,
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://hms1:9083"))),
        new ProxyConfig.BackendConfig(Map.of()),
        new ProxyConfig.CompatibilityConfig(ProxyConfig.FrontendProfile.APACHE_3_1_3, null, null, false),
        new ProxyConfig.FederationConfig(
            false,
            ProxyConfig.ViewTextRewriteMode.DISABLED,
            false),
        new ProxyConfig.TransactionalDdlGuardConfig(
            ProxyConfig.TransactionalDdlGuardMode.DISABLED,
            java.util.List.of()),
        new ProxyConfig.ManagementConfig(false, "127.0.0.1", 10083));
  }
}
