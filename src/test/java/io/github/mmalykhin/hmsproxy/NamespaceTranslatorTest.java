package io.github.mmalykhin.hmsproxy;

import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

public class NamespaceTranslatorTest {
  private static final CatalogRouter.ResolvedNamespace NAMESPACE =
      new CatalogRouter.ResolvedNamespace(
          null,
          "catalog1",
          "catalog1__sales",
          "sales");

  @Test
  public void internalizeClearsProxyCatalogAliasBeforeSendingToBackend() {
    GetTableRequest request = new GetTableRequest();
    request.setCatName("catalog1");
    request.setDbName("catalog1__sales");
    request.setTblName("events");

    GetTableRequest routed = (GetTableRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertNull(routed.getCatName());
    Assert.assertEquals("events", routed.getTblName());
  }

  @Test
  public void internalizePreservesNonProxyCatalogNameForBackendCompatibility() {
    GetTableRequest request = new GetTableRequest();
    request.setCatName("hive");
    request.setDbName("sales");
    request.setTblName("events");

    GetTableRequest routed = (GetTableRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("hive", routed.getCatName());
  }

  @Test
  public void internalizeClearsDefaultCatalogWhenDatabaseUsesProxyAlias() {
    GetTableRequest request = new GetTableRequest();
    request.setCatName("hive");
    request.setDbName("catalog1__sales");
    request.setTblName("events");

    GetTableRequest routed = (GetTableRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertNull(routed.getCatName());
  }

  @Test
  public void internalizeTableClearsDefaultCatalogWhenDatabaseUsesProxyAlias() {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("hive.catalog1__sales");
    table.setTableName("events");

    Table routed = (Table) NamespaceTranslator.internalizeArgument(table, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertNull(routed.getCatName());
    Assert.assertEquals("events", routed.getTableName());
  }
}
