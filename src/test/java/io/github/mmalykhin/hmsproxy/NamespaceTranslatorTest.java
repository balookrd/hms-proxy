package io.github.mmalykhin.hmsproxy;

import java.util.List;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
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

  @Test
  public void internalizeTableClearsDefaultCatalogWhenDatabaseUsesAtCatalogAlias() {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("@hive#catalog1__sales");
    table.setTableName("events");

    Table routed = (Table) NamespaceTranslator.internalizeArgument(table, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertNull(routed.getCatName());
    Assert.assertEquals("events", routed.getTableName());
  }

  @Test
  public void internalizeStatsRequestRewritesNestedColumnStatisticsNamespace() {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, "@hive#catalog1__sales", "events");
    statsDesc.setCatName("hive");

    ColumnStatistics statistics = new ColumnStatistics();
    statistics.setStatsDesc(statsDesc);
    statistics.setStatsObj(List.of());

    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
    request.setColStats(List.of(statistics));
    request.setNeedMerge(false);

    SetPartitionsStatsRequest routed =
        (SetPartitionsStatsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getColStats().get(0).getStatsDesc().getDbName());
    Assert.assertNull(routed.getColStats().get(0).getStatsDesc().getCatName());
    Assert.assertEquals("events", routed.getColStats().get(0).getStatsDesc().getTableName());
  }

  @Test
  public void internalizeStatsRequestHandlesUnionStatisticsPayload() {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, "@hive#catalog1__sales", "events");
    statsDesc.setCatName("hive");

    LongColumnStatsData longStats = new LongColumnStatsData();
    longStats.setLowValue(1L);
    longStats.setHighValue(1L);
    longStats.setNumDVs(1L);
    longStats.setNumNulls(0L);

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
    request.setNeedMerge(false);

    SetPartitionsStatsRequest routed =
        (SetPartitionsStatsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getColStats().get(0).getStatsDesc().getDbName());
    Assert.assertTrue(routed.getColStats().get(0).getStatsObj().get(0).getStatsData().isSetLongStats());
  }

  @Test
  public void extractDbNameReadsNestedStatsRequestNamespace() {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, "@hive#catalog1__sales", "events");
    statsDesc.setCatName("hive");

    ColumnStatistics statistics = new ColumnStatistics();
    statistics.setStatsDesc(statsDesc);
    statistics.setStatsObj(List.of());

    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
    request.setColStats(List.of(statistics));

    Assert.assertEquals("@hive#catalog1__sales", NamespaceTranslator.extractDbName(request));
  }

  @Test
  public void extractDbNameReadsDatabaseNameForCreateDatabaseStyleRequests() {
    Database database = new Database();
    database.setName("catalog1__sales");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(database));
  }
}
