package io.github.mmalykhin.hmsproxy;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

public class WriteTraceUtilTest {
  @Test
  public void traceListIncludesManagedWriteAndMetadataMethods() {
    Assert.assertTrue(WriteTraceUtil.shouldTrace("get_table_req"));
    Assert.assertTrue(WriteTraceUtil.shouldTrace("alter_table_with_environment_context"));
    Assert.assertTrue(WriteTraceUtil.shouldTrace("set_aggr_stats_for"));
    Assert.assertFalse(WriteTraceUtil.shouldTrace("get_all_databases"));
  }

  @Test
  public void summarizeTableKeepsManagedMetadataInCompactForm() {
    Table table = new Table();
    table.setCatName("catalog2");
    table.setDbName("catalog2__default");
    table.setTableName("a");
    table.setTableType("MANAGED_TABLE");
    table.setPartitionKeys(List.of(new FieldSchema("dt", "string", null)));
    table.setParameters(Map.of(
        "numFiles", "1",
        "numRows", "2",
        "totalSize", "189",
        "transient_lastDdlTime", "1774605758",
        "bucketing_version", "2"));
    table.setSd(storageDescriptor("hdfs://ns-rnd3/warehouse/tablespace/managed/hive/a"));

    String summary = WriteTraceUtil.summarizeArgs(new Object[] {"@hive#catalog2__default", "a", table});

    Assert.assertTrue(summary.contains("catalog2__default"));
    Assert.assertTrue(summary.contains("MANAGED_TABLE"));
    Assert.assertTrue(summary.contains("hdfs://ns-rnd3/warehouse/tablespace/managed/hive/a"));
    Assert.assertTrue(summary.contains("partitionKeys=[dt]"));
    Assert.assertTrue(summary.contains("numFiles=1"));
    Assert.assertTrue(summary.contains("transient_lastDdlTime=1774605758"));
  }

  @Test
  public void summarizeStatsRequestHighlightsNamespaceAndColumns() {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, "catalog2__default", "a");
    desc.setCatName("hive");

    ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
    statsObj.setColName("num");
    statsObj.setColType("int");

    ColumnStatistics statistics = new ColumnStatistics();
    statistics.setStatsDesc(desc);
    statistics.setStatsObj(List.of(statsObj));

    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
    request.setColStats(List.of(statistics));
    request.setNeedMerge(false);

    String summary = WriteTraceUtil.summarizeArgs(new Object[] {request});

    Assert.assertTrue(summary.contains("catalog2__default"));
    Assert.assertTrue(summary.contains("table=a"));
    Assert.assertTrue(summary.contains("columns=[num]"));
    Assert.assertTrue(summary.contains("needMerge=false"));
  }

  @Test
  public void summarizeGetTableRequestKeepsCapabilities() {
    GetTableRequest request = new GetTableRequest();
    request.setCatName("hive");
    request.setDbName("catalog2__default");
    request.setTblName("a");

    String summary = WriteTraceUtil.summarizeArgs(new Object[] {request});

    Assert.assertTrue(summary.contains("catName=hive"));
    Assert.assertTrue(summary.contains("dbName=catalog2__default"));
    Assert.assertTrue(summary.contains("table=a"));
    Assert.assertTrue(summary.contains("capabilities=[]"));
  }

  private static StorageDescriptor storageDescriptor(String location) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(List.of(new FieldSchema("num", "int", null)));
    storageDescriptor.setLocation(location);
    return storageDescriptor;
  }
}
