package io.github.mmalykhin.hmsproxy.routing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

/**
 * Namespace translation must never mutate its input and must never share mutable thrift state
 * between the input and the translated copy: request arguments are owned by the thrift processor.
 */
public class NamespaceTranslatorCopySemanticsTest {
  private static final CatalogRouter.ResolvedNamespace NAMESPACE =
      new CatalogRouter.ResolvedNamespace(
          null,
          "catalog1",
          "catalog1__sales",
          "sales");

  @Test
  public void internalizeLeavesRequestArgumentUntouched() {
    Table table = table("catalog1__sales");
    Table snapshot = new Table(table);

    Table routed = (Table) NamespaceTranslator.internalizeArgument(table, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("input argument must not be mutated", snapshot, table);
  }

  @Test
  public void externalizeLeavesBackendResultUntouched() {
    Table table = table("sales");
    Table snapshot = new Table(table);

    Table routed = (Table) NamespaceTranslator.externalizeResult(table, NAMESPACE);

    Assert.assertEquals("catalog1__sales", routed.getDbName());
    Assert.assertEquals("backend result must not be mutated", snapshot, table);
  }

  @Test
  public void internalizeDoesNotShareNestedStateWithRequestArgument() {
    Table table = table("catalog1__sales");

    Table routed = (Table) NamespaceTranslator.internalizeArgument(table, NAMESPACE);

    Assert.assertNotSame(table, routed);
    Assert.assertNotSame(table.getSd(), routed.getSd());
    Assert.assertNotSame(table.getSd().getSerdeInfo(), routed.getSd().getSerdeInfo());
    Assert.assertNotSame(table.getSd().getCols(), routed.getSd().getCols());
    Assert.assertNotSame(table.getSd().getCols().get(0), routed.getSd().getCols().get(0));
    Assert.assertNotSame(table.getPartitionKeys().get(0), routed.getPartitionKeys().get(0));
    Assert.assertNotSame(table.getParameters(), routed.getParameters());

    routed.getSd().setLocation("hdfs://other/path");
    routed.getSd().getCols().get(0).setName("renamed");
    routed.getSd().getSerdeInfo().getParameters().put("field.delim", ";");
    routed.getPartitionKeys().get(0).setName("renamed_key");
    routed.getParameters().put("owner", "changed");

    Assert.assertEquals("hdfs://backend/warehouse/events", table.getSd().getLocation());
    Assert.assertEquals("id", table.getSd().getCols().get(0).getName());
    Assert.assertEquals(",", table.getSd().getSerdeInfo().getParameters().get("field.delim"));
    Assert.assertEquals("dt", table.getPartitionKeys().get(0).getName());
    Assert.assertEquals("alice", table.getParameters().get("owner"));
  }

  @Test
  public void internalizeRewritesEveryElementOfAListArgumentWithoutTouchingTheOriginal() {
    List<Partition> partitions = new ArrayList<>();
    for (int index = 0; index < 3; index++) {
      Partition partition = new Partition();
      partition.setCatName("catalog1");
      partition.setDbName("catalog1__sales");
      partition.setTableName("events");
      partition.setValues(new ArrayList<>(List.of("2026-07-2" + index)));
      partition.setSd(storageDescriptor("hdfs://backend/warehouse/events/dt=2026-07-2" + index));
      partitions.add(partition);
    }
    List<Partition> snapshot = new ArrayList<>();
    for (Partition partition : partitions) {
      snapshot.add(new Partition(partition));
    }

    @SuppressWarnings("unchecked")
    List<Partition> routed = (List<Partition>) NamespaceTranslator.internalizeArgument(partitions, NAMESPACE);

    Assert.assertEquals(3, routed.size());
    for (int index = 0; index < routed.size(); index++) {
      Assert.assertEquals("sales", routed.get(index).getDbName());
      Assert.assertNull(routed.get(index).getCatName());
      Assert.assertEquals(List.of("2026-07-2" + index), routed.get(index).getValues());
      Assert.assertNotSame(partitions.get(index), routed.get(index));
      Assert.assertNotSame(partitions.get(index).getSd(), routed.get(index).getSd());
      Assert.assertEquals("input argument must not be mutated", snapshot.get(index), partitions.get(index));
    }
  }

  @Test
  public void internalizeRewritesDeeplyNestedRequestWithoutTouchingTheOriginal() {
    ColumnStatisticsDesc desc = new ColumnStatisticsDesc(true, "catalog1__sales", "events");
    desc.setCatName("catalog1");
    ColumnStatisticsObj statsObj = new ColumnStatisticsObj(
        "id", "bigint", ColumnStatisticsData.longStats(new LongColumnStatsData(1L, 2L)));
    ColumnStatistics statistics = new ColumnStatistics(desc, new ArrayList<>(List.of(statsObj)));
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
    request.setColStats(new ArrayList<>(List.of(statistics)));
    SetPartitionsStatsRequest snapshot = new SetPartitionsStatsRequest(request);

    SetPartitionsStatsRequest routed =
        (SetPartitionsStatsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    ColumnStatisticsDesc routedDesc = routed.getColStats().get(0).getStatsDesc();
    Assert.assertEquals("sales", routedDesc.getDbName());
    Assert.assertNull(routedDesc.getCatName());
    Assert.assertEquals("events", routedDesc.getTableName());
    Assert.assertEquals("id", routed.getColStats().get(0).getStatsObj().get(0).getColName());
    Assert.assertNotSame(request.getColStats().get(0), routed.getColStats().get(0));
    Assert.assertNotSame(
        request.getColStats().get(0).getStatsDesc(), routed.getColStats().get(0).getStatsDesc());
    Assert.assertNotSame(
        request.getColStats().get(0).getStatsObj().get(0),
        routed.getColStats().get(0).getStatsObj().get(0));
    Assert.assertEquals("input argument must not be mutated", snapshot, request);
  }

  private static Table table(String dbName) {
    Table table = new Table();
    table.setCatName("catalog1");
    table.setDbName(dbName);
    table.setTableName("events");
    table.setOwner("alice");
    table.setSd(storageDescriptor("hdfs://backend/warehouse/events"));
    table.setPartitionKeys(new ArrayList<>(List.of(new FieldSchema("dt", "string", null))));
    Map<String, String> parameters = new LinkedHashMap<>();
    parameters.put("owner", "alice");
    table.setParameters(parameters);
    return table;
  }

  private static StorageDescriptor storageDescriptor(String location) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(location);
    sd.setCols(new ArrayList<>(List.of(
        new FieldSchema("id", "bigint", null),
        new FieldSchema("name", "string", null))));
    SerDeInfo serdeInfo = new SerDeInfo();
    serdeInfo.setName("events-serde");
    serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    Map<String, String> serdeParameters = new LinkedHashMap<>();
    serdeParameters.put("field.delim", ",");
    serdeInfo.setParameters(serdeParameters);
    sd.setSerdeInfo(serdeInfo);
    return sd;
  }
}
