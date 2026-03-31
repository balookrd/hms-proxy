package io.github.mmalykhin.hmsproxy;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.junit.Assert;
import org.junit.Test;

public class NamespaceTranslatorTest {
  private static final CatalogRouter.ResolvedNamespace NAMESPACE =
      new CatalogRouter.ResolvedNamespace(
          null,
          "catalog1",
          "catalog1__sales",
          "sales");
  private static final CatalogRouter.ResolvedNamespace DEFAULT_NAMESPACE =
      new CatalogRouter.ResolvedNamespace(
          null,
          "catalog1",
          "sales",
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
  public void internalizePreservesNonProxyCatalogNameWhenDefaultCatalogUsesRawDatabaseName() {
    GetTableRequest request = new GetTableRequest();
    request.setCatName("hive");
    request.setDbName("sales");
    request.setTblName("events");

    GetTableRequest routed = (GetTableRequest) NamespaceTranslator.internalizeArgument(request, DEFAULT_NAMESPACE);

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
  public void internalizePreservesDefaultCatalogWhenDatabaseUsesProxyAliasInCompatibilityMode() {
    GetTableRequest request = new GetTableRequest();
    request.setCatName("hive");
    request.setDbName("catalog1__sales");
    request.setTblName("events");

    GetTableRequest routed =
        (GetTableRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE, true);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("hive", routed.getCatName());
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
  public void internalizeTablePreservesDefaultCatalogWhenDatabaseUsesProxyAliasInCompatibilityMode() {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("catalog1__sales");
    table.setTableName("events");

    Table routed = (Table) NamespaceTranslator.internalizeArgument(table, NAMESPACE, true);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("hive", routed.getCatName());
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
  public void externalizeExternalTablePreservesExternalLocationAndType() {
    Table table = table("sales", "clicks", "EXTERNAL_TABLE", "s3://warehouse/sales/clicks");
    table.setParameters(Map.of("EXTERNAL", "TRUE"));

    Table routed = NamespaceTranslator.externalizeTable(table, NAMESPACE);

    Assert.assertEquals("catalog1__sales", routed.getDbName());
    Assert.assertEquals("catalog1", routed.getCatName());
    Assert.assertEquals("EXTERNAL_TABLE", routed.getTableType());
    Assert.assertEquals("TRUE", routed.getParameters().get("EXTERNAL"));
    Assert.assertEquals("s3://warehouse/sales/clicks", routed.getSd().getLocation());
  }

  @Test
  public void externalizeAcidManagedTablePreservesTransactionalMetadata() {
    Table table = table("sales", "events", "MANAGED_TABLE", "/warehouse/sales.db/events");
    table.setCatName("hive");
    table.setParameters(Map.of(
        "transactional", "true",
        "transactional_properties", "default"));
    table.setPartitionKeys(List.of(new FieldSchema("dt", "string", null)));

    Table routed = NamespaceTranslator.externalizeTable(table, NAMESPACE);

    Assert.assertEquals("catalog1__sales", routed.getDbName());
    Assert.assertEquals("catalog1", routed.getCatName());
    Assert.assertEquals("MANAGED_TABLE", routed.getTableType());
    Assert.assertEquals("true", routed.getParameters().get("transactional"));
    Assert.assertEquals("default", routed.getParameters().get("transactional_properties"));
    Assert.assertEquals("/warehouse/sales.db/events", routed.getSd().getLocation());
    Assert.assertEquals("dt", routed.getPartitionKeys().get(0).getName());
  }

  @Test
  public void externalizeTablePreservesBackendCatalogNameInCompatibilityMode() {
    Table table = table("sales", "events", "MANAGED_TABLE", "/warehouse/sales.db/events");
    table.setCatName("hive");

    Table routed = NamespaceTranslator.externalizeTable(table, NAMESPACE, true);

    Assert.assertEquals("catalog1__sales", routed.getDbName());
    Assert.assertEquals("hive", routed.getCatName());
  }

  @Test
  public void externalizeDatabasePreservesBackendCatalogNameInCompatibilityMode() {
    Database database = new Database();
    database.setName("sales");
    database.setCatalogName("hive");

    Database routed = (Database) NamespaceTranslator.externalizeResult(database, NAMESPACE, true);

    Assert.assertEquals("catalog1__sales", routed.getName());
    Assert.assertEquals("hive", routed.getCatalogName());
  }

  @Test
  public void internalizeStringArgumentRewritesAtCatalogDatabaseAlias() {
    Assert.assertEquals("sales", NamespaceTranslator.internalizeStringArgument("@hive#catalog1__sales", NAMESPACE));
    Assert.assertEquals("sales", NamespaceTranslator.internalizeStringArgument("catalog1__sales", NAMESPACE));
    Assert.assertEquals("events", NamespaceTranslator.internalizeStringArgument("events", NAMESPACE));
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
  public void internalizeStatsRequestPreservesDefaultCatalogInCompatibilityMode() {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, "@hive#catalog1__sales", "events");
    statsDesc.setCatName("hive");

    ColumnStatistics statistics = new ColumnStatistics();
    statistics.setStatsDesc(statsDesc);
    statistics.setStatsObj(List.of());

    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest();
    request.setColStats(List.of(statistics));
    request.setNeedMerge(false);

    SetPartitionsStatsRequest routed =
        (SetPartitionsStatsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE, true);

    Assert.assertEquals("sales", routed.getColStats().get(0).getStatsDesc().getDbName());
    Assert.assertEquals("hive", routed.getColStats().get(0).getStatsDesc().getCatName());
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
  public void extractDbNameReadsFullTableNamesNamespace() {
    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest();
    request.setFullTableNames(List.of("catalog1__sales.events"));
    request.setValidTxnList("txns");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(request));
  }

  @Test
  public void extractDbNameReadsCompactionRequestNamespace() {
    CompactionRequest request = new CompactionRequest();
    request.setDbname("catalog1__sales");
    request.setTablename("events");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(request));
  }

  @Test
  public void extractDbNameReadsShowLocksRequestNamespace() {
    ShowLocksRequest request = new ShowLocksRequest();
    request.setDbname("catalog1__sales");
    request.setTablename("events");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(request));
  }

  @Test
  public void extractDbNameReadsFireEventRequestNamespace() {
    FireEventRequest request = new FireEventRequest();
    request.setDbName("catalog1__sales");
    request.setTableName("events");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(request));
  }

  @Test
  public void extractDbNameReadsSnakeCaseConstraintRequestNamespace() {
    PrimaryKeysRequest request = new PrimaryKeysRequest();
    request.setDb_name("catalog1__sales");
    request.setTbl_name("events");
    request.setCatName("hive");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(request));
  }

  @Test
  public void extractDbNameReadsDatabaseNameForCreateDatabaseStyleRequests() {
    Database database = new Database();
    database.setName("catalog1__sales");

    Assert.assertEquals("catalog1__sales", NamespaceTranslator.extractDbName(database));
  }

  @Test
  public void internalizePrivilegeBagRewritesNestedHiveObjectReferences() {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setObjectType(HiveObjectType.TABLE);
    hiveObjectRef.setDbName("@hive#catalog1__sales");
    hiveObjectRef.setObjectName("events");
    hiveObjectRef.setCatName("hive");

    HiveObjectPrivilege privilege = new HiveObjectPrivilege();
    privilege.setHiveObject(hiveObjectRef);

    PrivilegeBag privilegeBag = new PrivilegeBag();
    privilegeBag.setPrivileges(List.of(privilege));

    PrivilegeBag routed = (PrivilegeBag) NamespaceTranslator.internalizeArgument(privilegeBag, NAMESPACE);

    Assert.assertEquals("sales", routed.getPrivileges().get(0).getHiveObject().getDbName());
    Assert.assertNull(routed.getPrivileges().get(0).getHiveObject().getCatName());
    Assert.assertEquals("events", routed.getPrivileges().get(0).getHiveObject().getObjectName());
  }

  @Test
  public void internalizeValidWriteIdsRequestRewritesFullTableNames() {
    GetValidWriteIdsRequest request = new GetValidWriteIdsRequest();
    request.setFullTableNames(List.of("@hive#catalog1__sales.events"));
    request.setValidTxnList("txns");

    GetValidWriteIdsRequest routed =
        (GetValidWriteIdsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals(List.of("sales.events"), routed.getFullTableNames());
    Assert.assertEquals("txns", routed.getValidTxnList());
  }

  @Test
  public void internalizeAllocateTableWriteIdsRequestRewritesDbName() {
    AllocateTableWriteIdsRequest request = new AllocateTableWriteIdsRequest();
    request.setDbName("@hive#catalog1__sales");
    request.setTableName("events");
    request.setTxnIds(List.of(1L, 2L));

    AllocateTableWriteIdsRequest routed =
        (AllocateTableWriteIdsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("events", routed.getTableName());
    Assert.assertEquals(List.of(1L, 2L), routed.getTxnIds());
  }

  @Test
  public void internalizeReplTblWriteIdStateRequestRewritesDbName() {
    ReplTblWriteIdStateRequest request = new ReplTblWriteIdStateRequest();
    request.setDbName("@hive#catalog1__sales");
    request.setTableName("events");
    request.setValidWriteIdlist("1:2:3");
    request.setUser("alice");
    request.setHostName("host1");

    ReplTblWriteIdStateRequest routed =
        (ReplTblWriteIdStateRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertEquals("events", routed.getTableName());
    Assert.assertEquals("1:2:3", routed.getValidWriteIdlist());
  }

  @Test
  public void internalizeSnakeCaseConstraintRequestRewritesDbName() {
    NotNullConstraintsRequest request = new NotNullConstraintsRequest();
    request.setCatName("hive");
    request.setDb_name("@hive#catalog1__sales");
    request.setTbl_name("events");

    NotNullConstraintsRequest routed =
        (NotNullConstraintsRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getDb_name());
    Assert.assertEquals("hive", routed.getCatName());
    Assert.assertEquals("events", routed.getTbl_name());
  }

  @Test
  public void internalizeForeignKeysRequestRewritesAllDbNameFields() {
    ForeignKeysRequest request = new ForeignKeysRequest();
    request.setCatName("hive");
    request.setParent_db_name("@hive#catalog1__sales");
    request.setParent_tbl_name("parent_events");
    request.setForeign_db_name("@hive#catalog1__sales");
    request.setForeign_tbl_name("events");

    ForeignKeysRequest routed = (ForeignKeysRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getParent_db_name());
    Assert.assertEquals("sales", routed.getForeign_db_name());
    Assert.assertEquals("hive", routed.getCatName());
  }

  @Test
  public void externalizeValidWriteIdsResponseRewritesFullTableName() {
    TableValidWriteIds tableValidWriteIds = new TableValidWriteIds();
    tableValidWriteIds.setFullTableName("hive.sales.events");
    tableValidWriteIds.setWriteIdHighWaterMark(7L);
    tableValidWriteIds.setInvalidWriteIds(List.of());
    tableValidWriteIds.setAbortedBits(new byte[0]);

    GetValidWriteIdsResponse response = new GetValidWriteIdsResponse();
    response.setTblValidWriteIds(List.of(tableValidWriteIds));

    GetValidWriteIdsResponse routed =
        (GetValidWriteIdsResponse) NamespaceTranslator.externalizeResult(response, NAMESPACE);

    Assert.assertEquals("catalog1__sales.events", routed.getTblValidWriteIds().get(0).getFullTableName());
  }

  @Test
  public void internalizeLockRequestRewritesNestedDbname() {
    LockComponent component = new LockComponent();
    component.setDbname("@hive#catalog1__sales");
    component.setTablename("events");

    LockRequest request = new LockRequest();
    request.setComponent(List.of(component));
    request.setUser("alice");
    request.setHostname("host");

    LockRequest routed = (LockRequest) NamespaceTranslator.internalizeArgument(request, NAMESPACE);

    Assert.assertEquals("sales", routed.getComponent().get(0).getDbname());
    Assert.assertEquals("events", routed.getComponent().get(0).getTablename());
  }

  @Test
  public void externalizePartitionPreservesLocationAndValuesForPartitionedTable() {
    Partition partition = new Partition();
    partition.setCatName("hive");
    partition.setDbName("sales");
    partition.setTableName("events");
    partition.setValues(List.of("2026-03-27"));
    partition.setSd(storageDescriptor("/warehouse/sales.db/events/dt=2026-03-27"));
    partition.setParameters(Map.of("numFiles", "3"));

    Partition routed = (Partition) NamespaceTranslator.externalizeResult(partition, NAMESPACE);

    Assert.assertEquals("catalog1__sales", routed.getDbName());
    Assert.assertEquals("catalog1", routed.getCatName());
    Assert.assertEquals(List.of("2026-03-27"), routed.getValues());
    Assert.assertEquals("/warehouse/sales.db/events/dt=2026-03-27", routed.getSd().getLocation());
    Assert.assertEquals("3", routed.getParameters().get("numFiles"));
  }

  @Test
  public void internalizePartitionPreservesLocationAndValuesForPartitionedTable() {
    Partition partition = new Partition();
    partition.setCatName("hive");
    partition.setDbName("@hive#catalog1__sales");
    partition.setTableName("events");
    partition.setValues(List.of("2026-03-27"));
    partition.setSd(storageDescriptor("/warehouse/sales.db/events/dt=2026-03-27"));
    partition.setParameters(Map.of("numFiles", "3"));

    Partition routed = (Partition) NamespaceTranslator.internalizeArgument(partition, NAMESPACE);

    Assert.assertEquals("sales", routed.getDbName());
    Assert.assertNull(routed.getCatName());
    Assert.assertEquals(List.of("2026-03-27"), routed.getValues());
    Assert.assertEquals("/warehouse/sales.db/events/dt=2026-03-27", routed.getSd().getLocation());
    Assert.assertEquals("3", routed.getParameters().get("numFiles"));
  }

  private static Table table(String dbName, String tableName, String tableType, String location) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setTableType(tableType);
    table.setSd(storageDescriptor(location));
    return table;
  }

  private static StorageDescriptor storageDescriptor(String location) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(List.of(new FieldSchema("id", "bigint", null)));
    storageDescriptor.setLocation(location);
    return storageDescriptor;
  }
}
