package io.github.mmalykhin.hmsproxy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;

final class WriteTraceUtil {
  private static final Set<String> TRACE_METHODS = Set.of(
      "get_database",
      "get_table",
      "get_table_req",
      "get_valid_write_ids",
      "open_txns",
      "allocate_table_write_ids",
      "lock",
      "check_lock",
      "commit_txn",
      "rollback_txn",
      "abort_txn",
      "heartbeat",
      "heartbeat_txn_range",
      "truncate_table",
      "alter_table",
      "alter_table_with_environment_context",
      "add_partition",
      "add_partitions",
      "add_partitions_req",
      "append_partition",
      "append_partition_by_name",
      "alter_partition",
      "alter_partitions",
      "rename_partition",
      "set_aggr_stats_for",
      "update_table_column_statistics",
      "update_partition_column_statistics"
  );
  private static final List<String> TABLE_PARAMETER_KEYS = List.of(
      "EXTERNAL",
      "TRANSLATED_TO_EXTERNAL",
      "transactional",
      "transactional_properties",
      "bucketing_version",
      "numFiles",
      "numRows",
      "totalSize",
      "rawDataSize",
      "transient_lastDdlTime"
  );
  private static final int MAX_ITEMS = 5;

  private WriteTraceUtil() {
  }

  static boolean shouldTrace(String methodName) {
    return TRACE_METHODS.contains(methodName);
  }

  static String summarizeArgs(Object[] args) {
    if (args == null || args.length == 0) {
      return "[]";
    }
    List<String> parts = new ArrayList<>(args.length);
    for (int index = 0; index < args.length; index++) {
      parts.add("arg" + index + "=" + summarizeValue(args[index]));
    }
    return '[' + String.join(", ", parts) + ']';
  }

  static String summarizeResult(Object value) {
    return summarizeValue(value);
  }

  private static String summarizeValue(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Table table) {
      return summarizeTable(table);
    }
    if (value instanceof TableMeta tableMeta) {
      return summarizeTableMeta(tableMeta);
    }
    if (value instanceof Partition partition) {
      return summarizePartition(partition);
    }
    if (value instanceof Database database) {
      return summarizeDatabase(database);
    }
    if (value instanceof GetTableRequest request) {
      return summarizeGetTableRequest(request);
    }
    if (value instanceof SetPartitionsStatsRequest request) {
      return summarizeStatsRequest(request);
    }
    if (value instanceof EnvironmentContext context) {
      return "EnvironmentContext(properties=" + summarizeMap(context.getProperties()) + ")";
    }
    if (value instanceof GetValidWriteIdsRequest request) {
      return "GetValidWriteIdsRequest(fullTableNames=" + summarizeList(request.getFullTableNames())
          + ", validTxnList=" + abbreviate(request.getValidTxnList()) + ')';
    }
    if (value instanceof GetValidWriteIdsResponse response) {
      return "GetValidWriteIdsResponse(tblValidWriteIds="
          + summarizeTableValidWriteIds(response.getTblValidWriteIds()) + ')';
    }
    if (value instanceof LockRequest request) {
      return "LockRequest(txnid=" + request.getTxnid()
          + ", user=" + request.getUser()
          + ", components=" + summarizeLockComponents(request.getComponent()) + ')';
    }
    if (value instanceof List<?> list) {
      return summarizeList(list);
    }
    if (value instanceof Map<?, ?> map) {
      return summarizeMap(map);
    }
    if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean
        || value instanceof Enum<?>) {
      return String.valueOf(value);
    }
    return DebugLogUtil.formatValue(value);
  }

  private static String summarizeGetTableRequest(GetTableRequest request) {
    return "GetTableRequest(catName=" + blankToDash(request.getCatName())
        + ", dbName=" + blankToDash(request.getDbName())
        + ", table=" + blankToDash(request.getTblName())
        + ", capabilities=" + summarizeCapabilities(request.getCapabilities()) + ')';
  }

  private static String summarizeDatabase(Database database) {
    return "Database(name=" + blankToDash(database.getName())
        + ", catalog=" + blankToDash(database.getCatalogName())
        + ", location=" + blankToDash(database.getLocationUri()) + ')';
  }

  private static String summarizeTableMeta(TableMeta tableMeta) {
    return "TableMeta(cat=" + blankToDash(tableMeta.getCatName())
        + ", db=" + blankToDash(tableMeta.getDbName())
        + ", table=" + blankToDash(tableMeta.getTableName())
        + ", type=" + blankToDash(tableMeta.getTableType()) + ')';
  }

  private static String summarizeTable(Table table) {
    return "Table(cat=" + blankToDash(table.getCatName())
        + ", db=" + blankToDash(table.getDbName())
        + ", table=" + blankToDash(table.getTableName())
        + ", type=" + blankToDash(table.getTableType())
        + ", location=" + summarizeTableLocation(table)
        + ", partitionKeys=" + summarizeFieldNames(table.getPartitionKeys())
        + ", params=" + summarizeTableParameters(table.getParameters()) + ')';
  }

  private static String summarizePartition(Partition partition) {
    return "Partition(cat=" + blankToDash(partition.getCatName())
        + ", db=" + blankToDash(partition.getDbName())
        + ", table=" + blankToDash(partition.getTableName())
        + ", values=" + summarizeList(partition.getValues())
        + ", location=" + (partition.isSetSd() ? blankToDash(partition.getSd().getLocation()) : "-")
        + ", params=" + summarizeTableParameters(partition.getParameters()) + ')';
  }

  private static String summarizeStatsRequest(SetPartitionsStatsRequest request) {
    List<String> parts = new ArrayList<>();
    List<ColumnStatistics> statistics = request.getColStats();
    if (statistics != null) {
      int limit = Math.min(statistics.size(), MAX_ITEMS);
      for (int index = 0; index < limit; index++) {
        parts.add(summarizeColumnStatistics(statistics.get(index)));
      }
      if (statistics.size() > limit) {
        parts.add("... size=" + statistics.size());
      }
    }
    return "SetPartitionsStatsRequest(needMerge=" + request.isNeedMerge()
        + ", colStats=" + parts + ')';
  }

  private static String summarizeColumnStatistics(ColumnStatistics statistics) {
    ColumnStatisticsDesc desc = statistics.getStatsDesc();
    if (desc == null) {
      return "ColumnStatistics(desc=-)";
    }
    List<String> columnNames = new ArrayList<>();
    if (statistics.getStatsObj() != null) {
      int limit = Math.min(statistics.getStatsObj().size(), MAX_ITEMS);
      for (int index = 0; index < limit; index++) {
        columnNames.add(statistics.getStatsObj().get(index).getColName());
      }
      if (statistics.getStatsObj().size() > limit) {
        columnNames.add("... size=" + statistics.getStatsObj().size());
      }
    }
    return "ColumnStatistics(cat=" + blankToDash(desc.getCatName())
        + ", db=" + blankToDash(desc.getDbName())
        + ", table=" + blankToDash(desc.getTableName())
        + ", tblLevel=" + desc.isIsTblLevel()
        + ", columns=" + columnNames + ')';
  }

  private static String summarizeCapabilities(org.apache.hadoop.hive.metastore.api.ClientCapabilities capabilities) {
    if (capabilities == null || capabilities.getValues() == null) {
      return "[]";
    }
    return summarizeList(capabilities.getValues());
  }

  private static String summarizeTableValidWriteIds(List<TableValidWriteIds> values) {
    if (values == null || values.isEmpty()) {
      return "[]";
    }
    List<String> parts = new ArrayList<>();
    int limit = Math.min(values.size(), MAX_ITEMS);
    for (int index = 0; index < limit; index++) {
      TableValidWriteIds value = values.get(index);
      parts.add(value.getFullTableName() + "@highWatermark=" + value.getWriteIdHighWaterMark());
    }
    if (values.size() > limit) {
      parts.add("... size=" + values.size());
    }
    return parts.toString();
  }

  private static String summarizeLockComponents(List<LockComponent> components) {
    if (components == null || components.isEmpty()) {
      return "[]";
    }
    List<String> parts = new ArrayList<>();
    int limit = Math.min(components.size(), MAX_ITEMS);
    for (int index = 0; index < limit; index++) {
      LockComponent component = components.get(index);
      parts.add("LockComponent(db=" + blankToDash(component.getDbname())
          + ", table=" + blankToDash(component.getTablename())
          + ", partition=" + blankToDash(component.getPartitionname()) + ')');
    }
    if (components.size() > limit) {
      parts.add("... size=" + components.size());
    }
    return parts.toString();
  }

  private static String summarizeFieldNames(List<org.apache.hadoop.hive.metastore.api.FieldSchema> fields) {
    if (fields == null || fields.isEmpty()) {
      return "[]";
    }
    List<String> names = new ArrayList<>();
    int limit = Math.min(fields.size(), MAX_ITEMS);
    for (int index = 0; index < limit; index++) {
      names.add(fields.get(index).getName());
    }
    if (fields.size() > limit) {
      names.add("... size=" + fields.size());
    }
    return names.toString();
  }

  private static String summarizeTableLocation(Table table) {
    if (!table.isSetSd() || table.getSd() == null) {
      return "-";
    }
    return blankToDash(table.getSd().getLocation());
  }

  private static String summarizeTableParameters(Map<String, String> parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return "{}";
    }
    Map<String, String> summary = new LinkedHashMap<>();
    for (String key : TABLE_PARAMETER_KEYS) {
      if (parameters.containsKey(key)) {
        summary.put(key, parameters.get(key));
      }
    }
    if (summary.isEmpty()) {
      return "{}";
    }
    return summary.toString();
  }

  private static String summarizeList(List<?> values) {
    if (values == null || values.isEmpty()) {
      return "[]";
    }
    List<String> parts = new ArrayList<>();
    int limit = Math.min(values.size(), MAX_ITEMS);
    for (int index = 0; index < limit; index++) {
      parts.add(abbreviate(String.valueOf(values.get(index))));
    }
    if (values.size() > limit) {
      parts.add("... size=" + values.size());
    }
    return parts.toString();
  }

  private static String summarizeMap(Map<?, ?> values) {
    if (values == null || values.isEmpty()) {
      return "{}";
    }
    Map<String, String> summary = new LinkedHashMap<>();
    int count = 0;
    for (Map.Entry<?, ?> entry : values.entrySet()) {
      if (count >= MAX_ITEMS) {
        summary.put("...", "size=" + values.size());
        break;
      }
      summary.put(abbreviate(String.valueOf(entry.getKey())), abbreviate(String.valueOf(entry.getValue())));
      count++;
    }
    return summary.toString();
  }

  private static String abbreviate(String value) {
    if (value == null) {
      return "-";
    }
    return value.length() <= 120 ? value : value.substring(0, 120) + "...";
  }

  private static String blankToDash(String value) {
    return value == null || value.isBlank() ? "-" : value;
  }
}
