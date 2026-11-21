package io.github.mmalykhin.hmsproxy.routing;

import java.util.Map;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Uniform mutable view over either a parent-classloader {@link Table} or a child-classloader
 * {@code Table} carried directly or inside a {@code *_req} envelope (HDP / Hive 4).
 */
final class TableObjectView {
  private static final String TABLE_CLASS_NAME = "org.apache.hadoop.hive.metastore.api.Table";

  private final Object target;
  private final Table parentTable;

  private TableObjectView(Object target) {
    this.target = target;
    this.parentTable = target instanceof Table t ? t : null;
  }

  static TableObjectView findInArgs(Object[] args) {
    if (args == null) {
      return null;
    }
    for (Object arg : args) {
      if (isTableObject(arg)) {
        return new TableObjectView(arg);
      }
    }
    for (Object arg : args) {
      if (arg == null) {
        continue;
      }
      Object nested = ThriftReflectionCache.invokeGetter(arg, "getTable");
      if (isTableObject(nested)) {
        return new TableObjectView(nested);
      }
    }
    return null;
  }

  private static boolean isTableObject(Object candidate) {
    return candidate instanceof Table
        || (candidate != null && TABLE_CLASS_NAME.equals(candidate.getClass().getName()));
  }

  Object unwrap() {
    return target;
  }

  String dbName() {
    return parentTable != null
        ? parentTable.getDbName()
        : ThriftReflectionCache.readString(target, "getDbName");
  }

  String tableName() {
    return parentTable != null
        ? parentTable.getTableName()
        : ThriftReflectionCache.readString(target, "getTableName");
  }

  String tableType() {
    return parentTable != null
        ? parentTable.getTableType()
        : ThriftReflectionCache.readString(target, "getTableType");
  }

  void setTableType(String tableType) {
    if (parentTable != null) {
      parentTable.setTableType(tableType);
    } else {
      ThriftReflectionCache.invokeStringSetter(target, "setTableType", tableType);
    }
  }

  boolean hasParameters() {
    if (parentTable != null) {
      return parentTable.isSetParameters();
    }
    Object isSet = ThriftReflectionCache.invokeGetter(target, "isSetParameters");
    if (isSet instanceof Boolean b) {
      return b;
    }
    return parameters() != null;
  }

  @SuppressWarnings("unchecked")
  Map<String, String> parameters() {
    if (parentTable != null) {
      return parentTable.getParameters();
    }
    Object value = ThriftReflectionCache.invokeGetter(target, "getParameters");
    return value instanceof Map<?, ?> ? (Map<String, String>) value : null;
  }

  void setParameters(Map<String, String> parameters) {
    if (parentTable != null) {
      parentTable.setParameters(parameters);
      return;
    }
    for (java.lang.reflect.Method method : target.getClass().getMethods()) {
      if ("setParameters".equals(method.getName()) && method.getParameterCount() == 1) {
        try {
          method.invoke(target, parameters);
          return;
        } catch (Exception e) {
          throw new IllegalStateException("Failed to invoke setParameters on " + target.getClass().getName(), e);
        }
      }
    }
  }

  void copyStorageFormatsFrom(StorageDescriptor from) {
    if (from == null) {
      return;
    }
    if (parentTable != null) {
      StorageDescriptor to = parentTable.getSd();
      if (to == null) {
        return;
      }
      to.setInputFormat(from.getInputFormat());
      to.setOutputFormat(from.getOutputFormat());
      SerDeInfo fromSerde = from.getSerdeInfo();
      SerDeInfo toSerde = to.getSerdeInfo();
      if (fromSerde != null && toSerde != null) {
        toSerde.setSerializationLib(fromSerde.getSerializationLib());
      }
      return;
    }
    Object sd = ThriftReflectionCache.invokeGetter(target, "getSd");
    if (sd == null) {
      return;
    }
    ThriftReflectionCache.invokeStringSetter(sd, "setInputFormat", from.getInputFormat());
    ThriftReflectionCache.invokeStringSetter(sd, "setOutputFormat", from.getOutputFormat());
    SerDeInfo fromSerde = from.getSerdeInfo();
    Object toSerde = ThriftReflectionCache.invokeGetter(sd, "getSerdeInfo");
    if (fromSerde != null && toSerde != null) {
      ThriftReflectionCache.invokeStringSetter(toSerde, "setSerializationLib", fromSerde.getSerializationLib());
    }
  }
}
