package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TBase;

final class NamespaceTranslator {
  private NamespaceTranslator() {
  }

  static Object externalizeResult(Object value, CatalogRouter.ResolvedNamespace namespace) {
    if (value == null) {
      return null;
    }
    if (value instanceof Database database) {
      Database copy = new Database(database);
      copy.setName(namespace.externalDbName());
      copy.setCatalogName(namespace.catalogName());
      return copy;
    }
    if (value instanceof Table table) {
      return externalizeTable(table, namespace);
    }
    if (value instanceof TableMeta tableMeta) {
      return externalizeTableMeta(tableMeta, namespace);
    }
    if (value instanceof GetTableResult result) {
      GetTableResult copy = new GetTableResult(result);
      if (copy.isSetTable()) {
        copy.setTable((Table) externalizeTable(copy.getTable(), namespace));
      }
      return copy;
    }
    if (value instanceof GetTablesResult result) {
      GetTablesResult copy = new GetTablesResult(result);
      if (copy.isSetTables()) {
        List<Table> tables = new ArrayList<>();
        for (Table table : result.getTables()) {
          tables.add((Table) externalizeTable(table, namespace));
        }
        copy.setTables(tables);
      }
      return copy;
    }
    if (value instanceof List<?> list) {
      List<Object> transformed = new ArrayList<>(list.size());
      for (Object element : list) {
        transformed.add(externalizeResult(element, namespace));
      }
      return transformed;
    }
    return trySetNamespace(copyIfThrift(value), namespace.externalDbName(), namespace.catalogName(), true);
  }

  static Object internalizeArgument(Object value, CatalogRouter.ResolvedNamespace namespace) {
    if (value == null) {
      return null;
    }
    if (value instanceof Database database) {
      Database copy = new Database(database);
      copy.setName(namespace.backendDbName());
      copy.setCatalogName(namespace.catalogName());
      return copy;
    }
    if (value instanceof Table table) {
      Table copy = new Table(table);
      copy.setDbName(namespace.backendDbName());
      copy.setCatName(namespace.catalogName());
      return copy;
    }
    if (value instanceof GetTableRequest request) {
      GetTableRequest copy = new GetTableRequest(request);
      copy.setDbName(namespace.backendDbName());
      copy.setCatName(namespace.catalogName());
      return copy;
    }
    if (value instanceof GetTablesRequest request) {
      GetTablesRequest copy = new GetTablesRequest(request);
      copy.setDbName(namespace.backendDbName());
      copy.setCatName(namespace.catalogName());
      return copy;
    }
    if (value instanceof List<?> list) {
      List<Object> transformed = new ArrayList<>(list.size());
      for (Object element : list) {
        transformed.add(internalizeArgument(element, namespace));
      }
      return transformed;
    }
    return trySetNamespace(copyIfThrift(value), namespace.backendDbName(), namespace.catalogName(), false);
  }

  static Table externalizeTable(Table table, CatalogRouter.ResolvedNamespace namespace) {
    Table copy = new Table(table);
    copy.setDbName(namespace.externalDbName());
    copy.setCatName(namespace.catalogName());
    return copy;
  }

  static TableMeta externalizeTableMeta(TableMeta tableMeta, CatalogRouter.ResolvedNamespace namespace) {
    TableMeta copy = new TableMeta(tableMeta);
    copy.setDbName(namespace.externalDbName());
    copy.setCatName(namespace.catalogName());
    return copy;
  }

  private static Object copyIfThrift(Object value) {
    if (value instanceof TBase<?, ?> thriftValue) {
      return thriftValue.deepCopy();
    }
    return value;
  }

  private static Object trySetNamespace(Object value, String dbName, String catalogName, boolean externalizeDatabaseName) {
    if (value == null) {
      return null;
    }
    maybeInvoke(value, "setDbName", dbName);
    maybeInvoke(value, "setCatName", catalogName);
    maybeInvoke(value, "setCatalogName", catalogName);
    if (externalizeDatabaseName && value instanceof Database database) {
      database.setName(dbName);
    }
    return value;
  }

  private static void maybeInvoke(Object target, String methodName, String argument) {
    try {
      Method method = target.getClass().getMethod(methodName, String.class);
      method.invoke(target, argument);
    } catch (NoSuchMethodException ignored) {
      // Namespace propagation is best-effort for thrift DTOs with compatible setters.
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + methodName + " on " + target.getClass().getName(), e);
    }
  }
}
