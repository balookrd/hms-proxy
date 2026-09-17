package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class CreateTableReqHandler implements SpecialCaseHandler {
  private static final Method CREATE_TABLE = findMethod("create_table", Table.class);
  private static final Method CREATE_TABLE_WITH_ENV = findMethod("create_table_with_environment_context",
      Table.class, EnvironmentContext.class);
  private static final Method CREATE_TABLE_WITH_CONSTRAINTS = findMethod("create_table_with_constraints",
      Table.class, List.class, List.class, List.class, List.class, List.class, List.class);
  private static final Method ALTER_TABLE_WITH_ENV = findMethod("alter_table_with_environment_context",
      String.class, String.class, Table.class, EnvironmentContext.class);

  private final RoutingSupport support;

  CreateTableReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    Object rawTable = ThriftReflectionCache.invokeGetter(request, "getTable");
    if (rawTable == null) {
      throw new MetaException("create_table_req requires a table");
    }
    String dbName = (String) ThriftReflectionCache.invokeGetter(rawTable, "getDbName");
    if (dbName == null) {
      throw new MetaException("create_table_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("create_table_req", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "create_table_req", namespace.backendDbName());

    internalizeCreateTableRequest(request, namespace.backendDbName());

    try {
      return support.invokeBackendNamed(backend, "create_table_req", request);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        return fallbackToLegacy(backend, request);
      }
      throw cause;
    }
  }

  private void internalizeCreateTableRequest(Object request, String backendDbName) {
    Object rawTable = ThriftReflectionCache.invokeGetter(request, "getTable");
    if (rawTable != null) {
      ThriftReflectionCache.invokeStringSetter(rawTable, "setDbName", backendDbName);
    }
    internalizeConstraintList(ThriftReflectionCache.invokeGetter(request, "getPrimaryKeys"), "setTable_db", backendDbName);
    internalizeConstraintList(ThriftReflectionCache.invokeGetter(request, "getForeignKeys"), "setFktable_db", backendDbName);
    internalizeConstraintList(ThriftReflectionCache.invokeGetter(request, "getUniqueConstraints"), "setTable_db", backendDbName);
    internalizeConstraintList(ThriftReflectionCache.invokeGetter(request, "getNotNullConstraints"), "setTable_db", backendDbName);
    internalizeConstraintList(ThriftReflectionCache.invokeGetter(request, "getDefaultConstraints"), "setTable_db", backendDbName);
    internalizeConstraintList(ThriftReflectionCache.invokeGetter(request, "getCheckConstraints"), "setTable_db", backendDbName);
  }

  private void internalizeConstraintList(Object listObj, String setterName, String backendDbName) {
    if (listObj instanceof List<?> list) {
      for (Object item : list) {
        if (item != null) {
          ThriftReflectionCache.invokeStringSetter(item, setterName, backendDbName);
        }
      }
    }
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    Object rawTable = ThriftReflectionCache.invokeGetter(routedRequest, "getTable");
    Table apacheTable = (Table) ThriftValueConverter.convertTBase(rawTable, Table.class);
    Object rawEnv = ThriftReflectionCache.invokeGetter(routedRequest, "getEnvContext");
    EnvironmentContext apacheEnv = rawEnv == null ? null : (EnvironmentContext) ThriftValueConverter.convertTBase(rawEnv, EnvironmentContext.class);

    @SuppressWarnings("unchecked")
    List<Object> pks = (List<Object>) ThriftReflectionCache.invokeGetter(routedRequest, "getPrimaryKeys");
    @SuppressWarnings("unchecked")
    List<Object> fks = (List<Object>) ThriftReflectionCache.invokeGetter(routedRequest, "getForeignKeys");
    @SuppressWarnings("unchecked")
    List<Object> uqs = (List<Object>) ThriftReflectionCache.invokeGetter(routedRequest, "getUniqueConstraints");
    @SuppressWarnings("unchecked")
    List<Object> nns = (List<Object>) ThriftReflectionCache.invokeGetter(routedRequest, "getNotNullConstraints");
    @SuppressWarnings("unchecked")
    List<Object> dfs = (List<Object>) ThriftReflectionCache.invokeGetter(routedRequest, "getDefaultConstraints");
    @SuppressWarnings("unchecked")
    List<Object> cks = (List<Object>) ThriftReflectionCache.invokeGetter(routedRequest, "getCheckConstraints");

    boolean hasConstraints = hasAny(pks, fks, uqs, nns, dfs, cks);
    if (hasConstraints) {
      support.invokeDirect(backend, CREATE_TABLE_WITH_CONSTRAINTS, new Object[]{
          apacheTable,
          typedList(pks, org.apache.hadoop.hive.metastore.api.SQLPrimaryKey.class),
          typedList(fks, org.apache.hadoop.hive.metastore.api.SQLForeignKey.class),
          typedList(uqs, org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint.class),
          typedList(nns, org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint.class),
          typedList(dfs, org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint.class),
          typedList(cks, org.apache.hadoop.hive.metastore.api.SQLCheckConstraint.class)
      });
      if (apacheEnv != null) {
        try {
          support.invokeDirect(backend, ALTER_TABLE_WITH_ENV, new Object[]{
              apacheTable.getDbName(), apacheTable.getTableName(), apacheTable, apacheEnv
          });
        } catch (Throwable ignored) {
          // Best-effort application of environmentContext after creation with constraints
        }
      }
    } else if (apacheEnv != null) {
      support.invokeDirect(backend, CREATE_TABLE_WITH_ENV, new Object[]{apacheTable, apacheEnv});
    } else {
      support.invokeDirect(backend, CREATE_TABLE, new Object[]{apacheTable});
    }
    return null;
  }

  private static boolean hasAny(List<?>... lists) {
    for (List<?> list : lists) {
      if (list != null && !list.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static <T> List<T> typedList(List<Object> value, Class<T> type) {
    if (value == null || value.isEmpty()) {
      return List.of();
    }
    try {
      return (List<T>) ThriftValueConverter.convertDynamicValue(value, CreateTableReqHandler.class.getClassLoader());
    } catch (Exception e) {
      return List.of();
    }
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
