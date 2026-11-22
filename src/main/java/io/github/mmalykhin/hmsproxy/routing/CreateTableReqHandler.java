package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

final class CreateTableReqHandler implements SpecialCaseHandler {
  private static final Method CREATE_TABLE = RequestObjectRouting.findIfaceMethod("create_table", Table.class);
  private static final Method CREATE_TABLE_WITH_ENV = RequestObjectRouting.findIfaceMethod(
      "create_table_with_environment_context", Table.class, EnvironmentContext.class);
  private static final Method CREATE_TABLE_WITH_CONSTRAINTS = RequestObjectRouting.findIfaceMethod(
      "create_table_with_constraints",
      Table.class, List.class, List.class, List.class, List.class, List.class, List.class);
  private static final Method ALTER_TABLE_WITH_ENV = RequestObjectRouting.findIfaceMethod(
      "alter_table_with_environment_context",
      String.class, String.class, Table.class, EnvironmentContext.class);

  private record ValidatedFk(CatalogRouter.ResolvedNamespace pkNamespace, String fkCat, String pkDb) {
  }

  private final RoutingSupport support;
  private final IcebergTablePointerGuard icebergTablePointerGuard;

  CreateTableReqHandler(
      RoutingSupport support,
      IcebergTablePointerGuard icebergTablePointerGuard
  ) {
    this.support = support;
    this.icebergTablePointerGuard = icebergTablePointerGuard;
  }

  CreateTableReqHandler(
      RoutingSupport support,
      ExternalTableLocationRewriter ignored,
      IcebergTablePointerGuard icebergTablePointerGuard
  ) {
    this(support, icebergTablePointerGuard);
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    Object rawTable = ThriftReflectionCache.invokeGetter(request, "getTable");
    if (rawTable == null) {
      throw new MetaException("create_table_req requires a table");
    }
    String dbName = (String) ThriftReflectionCache.invokeGetter(rawTable, "getDbName");
    String tableName = (String) ThriftReflectionCache.invokeGetter(rawTable, "getTableName");
    String catName = ThriftReflectionCache.readString(rawTable, "getCatName", "getCatalogName");
    if (catName == null) {
      catName = RequestObjectRouting.readCatalogName(request);
    }

    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolve(support, "create_table_req", request, catName, dbName, tableName);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();
    support.validateCatalogAccess(backend, "create_table_req", namespace.backendDbName());

    if (!namespace.catalogName().equals(support.config.defaultCatalog())) {
      @SuppressWarnings("unchecked")
      Map<String, String> params = (Map<String, String>) ThriftReflectionCache.invokeGetter(rawTable, "getParameters");
      if (params != null && "true".equalsIgnoreCase(params.get("transactional"))) {
        throw new MetaException("Cannot create transactional table '"
            + dbName + "." + tableName
            + "' in non-default catalog '" + namespace.catalogName()
            + "'. Transactional (ACID) tables are only supported in the default catalog '"
            + support.config.defaultCatalog() + "'");
      }
    }

    List<ValidatedFk> validatedFks =
        validateForeignKeys(ThriftReflectionCache.invokeGetter(request, "getForeignKeys"), namespace);

    Object routedRequest = ThriftReflectionCache.deepCopy(request);
    Object routedTable = ThriftReflectionCache.invokeGetter(routedRequest, "getTable");
    if (routedTable != null) {
      Object internalizedTable =
          support.federationLayer.internalizeObjectArguments(new Object[]{routedTable}, namespace)[0];
      if (internalizedTable != routedTable) {
        invokeSetter(routedRequest, "setTable", internalizedTable);
      }
    }
    internalizeCreateTableRequest(routedRequest, namespace, catName, dbName, validatedFks);

    ExternalTableLocationRewriter externalTableLocationRewriter = support.externalTableLocationRewriter();
    if (externalTableLocationRewriter != null) {
      Object tableObj = ThriftReflectionCache.invokeGetter(routedRequest, "getTable");
      if (tableObj != null) {
        externalTableLocationRewriter.rewriteObjectArguments(new Object[]{tableObj}, namespace, "create_table_req");
      }
    }

    Object result = RequestObjectRouting.invokeWithUnsupportedFallback(
        support,
        backend,
        "create_table_req",
        routedRequest,
        () -> fallbackToLegacy(backend, routedRequest));

    if (tableName != null) {
      if (icebergTablePointerGuard != null) {
        icebergTablePointerGuard.invalidate(namespace.catalogName(), namespace.backendDbName(), tableName);
      }
      support.invalidateTable(namespace.catalogName(), namespace.backendDbName(), tableName);
    }

    return result;
  }

  private List<ValidatedFk> validateForeignKeys(
      Object fksObj,
      CatalogRouter.ResolvedNamespace namespace
  ) throws MetaException {
    List<ValidatedFk> validated = new ArrayList<>();
    if (!(fksObj instanceof List<?> list)) {
      return validated;
    }
    for (Object fk : list) {
      if (fk == null) {
        validated.add(null);
        continue;
      }
      String fkCat = ThriftReflectionCache.readString(fk, "getCatName", "getCatalogName");
      String pkDb = ThriftReflectionCache.readString(fk, "getPktable_db");
      if (pkDb != null && !pkDb.isBlank()) {
        CatalogRouter.ResolvedNamespace pkNamespace = support.resolveRequestNamespace(fkCat, pkDb);
        if (!pkNamespace.catalogName().equals(namespace.catalogName())) {
          throw new MetaException("Cannot create foreign key across different catalogs: FK catalog '"
              + namespace.catalogName() + "' and PK catalog '" + pkNamespace.catalogName() + "'");
        }
        validated.add(new ValidatedFk(pkNamespace, fkCat, pkDb));
      } else {
        validated.add(new ValidatedFk(null, fkCat, null));
      }
    }
    return validated;
  }

  private void internalizeCreateTableRequest(
      Object request,
      CatalogRouter.ResolvedNamespace namespace,
      String catName,
      String dbName,
      List<ValidatedFk> validatedFks
  ) {
    String backendDbName = namespace.backendDbName();
    boolean preserveBackendCatalogName = support.federationLayer.preserveBackendCatalogName();
    if (catName != null) {
      ThriftReflectionCache.invokeStringSetter(
          request,
          "setCatName",
          NamespaceTranslator.internalCatalogName(catName, dbName, namespace, preserveBackendCatalogName));
    }
    Object rawTable = ThriftReflectionCache.invokeGetter(request, "getTable");
    if (rawTable != null) {
      ThriftReflectionCache.invokeStringSetter(rawTable, "setDbName", backendDbName);
      if (catName != null) {
        ThriftReflectionCache.invokeStringSetter(
            rawTable,
            "setCatName",
            NamespaceTranslator.internalCatalogName(catName, dbName, namespace, preserveBackendCatalogName));
      }
    }
    internalizeConstraintList(
        ThriftReflectionCache.invokeGetter(request, "getPrimaryKeys"),
        "setTable_db",
        namespace,
        dbName,
        preserveBackendCatalogName);
    internalizeForeignKeys(
        ThriftReflectionCache.invokeGetter(request, "getForeignKeys"),
        namespace,
        dbName,
        preserveBackendCatalogName,
        validatedFks);
    internalizeConstraintList(
        ThriftReflectionCache.invokeGetter(request, "getUniqueConstraints"),
        "setTable_db",
        namespace,
        dbName,
        preserveBackendCatalogName);
    internalizeConstraintList(
        ThriftReflectionCache.invokeGetter(request, "getNotNullConstraints"),
        "setTable_db",
        namespace,
        dbName,
        preserveBackendCatalogName);
    internalizeConstraintList(
        ThriftReflectionCache.invokeGetter(request, "getDefaultConstraints"),
        "setTable_db",
        namespace,
        dbName,
        preserveBackendCatalogName);
    internalizeConstraintList(
        ThriftReflectionCache.invokeGetter(request, "getCheckConstraints"),
        "setTable_db",
        namespace,
        dbName,
        preserveBackendCatalogName);
  }

  private void internalizeForeignKeys(
      Object fksObj,
      CatalogRouter.ResolvedNamespace namespace,
      String dbName,
      boolean preserveBackendCatalogName,
      List<ValidatedFk> validatedFks
  ) {
    if (!(fksObj instanceof List<?> list)) {
      return;
    }
    for (int i = 0; i < list.size(); i++) {
      Object fk = list.get(i);
      if (fk == null) {
        continue;
      }
      ValidatedFk validated = i < validatedFks.size() ? validatedFks.get(i) : null;
      ThriftReflectionCache.invokeStringSetter(fk, "setFktable_db", namespace.backendDbName());
      if (validated != null && validated.pkNamespace() != null) {
        ThriftReflectionCache.invokeStringSetter(fk, "setPktable_db", validated.pkNamespace().backendDbName());
      }
      String fkCat = validated != null && validated.fkCat() != null
          ? validated.fkCat()
          : ThriftReflectionCache.readString(fk, "getCatName", "getCatalogName");
      if (fkCat != null) {
        CatalogRouter.ResolvedNamespace catNamespace =
            (validated != null && validated.pkNamespace() != null) ? validated.pkNamespace() : namespace;
        String targetDb = (validated != null && validated.pkDb() != null) ? validated.pkDb() : dbName;
        ThriftReflectionCache.invokeStringSetter(
            fk,
            "setCatName",
            NamespaceTranslator.internalCatalogName(fkCat, targetDb, catNamespace, preserveBackendCatalogName));
      }
    }
  }

  private void internalizeConstraintList(
      Object listObj,
      String setterName,
      CatalogRouter.ResolvedNamespace namespace,
      String dbName,
      boolean preserveBackendCatalogName
  ) {
    if (listObj instanceof List<?> list) {
      for (Object item : list) {
        if (item != null) {
          ThriftReflectionCache.invokeStringSetter(item, setterName, namespace.backendDbName());
          String itemCat = ThriftReflectionCache.readString(item, "getCatName", "getCatalogName");
          if (itemCat != null) {
            ThriftReflectionCache.invokeStringSetter(
                item,
                "setCatName",
                NamespaceTranslator.internalCatalogName(itemCat, dbName, namespace, preserveBackendCatalogName));
          }
        }
      }
    }
  }

  private static void invokeSetter(Object target, String setterName, Object value) {
    if (target == null || value == null) {
      return;
    }
    try {
      Method setter = target.getClass().getMethod(setterName, value.getClass());
      setter.invoke(target, value);
    } catch (NoSuchMethodException ignored) {
      // Request wrapper doesn't declare a matching setter for the internalized table object
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + setterName + " on " + target.getClass().getName(), e);
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
}
