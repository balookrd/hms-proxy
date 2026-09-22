package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class AlterTableHandler implements SpecialCaseHandler {
  private static final Method ALTER_TABLE = findMethod("alter_table", String.class, String.class, Table.class);
  private static final Method ALTER_TABLE_WITH_ENV = findMethod("alter_table_with_environment_context",
      String.class, String.class, Table.class, EnvironmentContext.class);

  private final RoutingSupport support;
  private final IcebergTablePointerGuard icebergTablePointerGuard;
  private final ExternalTableLocationRewriter externalTableLocationRewriter;

  AlterTableHandler(
      RoutingSupport support,
      IcebergTablePointerGuard icebergTablePointerGuard,
      ExternalTableLocationRewriter externalTableLocationRewriter
  ) {
    this.support = support;
    this.icebergTablePointerGuard = icebergTablePointerGuard;
    this.externalTableLocationRewriter = externalTableLocationRewriter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object handle(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    boolean isReq = "alter_table_req".equals(methodName);

    String sourceDb;
    String sourceTable;
    Object rawTable;
    Long writeId = null;
    Object request = null;

    if (isReq) {
      if (args == null || args.length == 0 || args[0] == null) {
        throw new MetaException("alter_table_req requires a non-null request argument");
      }
      request = args[0];
      sourceDb = NamespaceTranslator.extractDbName(request);
      if (sourceDb == null) {
        throw new MetaException("alter_table_req requires a target database");
      }
      sourceTable = ThriftReflectionCache.readString(request, "getTableName");
      rawTable = ThriftReflectionCache.invokeGetter(request, "getTable");
      writeId = (Long) ThriftReflectionCache.invokeGetter(request, "getWriteId");
    } else {
      if (args == null || args.length < 3) {
        throw new MetaException(methodName + " requires at least database, table name, and Table arguments");
      }
      sourceDb = args[0] instanceof String s ? s : null;
      if (sourceDb == null) {
        throw new MetaException(methodName + " requires a database name");
      }
      sourceTable = args[1] instanceof String s ? s : null;
      rawTable = args[2];
    }

    String targetDb = rawTable != null ? ThriftReflectionCache.readString(rawTable, "getDbName") : null;
    if (targetDb == null || targetDb.isBlank()) {
      targetDb = sourceDb;
    }
    String targetTable = rawTable != null ? ThriftReflectionCache.readString(rawTable, "getTableName") : null;
    if (targetTable == null || targetTable.isBlank()) {
      targetTable = sourceTable;
    }

    CatalogRouter.ResolvedNamespace sourceNamespace = support.router.resolveDatabase(sourceDb);
    CatalogRouter.ResolvedNamespace targetNamespace = support.router.resolveDatabase(targetDb);

    if (!sourceNamespace.catalogName().equals(targetNamespace.catalogName())) {
      throw new MetaException("Cannot rename/move table across different catalogs: source catalog '"
          + sourceNamespace.catalogName() + "' and target catalog '" + targetNamespace.catalogName() + "'");
    }

    RequestContext.currentObservation().recordNamespace(sourceNamespace);
    support.recordDefaultCatalogRouteIfImplicit(methodName, sourceDb, sourceNamespace);
    CatalogBackend backend = sourceNamespace.backend();
    support.validateCatalogAccess(backend, methodName, sourceNamespace.backendDbName());
    if (!sourceNamespace.backendDbName().equalsIgnoreCase(targetNamespace.backendDbName())) {
      support.validateCatalogAccess(backend, methodName, targetNamespace.backendDbName());
    }

    if (writeId != null && writeId > 0 && !sourceNamespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation '" + methodName + "' is not supported for non-default catalog '"
          + sourceNamespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    if (!sourceNamespace.catalogName().equals(support.config.defaultCatalog()) && rawTable != null) {
      Map<String, String> params = (Map<String, String>) ThriftReflectionCache.invokeGetter(rawTable, "getParameters");
      if (params != null && "true".equalsIgnoreCase(params.get("transactional"))) {
        throw new MetaException("Transactional table creation or alter is not allowed on non-default catalog '"
            + sourceNamespace.catalogName() + "'");
      }
    }

    Object internalizedTable = null;
    if (rawTable != null) {
      internalizedTable = support.federationLayer.internalizeArgument(rawTable, targetNamespace);
      if (externalTableLocationRewriter != null) {
        externalTableLocationRewriter.rewriteObjectArguments(new Object[]{internalizedTable}, targetNamespace, methodName);
      }
    }

    Object result;
    if (isReq) {
      Object routedRequest = ThriftReflectionCache.deepCopy(request);
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setDbName", sourceNamespace.backendDbName());
      String catName = ThriftReflectionCache.readString(request, "getCatName");
      if (catName != null) {
        ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatName",
            NamespaceTranslator.internalCatalogName(catName, sourceDb, sourceNamespace,
                support.federationLayer.preserveBackendCatalogName()));
      }
      String validWriteIdList = ThriftReflectionCache.readString(request, "getValidWriteIdList");
      if (validWriteIdList != null) {
        ThriftReflectionCache.invokeStringSetter(routedRequest, "setValidWriteIdList",
            NamespaceInternalizer.transformValidWriteIdList(validWriteIdList, sourceNamespace));
      }
      if (internalizedTable != null) {
        setTableOnRequest(routedRequest, internalizedTable);
      }

      Object[] guardArgs = new Object[]{
          sourceNamespace.backendDbName(),
          sourceTable,
          internalizedTable != null ? internalizedTable : rawTable,
          routedRequest
      };

      try (IcebergTablePointerGuard.Protection ignored =
               icebergTablePointerGuard != null ? icebergTablePointerGuard.protect(guardArgs, sourceNamespace, methodName) : null) {
        try {
          result = support.invokeBackendNamed(backend, "alter_table_req", routedRequest);
        } catch (Throwable cause) {
          if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
            result = fallbackToLegacy(backend, routedRequest);
          } else {
            throw cause;
          }
        }
      }
    } else {
      Object[] routedArgs = Arrays.copyOf(args, args.length);
      routedArgs[0] = sourceNamespace.backendDbName();
      routedArgs[1] = sourceTable;
      routedArgs[2] = internalizedTable != null ? internalizedTable : rawTable;

      try (IcebergTablePointerGuard.Protection ignored =
               icebergTablePointerGuard != null ? icebergTablePointerGuard.protect(routedArgs, sourceNamespace, methodName) : null) {
        result = support.invokeDirect(backend, method, routedArgs);
      }
    }

    if (icebergTablePointerGuard != null) {
      boolean isRename = !sourceNamespace.backendDbName().equalsIgnoreCase(targetNamespace.backendDbName())
          || (targetTable != null && !targetTable.equalsIgnoreCase(sourceTable));
      if (isRename) {
        icebergTablePointerGuard.invalidate(sourceNamespace.catalogName(), sourceNamespace.backendDbName(), sourceTable);
        if (targetTable != null) {
          icebergTablePointerGuard.invalidate(targetNamespace.catalogName(), targetNamespace.backendDbName(), targetTable);
        }
      }
    }

    if (support.tableMetadataCache != null) {
      support.tableMetadataCache.invalidateTable(sourceNamespace.catalogName(), sourceNamespace.backendDbName(), sourceTable);
      if (targetTable != null && !targetTable.equalsIgnoreCase(sourceTable)) {
        support.tableMetadataCache.invalidateTable(targetNamespace.catalogName(), targetNamespace.backendDbName(), targetTable);
      }
    }
    if (support.partitionMetadataCache != null) {
      support.partitionMetadataCache.invalidateTable(sourceNamespace.catalogName(), sourceNamespace.backendDbName(), sourceTable);
      if (targetTable != null && !targetTable.equalsIgnoreCase(sourceTable)) {
        support.partitionMetadataCache.invalidateTable(targetNamespace.catalogName(), targetNamespace.backendDbName(), targetTable);
      }
    }

    return result;
  }

  private static void setTableOnRequest(Object request, Object table) {
    if (request == null || table == null) {
      return;
    }
    for (Method method : request.getClass().getMethods()) {
      if ("setTable".equals(method.getName()) && method.getParameterCount() == 1) {
        Class<?> paramType = method.getParameterTypes()[0];
        if (paramType.isInstance(table)) {
          try {
            method.invoke(request, table);
            return;
          } catch (Exception e) {
            throw new IllegalStateException("Failed to invoke setTable on " + request.getClass().getName(), e);
          }
        }
      }
    }
    for (Method method : request.getClass().getMethods()) {
      if ("setTable".equals(method.getName()) && method.getParameterCount() == 1) {
        try {
          Object converted = ThriftValueConverter.convertTBase(table, method.getParameterTypes()[0]);
          method.invoke(request, converted);
          return;
        } catch (Exception e) {
          throw new IllegalStateException("Failed to invoke setTable with conversion on " + request.getClass().getName(), e);
        }
      }
    }
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTableName");
    Object rawTable = ThriftReflectionCache.invokeGetter(routedRequest, "getTable");
    Object rawEnv = ThriftReflectionCache.invokeGetter(routedRequest, "getEnvironmentContext");

    Table apacheTable = (Table) ThriftValueConverter.convertTBase(rawTable, Table.class);
    EnvironmentContext apacheEnv = rawEnv == null ? null : (EnvironmentContext) ThriftValueConverter.convertTBase(rawEnv, EnvironmentContext.class);

    if (apacheEnv != null) {
      support.invokeDirect(backend, ALTER_TABLE_WITH_ENV, new Object[]{db, tbl, apacheTable, apacheEnv});
    } else {
      support.invokeDirect(backend, ALTER_TABLE, new Object[]{db, tbl, apacheTable});
    }
    return null;
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
