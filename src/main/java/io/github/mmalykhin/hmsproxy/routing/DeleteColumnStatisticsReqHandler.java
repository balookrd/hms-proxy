package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class DeleteColumnStatisticsReqHandler implements SpecialCaseHandler {
  private static final Method DELETE_TABLE_COL_STATS = findMethod("delete_table_column_statistics",
      String.class, String.class, String.class);
  private static final Method DELETE_PART_COL_STATS = findMethod("delete_partition_column_statistics",
      String.class, String.class, String.class, String.class);

  private final RoutingSupport support;

  DeleteColumnStatisticsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catName = ThriftReflectionCache.readString(request, "getCat_name", "getCatName", "getCatalogName");
    String dbName = ThriftReflectionCache.readString(request, "getDb_name", "getDbName");
    if (dbName == null) {
      throw new MetaException("delete_column_statistics_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("delete_column_statistics_req", catName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "delete_column_statistics_req", namespace.backendDbName());

    String tblName = RoutingSupport.extractTableName(request);
    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    if (routedRequest == request) {
      routedRequest = ThriftReflectionCache.deepCopy(request);
    }
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDb_name", namespace.backendDbName());
    if (catName != null) {
      String internalizedCat = NamespaceTranslator.internalCatalogName(
          catName, dbName, namespace, support.federationLayer.preserveBackendCatalogName());
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setCat_name", internalizedCat);
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatName", internalizedCat);
    }

    Object result;
    try {
      result = support.invokeBackendNamed(backend, "delete_column_statistics_req", routedRequest);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        result = fallbackToLegacy(backend, routedRequest, namespace.backendDbName());
      } else {
        throw cause;
      }
    }
    if (tblName != null) {
      support.invalidateTable(namespace.catalogName(), namespace.backendDbName(), tblName);
    }
    return result;
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest, String backendDb) throws Throwable {
    String tblName = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTbl_name");
    Boolean tableLevel = (Boolean) ThriftReflectionCache.invokeGetter(routedRequest, "isTableLevel");
    @SuppressWarnings("unchecked")
    List<String> colNames = (List<String>) ThriftReflectionCache.invokeGetter(routedRequest, "getCol_names");
    @SuppressWarnings("unchecked")
    List<String> partNames = (List<String>) ThriftReflectionCache.invokeGetter(routedRequest, "getPart_names");

    boolean allSuccess = true;
    if (Boolean.TRUE.equals(tableLevel) || partNames == null || partNames.isEmpty()) {
      if (colNames != null) {
        for (String col : colNames) {
          Object res = support.invokeDirect(backend, DELETE_TABLE_COL_STATS, new Object[]{backendDb, tblName, col});
          if (res instanceof Boolean b && !b) {
            allSuccess = false;
          }
        }
      }
    } else {
      if (colNames != null) {
        for (String part : partNames) {
          for (String col : colNames) {
            Object res = support.invokeDirect(backend, DELETE_PART_COL_STATS, new Object[]{backendDb, tblName, part, col});
            if (res instanceof Boolean b && !b) {
              allSuccess = false;
            }
          }
        }
      }
    }
    return allSuccess;
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
