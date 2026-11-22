package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.List;

final class DeleteColumnStatisticsReqHandler implements SpecialCaseHandler {
  private static final Method DELETE_TABLE_COL_STATS = RequestObjectRouting.findIfaceMethod(
      "delete_table_column_statistics", String.class, String.class, String.class);
  private static final Method DELETE_PART_COL_STATS = RequestObjectRouting.findIfaceMethod(
      "delete_partition_column_statistics", String.class, String.class, String.class, String.class);

  private final RoutingSupport support;

  DeleteColumnStatisticsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveWrite(support, "delete_column_statistics_req", request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();
    String catName = resolved.catName();
    String dbName = resolved.dbName();
    String tblName = resolved.tableName();

    Object internalized = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object routedRequest = internalized == request ? ThriftReflectionCache.deepCopy(request) : internalized;
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDb_name", namespace.backendDbName());
    if (catName != null) {
      String internalizedCat = NamespaceTranslator.internalCatalogName(
          catName, dbName, namespace, support.federationLayer.preserveBackendCatalogName());
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setCat_name", internalizedCat);
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatName", internalizedCat);
    }

    Object result = RequestObjectRouting.invokeWithUnsupportedFallback(
        support,
        backend,
        "delete_column_statistics_req",
        routedRequest,
        () -> fallbackToLegacy(backend, routedRequest, namespace.backendDbName()));
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
}
