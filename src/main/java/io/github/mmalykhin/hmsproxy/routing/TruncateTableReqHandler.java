package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class TruncateTableReqHandler implements SpecialCaseHandler {
  private static final Method TRUNCATE_TABLE =
      RequestObjectRouting.findIfaceMethod("truncate_table", String.class, String.class, List.class);

  private final RoutingSupport support;

  TruncateTableReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveWrite(support, "truncate_table_req", request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();

    Long writeId = (Long) ThriftReflectionCache.invokeGetter(request, "getWriteId");
    if (writeId != null && writeId > 0 && !namespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation 'truncate_table_req' is not supported for non-default catalog '"
          + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    String tblName = ThriftReflectionCache.readString(request, "getTableName");
    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object result = RequestObjectRouting.invokeWithUnsupportedFallback(
        support,
        backend,
        "truncate_table_req",
        routedRequest,
        () -> fallbackToLegacy(backend, routedRequest));
    if (tblName != null) {
      support.invalidateTable(namespace.catalogName(), namespace.backendDbName(), tblName);
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTableName");
    Object partNames = ThriftReflectionCache.invokeGetter(routedRequest, "getPartNames");
    List<String> partNamesList = (List<String>) partNames;
    support.invokeDirect(backend, TRUNCATE_TABLE, new Object[]{db, tbl, partNamesList});
    return null;
  }
}
