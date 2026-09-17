package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;

final class GetTableReqHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetTableReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catName = ThriftReflectionCache.readString(request, "catName", "getCatName");
    String dbName = ThriftReflectionCache.readString(request, "dbName", "getDbName");
    String tblName = ThriftReflectionCache.readString(request, "tblName", "getTblName");

    CatalogRouter.ResolvedNamespace namespace = support.federationLayer.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), catName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateExposedDatabaseAccess(method.getName(), namespace);
    support.validateExposedTableAccess(method.getName(), namespace, tblName);

    Object result;
    if (backend.runtimeProfile().isHive4() && !(request instanceof GetTableRequest)) {
      Object routedRequest = support.federationLayer.internalizeArgument(request, namespace);
      result = support.invokeBackendNamed(backend, "get_table_req", routedRequest);
    } else {
      GetTableRequest apacheReq = request instanceof GetTableRequest standardReq
          ? standardReq
          : (GetTableRequest) ThriftValueConverter.convertTBase(request, GetTableRequest.class);
      GetTableRequest routedRequest =
          (GetTableRequest) support.federationLayer.internalizeTableRequest(apacheReq, namespace);
      result = support.invokeViaRequest(backend, routedRequest, method.getName());
    }

    result = support.filterSingleTableResult(method.getName(), namespace, result);
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
