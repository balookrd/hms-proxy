package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;

final class GetTableReqHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetTableReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    GetTableRequest request = (GetTableRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = support.federationLayer.resolveRequestNamespace(
        request.getCatName(), request.getDbName());
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), request.getCatName(), request.getDbName(), namespace);
    CatalogBackend backend = namespace.backend();
    support.validateExposedDatabaseAccess(method.getName(), namespace);
    support.validateExposedTableAccess(method.getName(), namespace, request.getTblName());
    GetTableRequest routedRequest =
        (GetTableRequest) support.federationLayer.internalizeTableRequest(request, namespace);
    Object result = support.invokeViaRequest(backend, routedRequest, method.getName());
    result = support.filterSingleTableResult(method.getName(), namespace, result);
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
