package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;

final class GetTablesReqHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetTablesReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catName = ThriftReflectionCache.readString(request, "catName", "getCatName");
    String dbName = ThriftReflectionCache.readString(request, "dbName", "getDbName");

    CatalogRouter.ResolvedNamespace namespace = support.federationLayer.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), catName, dbName, namespace);
    if (!support.isDatabaseAccessible(method.getName(), namespace)) {
      return new GetTablesResult(new java.util.ArrayList<>());
    }
    CatalogBackend backend = namespace.backend();

    Object result;
    if (backend.runtimeProfile().isHive4() && !(request instanceof GetTablesRequest)) {
      Object routedRequest = support.federationLayer.internalizeArgument(request, namespace);
      result = support.invokeBackendNamed(backend, method.getName(), routedRequest);
    } else {
      GetTablesRequest apacheReq = request instanceof GetTablesRequest standardReq
          ? standardReq
          : (GetTablesRequest) io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter.convertTBase(request, GetTablesRequest.class);
      GetTablesRequest routedRequest =
          (GetTablesRequest) support.federationLayer.internalizeTablesRequest(apacheReq, namespace);
      result = support.invokeViaRequest(backend, routedRequest, method.getName());
    }
    result = support.filterTableCollectionResult(method.getName(), namespace, result);
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
