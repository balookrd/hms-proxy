package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;

final class TableStatisticsReqHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  TableStatisticsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveRead(support, method, request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();
    support.validateCatalogAccess(backend, method.getName(), namespace.backendDbName());

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object result = support.invokeBackendNamed(backend, method.getName(), routedRequest);
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
