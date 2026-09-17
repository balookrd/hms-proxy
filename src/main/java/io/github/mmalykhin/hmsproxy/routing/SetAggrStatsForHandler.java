package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.List;

final class SetAggrStatsForHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  SetAggrStatsForHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      Object colStats = ThriftReflectionCache.invokeGetter(request, "getColStats");
      if (colStats == null || (colStats instanceof List<?> list && list.isEmpty())) {
        return Boolean.TRUE;
      }
      CatalogBackend defaultBackend = support.router.defaultBackend();
      RequestContext.currentObservation().recordNamespace(
          support.router.resolveCatalog(support.config.defaultCatalog(), ""));
      RequestContext.currentObservation().markDefaultCatalogRoute();
      support.observability.metrics().recordDefaultCatalogRoute("set_aggr_stats_for");
      support.validateCatalogAccess(defaultBackend, "set_aggr_stats_for", null);
      return support.invokeBackendNamed(defaultBackend, "set_aggr_stats_for", request);
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("set_aggr_stats_for", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "set_aggr_stats_for", namespace.backendDbName());

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    return support.invokeBackendNamed(backend, "set_aggr_stats_for", routedRequest);
  }
}
