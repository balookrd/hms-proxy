package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
      Object result = support.invokeBackendNamed(defaultBackend, "set_aggr_stats_for", request);
      invalidateColStats(colStats, null);
      return result;
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("set_aggr_stats_for", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "set_aggr_stats_for", namespace.backendDbName());

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object result = support.invokeBackendNamed(backend, "set_aggr_stats_for", routedRequest);
    Object colStats = ThriftReflectionCache.invokeGetter(routedRequest, "getColStats");
    invalidateColStats(colStats, namespace);
    String requestTable = RoutingSupport.extractTableName(routedRequest);
    if (requestTable != null) {
      support.invalidateTable(namespace.catalogName(), namespace.backendDbName(), requestTable);
    }
    return result;
  }

  private void invalidateColStats(Object colStats, CatalogRouter.ResolvedNamespace fixedNamespace) {
    if (!(colStats instanceof Iterable<?> items)) {
      return;
    }
    Set<String> invalidated = new HashSet<>();
    for (Object stat : items) {
      if (stat == null) {
        continue;
      }
      Object statsDesc = ThriftReflectionCache.invokeGetter(stat, "getStatsDesc");
      String tableName = statsDesc != null
          ? RoutingSupport.extractTableName(statsDesc)
          : RoutingSupport.extractTableName(stat);
      if (tableName == null) {
        continue;
      }
      String catalogName;
      String backendDbName;
      if (fixedNamespace != null) {
        catalogName = fixedNamespace.catalogName();
        backendDbName = fixedNamespace.backendDbName();
      } else {
        String statDb = statsDesc != null ? NamespaceTranslator.extractDbName(statsDesc) : null;
        if (statDb != null) {
          try {
            CatalogRouter.ResolvedNamespace resolved = support.router.resolveDatabase(statDb);
            catalogName = resolved.catalogName();
            backendDbName = resolved.backendDbName();
          } catch (Exception ignored) {
            catalogName = support.config.defaultCatalog();
            backendDbName = statDb;
          }
        } else {
          continue;
        }
      }
      String dedupeKey = (catalogName + '\0' + backendDbName + '\0' + tableName).toLowerCase(Locale.ROOT);
      if (invalidated.add(dedupeKey)) {
        support.invalidateTable(catalogName, backendDbName, tableName);
      }
    }
  }
}
