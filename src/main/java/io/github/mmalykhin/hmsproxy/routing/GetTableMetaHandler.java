package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.TableMeta;

final class GetTableMetaHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetTableMetaHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    String dbPattern = (String) args[0];
    String tablePattern = (String) args[1];
    @SuppressWarnings("unchecked")
    List<String> tableTypes = (List<String>) args[2];

    CatalogRouter.ResolvedNamespace resolved = support.router.resolvePattern(dbPattern).orElse(null);
    if (resolved != null) {
      RequestContext.currentObservation().recordNamespace(resolved);
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults = (List<TableMeta>) support.invokeDirect(
          resolved.backend(), method, new Object[]{resolved.backendDbName(), tablePattern, tableTypes});
      return support.filterExposed(
          method.getName(),
          resolved.catalogName(),
          "table",
          backendResults,
          result -> support.federationLayer.isTableExposed(
              resolved.catalogName(), result.getDbName(), result.getTableName()),
          result -> NamespaceTranslator.externalizeTableMeta(
              result,
              support.router.resolveCatalog(resolved.catalogName(), result.getDbName()),
              support.federationLayer.preserveBackendCatalogName()));
    }

    if (!support.router.canMatchRemoteCatalogs(dbPattern)) {
      CatalogBackend defaultBackend = support.router.defaultBackend();
      CatalogRouter.ResolvedNamespace defaultNamespace =
          support.router.resolveCatalog(defaultBackend.name(), dbPattern);
      RequestContext.currentObservation().recordNamespace(defaultNamespace);
      support.recordDefaultCatalogRouteIfImplicit(method.getName(), dbPattern, defaultNamespace);
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults = (List<TableMeta>) support.invokeDirect(
          defaultBackend, method, new Object[]{dbPattern, tablePattern, tableTypes});
      return support.filterExposed(
          method.getName(),
          defaultNamespace.catalogName(),
          "table",
          backendResults,
          result -> support.federationLayer.isTableExposed(
              defaultNamespace.catalogName(), result.getDbName(), result.getTableName()),
          result -> NamespaceTranslator.externalizeTableMeta(
              result,
              support.router.resolveCatalog(defaultNamespace.catalogName(), result.getDbName()),
              support.federationLayer.preserveBackendCatalogName()));
    }

    RequestContext.currentObservation().recordFanout();
    List<TableMeta> results = new ArrayList<>();
    for (FanoutExecutor.FanoutBackendResult<List<TableMeta>> fanoutResult : support.invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<TableMeta> result = (List<TableMeta>) support.dispatcher.invokeDirect(
              backend, method, new Object[]{dbPattern, tablePattern, tableTypes},
              impersonation, requestId, false, false);
          return result;
        })) {
      String catalogName = fanoutResult.backend().name();
      results.addAll(support.filterExposed(
          method.getName(),
          catalogName,
          "table",
          fanoutResult.value(),
          result -> support.federationLayer.isTableExposed(catalogName, result.getDbName(), result.getTableName()),
          result -> NamespaceTranslator.externalizeTableMeta(
              result,
              support.router.resolveCatalog(catalogName, result.getDbName()),
              support.federationLayer.preserveBackendCatalogName())));
    }
    return results;
  }
}
