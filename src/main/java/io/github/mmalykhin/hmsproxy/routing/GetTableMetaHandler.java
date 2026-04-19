package io.github.mmalykhin.hmsproxy.routing;

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
      List<TableMeta> results = new ArrayList<>();
      for (TableMeta result : backendResults) {
        if (!support.federationLayer.isTableExposed(resolved, result.getTableName())) {
          support.recordFilteredObject(method.getName(), resolved.catalogName(), "table");
          continue;
        }
        results.add(support.federationLayer.externalizeTableMeta(result, resolved));
      }
      return results;
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
      List<TableMeta> backendResults = fanoutResult.value();
      for (TableMeta result : backendResults) {
        if (!support.federationLayer.isTableExposed(
            fanoutResult.backend().name(),
            result.getDbName(),
            result.getTableName())) {
          support.recordFilteredObject(method.getName(), fanoutResult.backend().name(), "table");
          continue;
        }
        results.add(NamespaceTranslator.externalizeTableMeta(
            result,
            support.router.resolveCatalog(fanoutResult.backend().name(), result.getDbName()),
            support.federationLayer.preserveBackendCatalogName()));
      }
    }
    return results;
  }
}
