package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class GetAllDatabasesHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetAllDatabasesHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    RequestContext.currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    List<FanoutExecutor.FanoutBackendResult<List<String>>> fanoutResults = support.invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<String> result = support.databaseListCache.get(
              method.getName(),
              backend.name(),
              null,
              impersonation,
              (effectiveImpersonation, background) -> (List<String>) support.dispatcher.invokeDirect(
                  backend, method, null, effectiveImpersonation, background ? -1L : requestId, false, false));
          return result;
        });

    if (support.router.hasMergeWithDefaultCatalogs()) {
      updateMergedRoutingTable(support, fanoutResults, true);
    }

    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
      databases.addAll(support.exposedDatabaseNames(
          method.getName(), fanoutResult.backend().name(), fanoutResult.value()));
    }
    return databases;
  }

  static void updateMergedRoutingTable(
      RoutingSupport support,
      List<FanoutExecutor.FanoutBackendResult<List<String>>> fanoutResults,
      boolean fullWildcardPattern
  ) throws MetaException {
    boolean replaceSnapshot = fullWildcardPattern && !isUserScopedListing(support);
    String defaultCatalog = support.router.defaultBackend().name();
    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
      if (fanoutResult.backend().name().equals(defaultCatalog)) {
        applyDatabaseList(support.router, defaultCatalog, fanoutResult.value(), replaceSnapshot);
      }
    }
    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
      if (!fanoutResult.backend().name().equals(defaultCatalog)) {
        applyDatabaseList(support.router, fanoutResult.backend().name(), fanoutResult.value(), replaceSnapshot);
      }
    }
  }

  private static void applyDatabaseList(
      CatalogRouter router,
      String catalogName,
      List<String> databases,
      boolean replaceSnapshot
  ) {
    if (replaceSnapshot) {
      router.updateDatabases(catalogName, databases);
    } else {
      router.mergeDatabases(catalogName, databases);
    }
  }

  private static boolean isUserScopedListing(RoutingSupport support) throws MetaException {
    return support.impersonationResolver != null
        && !support.isServicePrincipalBypass()
        && support.impersonationResolver.resolve().isPresent();
  }
}
