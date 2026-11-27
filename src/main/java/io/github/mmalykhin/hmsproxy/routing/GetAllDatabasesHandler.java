package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
              (effectiveImpersonation, background) -> {
                @SuppressWarnings("unchecked")
                List<String> loaded = (List<String>) support.dispatcher.invokeDirect(
                    backend, method, null, effectiveImpersonation, background ? -1L : requestId, false, false);
                if (background && effectiveImpersonation == null && loaded != null
                    && support.router.hasMergeWithDefaultCatalogs()) {
                  String catalogName = backend.name();
                  if (catalogName.equals(support.router.defaultBackend().name())) {
                    support.router.updateDatabases(catalogName, loaded);
                  } else if (support.router.isMergeWithDefaultCatalog(catalogName)) {
                    support.router.updateDatabases(
                        catalogName, filterExposedDatabases(support, catalogName, loaded));
                  }
                }
                return loaded;
              });
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
    Map<String, List<String>> resultsByCatalog = new HashMap<>();
    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
      resultsByCatalog.put(fanoutResult.backend().name(), fanoutResult.value());
    }
    List<String> defaultDatabases = resultsByCatalog.get(defaultCatalog);
    if (defaultDatabases == null) {
      // Do not update merged catalogs when the default catalog did not respond in fanout;
      // otherwise default-owned names could be routed to a lower-priority merged catalog.
      return;
    }
    applyDatabaseList(support.router, defaultCatalog, defaultDatabases, replaceSnapshot);

    // Process merged catalogs strictly in declared priority order and stop if a higher-priority
    // merged catalog is missing from fanoutResults (degraded fanout), so lower-priority catalogs
    // cannot hijack shared database names.
    for (CatalogBackend backend : support.router.backends()) {
      String catalogName = backend.name();
      if (catalogName.equals(defaultCatalog) || !support.router.isMergeWithDefaultCatalog(catalogName)) {
        continue;
      }
      List<String> catalogDatabases = resultsByCatalog.get(catalogName);
      if (catalogDatabases == null) {
        break;
      }
      List<String> exposed = filterExposedDatabases(support, catalogName, catalogDatabases);
      applyDatabaseList(support.router, catalogName, exposed, replaceSnapshot);
    }
  }

  private static List<String> filterExposedDatabases(
      RoutingSupport support,
      String catalogName,
      List<String> databases
  ) {
    if (databases == null || databases.isEmpty()) {
      return List.of();
    }
    List<String> exposed = new ArrayList<>(databases.size());
    for (String db : databases) {
      if (db != null && support.federationLayer.isDatabaseExposed(catalogName, db)) {
        exposed.add(db);
      }
    }
    return exposed;
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
