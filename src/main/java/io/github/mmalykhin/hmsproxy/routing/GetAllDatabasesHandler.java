package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

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
              () -> (List<String>) support.dispatcher.invokeDirect(
                  backend, method, null, impersonation, requestId, false, false));
          return result;
        });

    if (support.router.hasMergeWithDefaultCatalogs()) {
      for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
        if (fanoutResult.backend().name().equals(support.router.defaultBackend().name())) {
          support.router.updateDatabases(fanoutResult.backend().name(), fanoutResult.value());
        }
      }
      for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
        if (!fanoutResult.backend().name().equals(support.router.defaultBackend().name())) {
          support.router.updateDatabases(fanoutResult.backend().name(), fanoutResult.value());
        }
      }
    }

    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : fanoutResults) {
      databases.addAll(support.exposedDatabaseNames(
          method.getName(), fanoutResult.backend().name(), fanoutResult.value()));
    }
    return databases;
  }
}
