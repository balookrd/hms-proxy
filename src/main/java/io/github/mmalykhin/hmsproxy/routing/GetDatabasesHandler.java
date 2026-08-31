package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

final class GetDatabasesHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetDatabasesHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    String pattern = (String) args[0];
    CatalogRouter.ResolvedNamespace resolved = support.router.resolvePattern(pattern).orElse(null);
    if (resolved != null) {
      RequestContext.currentObservation().recordNamespace(resolved);
      ImpersonationContext impersonation = support.impersonationResolver.resolve().orElse(null);
      @SuppressWarnings("unchecked")
      List<String> backendDatabases = support.databaseListCache.get(
          method.getName(),
          resolved.backend().name(),
          resolved.backendDbName(),
          impersonation,
          () -> (List<String>) support.dispatcher.invokeDirect(
              resolved.backend(),
              method,
              new Object[]{resolved.backendDbName()},
              impersonation,
              RequestContext.currentRequestId(),
              true,
              true));
      return support.exposedDatabaseNames(method.getName(), resolved.catalogName(), backendDatabases);
    }

    RequestContext.currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : support.invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<String> result = support.databaseListCache.get(
              method.getName(),
              backend.name(),
              pattern,
              impersonation,
              () -> (List<String>) support.dispatcher.invokeDirect(
                  backend, method, new Object[]{pattern}, impersonation, requestId, false, false));
          return result;
        })) {
      databases.addAll(support.exposedDatabaseNames(
          method.getName(), fanoutResult.backend().name(), fanoutResult.value()));
    }
    return databases;
  }
}
