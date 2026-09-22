package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

    if (!support.router.canMatchRemoteCatalogs(pattern)) {
      CatalogBackend defaultBackend = support.router.defaultBackend();
      CatalogRouter.ResolvedNamespace defaultNamespace =
          support.router.resolveCatalog(defaultBackend.name(), pattern);
      RequestContext.currentObservation().recordNamespace(defaultNamespace);
      support.recordDefaultCatalogRouteIfImplicit(method.getName(), pattern, defaultNamespace);
      ImpersonationContext impersonation = support.impersonationResolver.resolve().orElse(null);
      @SuppressWarnings("unchecked")
      List<String> backendDatabases = support.databaseListCache.get(
          method.getName(),
          defaultBackend.name(),
          pattern,
          impersonation,
          () -> (List<String>) support.dispatcher.invokeDirect(
              defaultBackend,
              method,
              new Object[]{pattern},
              impersonation,
              RequestContext.currentRequestId(),
              true,
              true));
      return support.exposedDatabaseNames(method.getName(), defaultNamespace.catalogName(), backendDatabases);
    }

    RequestContext.currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (FanoutExecutor.FanoutBackendResult<List<String>> fanoutResult : support.invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          Optional<String> backendPatternOpt = support.router.backendDatabasePattern(backend.name(), pattern);
          if (backendPatternOpt.isEmpty()) {
            return List.<String>of();
          }
          String backendPattern = backendPatternOpt.get();
          @SuppressWarnings("unchecked")
          List<String> result = support.databaseListCache.get(
              method.getName(),
              backend.name(),
              backendPattern,
              impersonation,
              () -> (List<String>) support.dispatcher.invokeDirect(
                  backend, method, new Object[]{backendPattern}, impersonation, requestId, false, false));
          return result;
        })) {
      databases.addAll(support.exposedDatabaseNames(
          method.getName(), fanoutResult.backend().name(), fanoutResult.value()));
    }
    if (pattern != null && !pattern.isBlank() && !"*".equals(pattern) && !".*".equals(pattern)) {
      databases.removeIf(db -> !CatalogRouter.matchesHivePattern(db, pattern));
    }
    return databases;
  }
}
