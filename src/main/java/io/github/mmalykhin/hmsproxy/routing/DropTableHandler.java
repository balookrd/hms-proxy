package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DropTableHandler implements SpecialCaseHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DropTableHandler.class);

  private final RoutingSupport support;
  private final NamespaceFallback fallback;
  private final ExternalTableDropPurger externalTableDropPurger;

  DropTableHandler(
      RoutingSupport support,
      NamespaceFallback fallback,
      ExternalTableDropPurger externalTableDropPurger
  ) {
    this.support = support;
    this.fallback = fallback;
    this.externalTableDropPurger = externalTableDropPurger;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    if (args == null || args.length < 2 || !(args[0] instanceof String dbName) || !(args[1] instanceof String)) {
      return fallback.routeByNamespaceOrFail(method, args);
    }
    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    support.validateCatalogAccess(namespace.backend(), method.getName(), namespace.backendDbName());
    Object[] routedArgs = support.federationLayer.internalizeDbStringArguments(args, namespace);

    Optional<ExternalTableDropPurger.PurgeRequest> purgeRequest = Optional.empty();
    if (externalTableDropPurger.enabledFor(namespace.backend())) {
      purgeRequest = prepareDropPurgeRequest(namespace, routedArgs);
    }

    Object result = support.invokeDirect(namespace.backend(), method, routedArgs);
    runBestEffortDropPurge(namespace, purgeRequest);
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private Optional<ExternalTableDropPurger.PurgeRequest> prepareDropPurgeRequest(
      CatalogRouter.ResolvedNamespace namespace,
      Object[] routedArgs
  ) {
    if (routedArgs.length < 2 || !(routedArgs[0] instanceof String backendDbName)
        || !(routedArgs[1] instanceof String tableName)) {
      return Optional.empty();
    }
    try {
      Table existingTable = (Table) support.invokeByReflection(
          namespace.backend(),
          "get_table",
          new Class<?>[] {String.class, String.class},
          new Object[] {backendDbName, tableName});
      return externalTableDropPurger.prepare(namespace.backend(), existingTable);
    } catch (Throwable throwable) {
      LOG.warn(
          "requestId={} unable to prepare external-table purge for catalog '{}' db='{}' table='{}': {}",
          RequestContext.currentRequestId(),
          namespace.catalogName(),
          backendDbName,
          tableName,
          throwable.toString());
      return Optional.empty();
    }
  }

  private void runBestEffortDropPurge(
      CatalogRouter.ResolvedNamespace namespace,
      Optional<ExternalTableDropPurger.PurgeRequest> purgeRequest
  ) {
    if (purgeRequest.isEmpty()) {
      return;
    }
    try {
      externalTableDropPurger.purge(namespace.backend(), purgeRequest.get());
    } catch (Exception exception) {
      LOG.warn(
          "requestId={} external-table purge failed after successful drop for catalog '{}' location='{}': {}",
          RequestContext.currentRequestId(),
          namespace.catalogName(),
          purgeRequest.get().location(),
          exception.toString(),
          exception);
    }
  }
}
