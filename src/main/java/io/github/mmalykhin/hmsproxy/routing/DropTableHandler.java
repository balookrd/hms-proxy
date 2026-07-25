package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DropTableHandler implements SpecialCaseHandler, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DropTableHandler.class);

  private final RoutingSupport support;
  private final NamespaceFallback fallback;
  private final ExternalTableDropPurger externalTableDropPurger;
  private final ExternalTableDropPurgeExecutor purgeExecutor;

  DropTableHandler(
      RoutingSupport support,
      NamespaceFallback fallback,
      ExternalTableDropPurger externalTableDropPurger
  ) {
    this(support, fallback, externalTableDropPurger, new ExternalTableDropPurgeExecutor());
  }

  DropTableHandler(
      RoutingSupport support,
      NamespaceFallback fallback,
      ExternalTableDropPurger externalTableDropPurger,
      ExternalTableDropPurgeExecutor purgeExecutor
  ) {
    this.support = support;
    this.fallback = fallback;
    this.externalTableDropPurger = externalTableDropPurger;
    this.purgeExecutor = purgeExecutor;
  }

  @Override
  public void close() {
    purgeExecutor.close();
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
      // TOCTOU: a concurrent alter_table can move the location between this read and the drop, so
      // the purge may target the previous location. Left as is because the proxy has no lock that
      // spans both calls; the allowlist in the purger still bounds what can be deleted.
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

  /**
   * Hands the purge to a background worker: a recursive delete of a large table location can take
   * minutes and would otherwise hold the Thrift worker for the whole drop_table call. The purge was
   * already best-effort and log-only, so nothing that used to reach the client is lost — the only
   * change is that the drop_table response no longer implies the data is gone yet.
   */
  private void runBestEffortDropPurge(
      CatalogRouter.ResolvedNamespace namespace,
      Optional<ExternalTableDropPurger.PurgeRequest> purgeRequest
  ) {
    if (purgeRequest.isEmpty()) {
      return;
    }
    ExternalTableDropPurger.PurgeRequest request = purgeRequest.get();
    CatalogBackend backend = namespace.backend();
    String catalogName = namespace.catalogName();
    long requestId = RequestContext.currentRequestId();
    purgeExecutor.submit(catalogName, request.location(), () -> {
      RequestContext.REQUEST_ID.set(requestId);
      try {
        externalTableDropPurger.purge(backend, request);
      } catch (Exception exception) {
        LOG.warn(
            "requestId={} external-table purge failed after successful drop for catalog '{}' location='{}': {}",
            requestId,
            catalogName,
            request.location(),
            exception.toString(),
            exception);
      } finally {
        RequestContext.REQUEST_ID.remove();
      }
    });
  }
}
