package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class TableStatisticsReqHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  TableStatisticsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      throw new MetaException(method.getName() + " requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, method.getName(), namespace.backendDbName());

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object result = support.invokeBackendNamed(backend, method.getName(), routedRequest);
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
