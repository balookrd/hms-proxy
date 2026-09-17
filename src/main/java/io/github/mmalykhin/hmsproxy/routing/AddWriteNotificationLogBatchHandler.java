package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class AddWriteNotificationLogBatchHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  AddWriteNotificationLogBatchHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = ThriftReflectionCache.readString(request, "getDb");
    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("add_write_notification_log_in_batch", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "add_write_notification_log_in_batch", namespace.backendDbName());
    if (!backend.runtimeProfile().isHive4()) {
      throw new MetaException(
          "add_write_notification_log_in_batch requires a Hive 4 backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    return support.invokeBackendNamed(backend, "add_write_notification_log_in_batch", routedRequest);
  }
}
