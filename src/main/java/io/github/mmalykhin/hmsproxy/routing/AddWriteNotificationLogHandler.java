package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class AddWriteNotificationLogHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  AddWriteNotificationLogHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = ThriftReflectionCache.readString(request, "getDb");
    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("add_write_notification_log", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "add_write_notification_log", namespace.backendDbName());
    if (!backend.runtimeProfile().isHortonworks()) {
      throw new MetaException(
          "Hortonworks add_write_notification_log requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    Object routedRequest = ThriftReflectionCache.deepCopy(request);
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDb", namespace.backendDbName());
    return support.invokeBackendNamed(backend, "add_write_notification_log", routedRequest);
  }
}
