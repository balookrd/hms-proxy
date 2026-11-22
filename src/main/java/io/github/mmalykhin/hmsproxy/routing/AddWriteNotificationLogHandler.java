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
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveWrite(support, "add_write_notification_log", request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();
    String catName = resolved.catName();
    String dbName = resolved.dbName();
    if (!backend.runtimeProfile().isHortonworks() && !backend.runtimeProfile().isHive4()) {
      throw new MetaException(
          "add_write_notification_log requires a Hortonworks or Hive 4 backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    Object routedRequest = ThriftReflectionCache.deepCopy(request);
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDb", namespace.backendDbName());
    if (catName != null) {
      ThriftReflectionCache.invokeStringSetter(
          routedRequest,
          "setCatName",
          NamespaceTranslator.internalCatalogName(
              catName, dbName, namespace, support.federationLayer.preserveBackendCatalogName()));
    }
    return support.invokeBackendNamed(backend, "add_write_notification_log", routedRequest);
  }
}
