package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class AddPartitionsReqHandler implements SpecialCaseHandler {
  private static final Method ADD_PARTITIONS_REQ = findMethod("add_partitions_req", AddPartitionsRequest.class);

  private final RoutingSupport support;

  AddPartitionsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = NamespaceTranslator.extractDbName(request);
    if (dbName == null) {
      throw new MetaException("add_partitions_req requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("add_partitions_req", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "add_partitions_req", namespace.backendDbName());

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    Object result;
    try {
      result = support.invokeBackendNamed(backend, "add_partitions_req", routedRequest);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        AddPartitionsRequest apacheReq =
            (AddPartitionsRequest) ThriftValueConverter.convertTBase(routedRequest, AddPartitionsRequest.class);
        result = support.invokeDirect(backend, ADD_PARTITIONS_REQ, new Object[]{apacheReq});
      } else {
        throw cause;
      }
    }
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
