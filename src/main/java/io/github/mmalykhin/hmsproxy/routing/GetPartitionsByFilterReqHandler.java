package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;

final class GetPartitionsByFilterReqHandler implements SpecialCaseHandler {
  private static final Method GET_PARTITIONS_BY_FILTER = RequestObjectRouting.findIfaceMethod(
      "get_partitions_by_filter", String.class, String.class, String.class, short.class);

  private final RoutingSupport support;

  GetPartitionsByFilterReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveReadByMethodName(support, method.getName(), request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();

    Object routedRequest = support.federationLayer.internalizeArgument(request, namespace);
    ImpersonationContext impersonation = support.impersonationResolver.resolve().orElse(null);
    String querySig = RoutingSupport.encodeQuerySignature(
        method.getName(),
        new Object[] {
            ThriftReflectionCache.invokeGetter(routedRequest, "getFilter"),
            ThriftReflectionCache.invokeGetter(routedRequest, "getMaxParts"),
            ThriftReflectionCache.invokeGetter(routedRequest, "isWithAuth"),
            ThriftReflectionCache.readString(routedRequest, "getUserName"),
            ThriftReflectionCache.invokeGetter(routedRequest, "getGroupNames")
        },
        0);
    Object result = support.partitionMetadataCache.get(
        namespace.catalogName(),
        namespace.backendDbName(),
        resolved.tableName(),
        querySig,
        impersonation,
        () -> backend.runtimeProfile().isHive4()
            ? support.invokeBackendNamed(backend, "get_partitions_by_filter_req", routedRequest)
            : fallbackToLegacy(backend, routedRequest));
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTblName");
    String filter = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getFilter");
    Object maxPartsObj = ThriftReflectionCache.invokeGetter(routedRequest, "getMaxParts");
    short maxParts = maxPartsObj instanceof Number number ? number.shortValue() : (short) -1;

    Object partitions = support.invokeDirect(backend, GET_PARTITIONS_BY_FILTER, new Object[]{db, tbl, filter, maxParts});
    ClassLoader cl = routedRequest.getClass().getClassLoader();
    return ThriftValueConverter.convertDynamicValue(partitions, cl);
  }
}
