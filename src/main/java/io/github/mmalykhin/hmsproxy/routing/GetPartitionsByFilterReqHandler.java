package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
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
    Object result;
    if (backend.runtimeProfile().isHive4()) {
      result = support.invokeBackendNamed(backend, "get_partitions_by_filter_req", routedRequest);
    } else {
      result = fallbackToLegacy(backend, routedRequest);
    }
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
