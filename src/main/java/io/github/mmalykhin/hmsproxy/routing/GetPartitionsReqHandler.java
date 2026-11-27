package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;

final class GetPartitionsReqHandler implements SpecialCaseHandler {
  private static final Method GET_PARTITIONS = RequestObjectRouting.findIfaceMethod("get_partitions",
      String.class, String.class, short.class);

  private final RoutingSupport support;

  GetPartitionsReqHandler(RoutingSupport support) {
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
            ? support.invokeBackendNamed(backend, "get_partitions_req", routedRequest)
            : fallbackToLegacy(backend, routedRequest));
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTblName");
    Object maxPartsObj = ThriftReflectionCache.invokeGetter(routedRequest, "getMaxParts");
    short maxParts = maxPartsObj instanceof Number number ? number.shortValue() : (short) -1;

    Object partitions = support.invokeDirect(backend, GET_PARTITIONS, new Object[]{db, tbl, maxParts});
    ClassLoader cl = routedRequest.getClass().getClassLoader();
    Class<?> responseClass = Class.forName("org.apache.hadoop.hive.metastore.api.PartitionsResponse", true, cl);
    Object response = responseClass.getConstructor().newInstance();
    Object convertedPartitions = ThriftValueConverter.convertDynamicValue(partitions, cl);
    responseClass.getMethod("setPartitions", List.class).invoke(response, convertedPartitions);
    return response;
  }
}
