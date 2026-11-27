package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;

final class GetPartitionsByNamesReqHandler implements SpecialCaseHandler {
  private static final Method GET_PARTITIONS_BY_NAMES = RequestObjectRouting.findIfaceMethod(
      "get_partitions_by_names", String.class, String.class, List.class);

  private final RoutingSupport support;

  GetPartitionsByNamesReqHandler(RoutingSupport support) {
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
            ThriftReflectionCache.invokeGetter(routedRequest, "getNames"),
            ThriftReflectionCache.invokeGetter(routedRequest, "isGet_col_stats"),
            ThriftReflectionCache.invokeGetter(routedRequest, "getProcessorCapabilities"),
            ThriftReflectionCache.readString(routedRequest, "getValidWriteIdList")
        },
        0);
    Object result = support.partitionMetadataCache.get(
        namespace.catalogName(),
        namespace.backendDbName(),
        resolved.tableName(),
        querySig,
        impersonation,
        () -> RequestObjectRouting.invokeWithUnsupportedFallback(
            support,
            backend,
            "get_partitions_by_names_req",
            routedRequest,
            () -> fallbackToLegacy(backend, routedRequest)));
    return support.federationLayer.externalizeResult(result, namespace);
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDb_name");
    if (db == null) {
      db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    }
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTbl_name");
    if (tbl == null) {
      tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTblName");
    }
    Object names = ThriftReflectionCache.invokeGetter(routedRequest, "getNames");
    List<String> namesList = names == null ? List.of() : (List<String>) names;

    Object partitions = support.invokeDirect(backend, GET_PARTITIONS_BY_NAMES, new Object[]{db, tbl, namesList});
    ClassLoader cl = routedRequest.getClass().getClassLoader();
    Class<?> responseClass = Class.forName("org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult", true, cl);
    Object response = responseClass.getConstructor().newInstance();
    Object convertedPartitions = ThriftValueConverter.convertDynamicValue(partitions, cl);
    responseClass.getMethod("setPartitions", List.class).invoke(response, convertedPartitions);
    return response;
  }
}
