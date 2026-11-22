package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.List;

final class GetPartitionReqHandler implements SpecialCaseHandler {
  private static final Method GET_PARTITION = RequestObjectRouting.findIfaceMethod("get_partition",
      String.class, String.class, List.class);

  private final RoutingSupport support;

  GetPartitionReqHandler(RoutingSupport support) {
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
      result = support.invokeBackendNamed(backend, "get_partition_req", routedRequest);
    } else {
      result = fallbackToLegacy(backend, routedRequest);
    }
    return support.federationLayer.externalizeResult(result, namespace);
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTblName");
    Object partVals = ThriftReflectionCache.invokeGetter(routedRequest, "getPartVals");
    List<String> partValsList = partVals == null ? List.of() : (List<String>) partVals;

    Object partition = support.invokeDirect(backend, GET_PARTITION, new Object[]{db, tbl, partValsList});
    if (partition == null) {
      return null;
    }
    ClassLoader cl = routedRequest.getClass().getClassLoader();
    Class<?> responseClass = Class.forName("org.apache.hadoop.hive.metastore.api.GetPartitionResponse", true, cl);
    Object response = responseClass.getConstructor().newInstance();
    Class<?> partitionClass = Class.forName("org.apache.hadoop.hive.metastore.api.Partition", true, cl);
    Object convertedPartition = ThriftValueConverter.convertValue(partition, partitionClass, cl);
    responseClass.getMethod("setPartition", partitionClass).invoke(response, convertedPartition);
    return response;
  }
}
