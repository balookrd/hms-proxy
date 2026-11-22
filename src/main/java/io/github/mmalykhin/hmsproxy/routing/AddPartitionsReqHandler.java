package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;

final class AddPartitionsReqHandler implements SpecialCaseHandler {
  private static final Method ADD_PARTITIONS_REQ =
      RequestObjectRouting.findIfaceMethod("add_partitions_req", AddPartitionsRequest.class);

  private final RoutingSupport support;

  AddPartitionsReqHandler(RoutingSupport support) {
    this.support = support;
  }

  AddPartitionsReqHandler(RoutingSupport support, ExternalTableLocationRewriter ignored) {
    this(support);
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    RequestObjectRouting.ResolvedRequest resolved =
        RequestObjectRouting.resolveWrite(support, "add_partitions_req", request);
    CatalogRouter.ResolvedNamespace namespace = resolved.namespace();
    CatalogBackend backend = resolved.backend();

    Object routedRequest = support.federationLayer.internalizeObjectArguments(new Object[]{request}, namespace)[0];
    ExternalTableLocationRewriter externalTableLocationRewriter = support.externalTableLocationRewriter();
    if (externalTableLocationRewriter != null) {
      externalTableLocationRewriter.rewriteObjectArguments(new Object[]{routedRequest}, namespace, "add_partitions_req");
    }
    Object result = RequestObjectRouting.invokeWithUnsupportedFallback(
        support,
        backend,
        "add_partitions_req",
        routedRequest,
        () -> {
          AddPartitionsRequest apacheReq =
              (AddPartitionsRequest) ThriftValueConverter.convertTBase(routedRequest, AddPartitionsRequest.class);
          return support.invokeDirect(backend, ADD_PARTITIONS_REQ, new Object[]{apacheReq});
        });
    String tableName = RoutingSupport.extractTableName(routedRequest);
    if (tableName != null) {
      support.invalidateTable(namespace.catalogName(), namespace.backendDbName(), tableName);
    }
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
