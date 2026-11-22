package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;

final class GetTableReqHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  GetTableReqHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catName = RequestObjectRouting.readCatalogName(request);
    String dbName = RequestObjectRouting.readDbName(request);
    String tblName = RequestObjectRouting.readTableName(request);

    if (HivePlaceholderNamespace.isPlaceholderTable(dbName, tblName)) {
      throw HivePlaceholderNamespace.newNoSuchObjectException(catName, dbName, tblName);
    }
    if (SparkPlaceholderNamespace.isSparkReservedTable(dbName, tblName)) {
      throw SparkPlaceholderNamespace.newNoSuchTableException(catName, dbName, tblName);
    }

    CatalogRouter.ResolvedNamespace namespace = support.resolveRequestNamespace(catName, dbName);
    if (HivePlaceholderNamespace.isPlaceholderTable(namespace.backendDbName(), tblName)) {
      throw HivePlaceholderNamespace.newNoSuchObjectException(namespace.catalogName(), dbName, tblName);
    }
    if (SparkPlaceholderNamespace.isSparkReservedTable(namespace.backendDbName(), tblName)) {
      throw SparkPlaceholderNamespace.newNoSuchTableException(namespace.catalogName(), dbName, tblName);
    }
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), catName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateExposedDatabaseAccess(method.getName(), namespace);
    support.validateExposedTableAccess(method.getName(), namespace, tblName);

    Object result;
    if (backend.runtimeProfile().isHive4() && !(request instanceof GetTableRequest)) {
      Object routedRequest = support.federationLayer.internalizeArgument(request, namespace);
      result = support.invokeBackendNamed(backend, "get_table_req", routedRequest);
    } else {
      GetTableRequest apacheReq = request instanceof GetTableRequest standardReq
          ? standardReq
          : (GetTableRequest) ThriftValueConverter.convertTBase(request, GetTableRequest.class);
      GetTableRequest routedRequest =
          (GetTableRequest) support.federationLayer.internalizeTableRequest(apacheReq, namespace);
      result = support.invokeViaRequest(backend, routedRequest, method.getName());
    }

    result = support.filterSingleTableResult(method.getName(), namespace, result);
    return support.federationLayer.externalizeResult(result, namespace);
  }
}
