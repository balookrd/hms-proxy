package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreHandler;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;

public final class HortonworksBackendAdapter extends AbstractBackendAdapter {
  HortonworksBackendAdapter() {
    super(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78);
  }

  @Override
  public Object invokeGetTableReq(
      CatalogBackend backend,
      GetTableRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    Object result = backend.invokeRawByName(
        "get_table",
        new Class<?>[] {String.class, String.class},
        new Object[] {request.getDbName(), request.getTblName()},
        impersonation);
    return new GetTableResult((org.apache.hadoop.hive.metastore.api.Table) result);
  }

  @Override
  public Object invokeGetTablesReq(
      CatalogBackend backend,
      GetTablesRequest request,
      RoutingMetaStoreHandler.ImpersonationContext impersonation
  ) throws Throwable {
    Object result = backend.invokeRawByName(
        "get_table_objects_by_name",
        new Class<?>[] {String.class, List.class},
        new Object[] {request.getDbName(), request.getTblNames()},
        impersonation);
    @SuppressWarnings("unchecked")
    List<org.apache.hadoop.hive.metastore.api.Table> tables =
        (List<org.apache.hadoop.hive.metastore.api.Table>) result;
    return new GetTablesResult(tables);
  }
}
