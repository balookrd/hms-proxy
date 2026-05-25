package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * BackendAdapter for an external Hive 4.1.x metastore server. The Iceberg/Hive
 * 4 Thrift IDL drops two positional read methods that Apache 3.1.3 still uses
 * on the proxy side:
 *
 *   - get_table(db, tbl)
 *   - get_table_objects_by_name(db, names)
 *
 * Both are replaced in Hive 4 by their request-wrapper equivalents
 * (get_table_req, get_table_objects_by_name_req). When routing handlers reach
 * this adapter with the positional methods, they are upgraded to the *_req
 * variants and the response is unwrapped back to the Apache 3.1.3 return type.
 * Everything else (the 199 methods shared with Apache 3.1.3 and all *_req
 * wrappers Hive 4 added) flows through the default delegation, which works
 * because Thrift binary serialization is forward/backward compatible at the
 * Iface level.
 */
public final class Hive4BackendAdapter extends AbstractBackendAdapter {
  private static final Map<String, MethodUpgradeHandler> METHOD_UPGRADES = Map.of(
      "get_table",
      (backend, args, impersonation) -> {
        GetTableRequest request = new GetTableRequest((String) args[0], (String) args[1]);
        GetTableResult result = (GetTableResult) backend.invokeRawByName(
            "get_table_req",
            new Class<?>[]{GetTableRequest.class},
            new Object[]{request},
            impersonation);
        return result == null ? null : result.getTable();
      },
      "get_table_objects_by_name",
      (backend, args, impersonation) -> {
        GetTablesRequest request = new GetTablesRequest((String) args[0]);
        @SuppressWarnings("unchecked")
        List<String> names = (List<String>) args[1];
        request.setTblNames(names);
        GetTablesResult result = (GetTablesResult) backend.invokeRawByName(
            "get_table_objects_by_name_req",
            new Class<?>[]{GetTablesRequest.class},
            new Object[]{request},
            impersonation);
        return result == null ? List.<Table>of() : result.getTables();
      });

  Hive4BackendAdapter() {
    super(MetastoreRuntimeProfile.APACHE_4_1_0);
  }

  @Override
  public Object invoke(
      CatalogBackend backend,
      Method method,
      Object[] args,
      ImpersonationContext impersonation
  ) throws Throwable {
    MethodUpgradeHandler upgrade = METHOD_UPGRADES.get(method.getName());
    if (upgrade != null) {
      return upgrade.invoke(backend, args, impersonation);
    }
    return super.invoke(backend, method, args, impersonation);
  }

  @FunctionalInterface
  private interface MethodUpgradeHandler {
    Object invoke(
        CatalogBackend backend,
        Object[] args,
        ImpersonationContext impersonation
    ) throws Throwable;
  }
}
