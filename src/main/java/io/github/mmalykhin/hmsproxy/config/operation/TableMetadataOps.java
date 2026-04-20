package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.catalog.ReadResultFilterKind;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;

/** Table-level metadata RPCs (db name in arg0) and table-request RPCs. */
final class TableMetadataOps {
  private TableMetadataOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).filter(ReadResultFilterKind.TABLE_NAME_LIST),
        "get_all_tables", "get_tables", "get_tables_by_type",
        "get_materialized_views_for_rewriting", "get_table_names_by_filter");
    r.op("get_table", o -> o.ns(NamespaceStrategy.DB_STRING_ARG0)
        .expose(TableExposureMode.TABLE_ARG1)
        .filter(ReadResultFilterKind.SINGLE_TABLE)
        .trace());
    r.op("get_table_objects_by_name",
        o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).filter(ReadResultFilterKind.TABLE_COLLECTION));
    r.op("truncate_table", o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).trace());
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0),
        "drop_table", "drop_table_with_environment_context");
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).expose(TableExposureMode.TABLE_ARG1),
        "get_fields", "get_fields_with_environment_context",
        "get_schema", "get_schema_with_environment_context");

    // Table-request RPCs that don't follow DB_STRING_ARG0 shape.
    r.op("get_table_req", o -> o.expose(TableExposureMode.TABLE_REQUEST)
        .filter(ReadResultFilterKind.SINGLE_TABLE)
        .trace());
    r.op("get_table_objects_by_name_req", o -> o.filter(ReadResultFilterKind.TABLE_COLLECTION));
  }
}
