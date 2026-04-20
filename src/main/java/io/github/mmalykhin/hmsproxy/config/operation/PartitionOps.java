package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.catalog.TableExposureMode;

/** Partition-scoped RPCs (db name is the first string arg, table name in arg1 for exposure). */
final class PartitionOps {
  private PartitionOps() {
  }

  static void contribute(OperationRegistry r) {
    // Reads.
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).expose(TableExposureMode.TABLE_ARG1),
        "get_partition", "get_partition_with_auth", "get_partition_by_name",
        "get_partitions", "get_partitions_with_auth", "get_partitions_pspec",
        "get_partition_names", "get_partitions_ps", "get_partitions_ps_with_auth",
        "get_partition_names_ps", "get_partitions_by_filter", "get_part_specs_by_filter",
        "get_num_partitions_by_filter", "get_partitions_by_names",
        "get_table_column_statistics", "get_partition_column_statistics");

    // Writes and bookkeeping.
    r.op("append_partition", o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).trace());
    r.op("append_partition_by_name", o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).trace());
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0),
        "update_creation_metadata",
        "append_partition_with_environment_context",
        "append_partition_by_name_with_environment_context",
        "drop_partition", "drop_partition_with_environment_context",
        "drop_partition_by_name", "drop_partition_by_name_with_environment_context",
        "markPartitionForEvent", "isPartitionMarkedForEvent",
        "delete_partition_column_statistics", "delete_table_column_statistics");
  }
}
