package io.github.mmalykhin.hmsproxy.config.operation;

/** Writes we trace for audit/observability but have no other policy overrides. */
final class TraceOnlyWriteOps {
  private TraceOnlyWriteOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.trace(),
        "rollback_txn",
        "alter_table", "alter_table_with_environment_context",
        "add_partition", "add_partitions", "add_partitions_req",
        "alter_partition", "alter_partitions", "rename_partition",
        "set_aggr_stats_for",
        "update_table_column_statistics", "update_partition_column_statistics");
  }
}
