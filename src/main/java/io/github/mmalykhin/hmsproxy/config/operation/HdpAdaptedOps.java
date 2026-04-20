package io.github.mmalykhin.hmsproxy.config.operation;

/** HDP-adapted request RPCs (argument-envelope translation only). */
final class HdpAdaptedOps {
  private HdpAdaptedOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.hdp(),
        "get_database_req", "create_table_req", "truncate_table_req",
        "alter_table_req", "alter_partitions_req", "rename_partition_req",
        "update_table_column_statistics_req", "update_partition_column_statistics_req",
        "get_partitions_by_names_req");
  }
}
