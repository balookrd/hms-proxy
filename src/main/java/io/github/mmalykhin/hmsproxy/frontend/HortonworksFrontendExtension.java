package io.github.mmalykhin.hmsproxy.frontend;

public interface HortonworksFrontendExtension {
  Object addWriteNotificationLog(Object request) throws Throwable;
  Object getTablesExt(Object request) throws Throwable;
  Object getAllMaterializedViewObjectsForRewriting() throws Throwable;
  Object set_aggr_stats_for(Object request) throws Throwable;
  Object alter_table_req(Object request) throws Throwable;
  Object alter_partitions_req(Object request) throws Throwable;
  Object truncate_table_req(Object request) throws Throwable;
  Object rename_partition_req(Object request) throws Throwable;
  Object get_table_statistics_req(Object request) throws Throwable;
  Object get_partitions_statistics_req(Object request) throws Throwable;
  Object add_partitions_req(Object request) throws Throwable;
  Object add_write_notification_log_in_batch(Object request) throws Throwable;
}
