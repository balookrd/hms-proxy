package io.github.mmalykhin.hmsproxy.frontend;

public interface HortonworksFrontendExtension {
  Object addWriteNotificationLog(Object request) throws Throwable;
  Object getTablesExt(Object request) throws Throwable;
  Object getAllMaterializedViewObjectsForRewriting() throws Throwable;
  Object set_aggr_stats_for(Object request) throws Throwable;
}
