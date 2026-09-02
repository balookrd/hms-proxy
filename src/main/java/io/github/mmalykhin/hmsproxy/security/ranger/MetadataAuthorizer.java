package io.github.mmalykhin.hmsproxy.security.ranger;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.List;

public interface MetadataAuthorizer extends AutoCloseable {
  boolean isDatabaseAllowed(String catalogName, String backendDbName, ImpersonationContext impersonation);

  List<String> filterDatabases(String catalogName, List<String> backendDbNames, ImpersonationContext impersonation);

  boolean isTableAllowed(String catalogName, String backendDbName, String tableName, ImpersonationContext impersonation);

  List<String> filterTables(String catalogName, String backendDbName, List<String> tableNames, ImpersonationContext impersonation);

  @Override
  void close();
}
