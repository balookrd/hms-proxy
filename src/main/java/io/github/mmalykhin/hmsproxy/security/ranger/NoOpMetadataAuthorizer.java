package io.github.mmalykhin.hmsproxy.security.ranger;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.ArrayList;
import java.util.List;

public final class NoOpMetadataAuthorizer implements MetadataAuthorizer {
  public static final NoOpMetadataAuthorizer INSTANCE = new NoOpMetadataAuthorizer();

  @Override
  public boolean isDatabaseAllowed(String catalogName, String backendDbName, ImpersonationContext impersonation) {
    return true;
  }

  @Override
  public List<String> filterDatabases(String catalogName, List<String> backendDbNames, ImpersonationContext impersonation) {
    return backendDbNames == null ? List.of() : new ArrayList<>(backendDbNames);
  }

  @Override
  public boolean isTableAllowed(String catalogName, String backendDbName, String tableName, ImpersonationContext impersonation) {
    return true;
  }

  @Override
  public List<String> filterTables(String catalogName, String backendDbName, List<String> tableNames, ImpersonationContext impersonation) {
    return tableNames == null ? List.of() : new ArrayList<>(tableNames);
  }

  @Override
  public void close() {
  }
}
