package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.Table;

interface ExternalTableDropPurger {
  boolean enabledFor(CatalogBackend backend);

  Optional<PurgeRequest> prepare(CatalogBackend backend, Table table) throws Exception;

  default Optional<PurgeRequest> prepare(
      CatalogBackend backend,
      Table table,
      ImpersonationContext impersonation
  ) throws Exception {
    return prepare(backend, table);
  }

  void purge(CatalogBackend backend, PurgeRequest request) throws Exception;

  record PurgeRequest(String location, ImpersonationContext impersonation) {
    PurgeRequest(String location) {
      this(location, null);
    }
  }
}
