package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.Table;

interface ExternalTableDropPurger {
  boolean enabledFor(CatalogBackend backend);

  Optional<PurgeRequest> prepare(CatalogBackend backend, Table table) throws Exception;

  void purge(CatalogBackend backend, PurgeRequest request) throws Exception;

  record PurgeRequest(String location) {
  }
}
