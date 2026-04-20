package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;

/** Database-level metadata RPCs (namespace is the db name in arg0). */
final class DatabaseMetadataOps {
  private DatabaseMetadataOps() {
  }

  static void contribute(OperationRegistry r) {
    r.op("get_database", o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).trace());
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0),
        "drop_database", "alter_database");
  }
}
