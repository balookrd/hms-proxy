package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;

/** Functions and materialization-rebuild locks (db-scoped, first string arg). */
final class FunctionOps {
  private FunctionOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0),
        "drop_function", "alter_function", "get_functions", "get_function",
        "heartbeat_lock_materialization_rebuild");
    // Despite the get_ prefix this acquires a MATERIALIZATION_REBUILD_LOCKS row,
    // so it is a mutating write like its heartbeat sibling.
    r.op("get_lock_materialization_rebuild",
        o -> o.cls(HmsOperationClass.METADATA_WRITE).ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).mutating());
  }
}
