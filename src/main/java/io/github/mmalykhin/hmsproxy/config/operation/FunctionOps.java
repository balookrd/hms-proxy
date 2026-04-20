package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;

/** Functions and materialization-rebuild locks (db-scoped, first string arg). */
final class FunctionOps {
  private FunctionOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0),
        "drop_function", "alter_function", "get_functions", "get_function",
        "get_lock_materialization_rebuild", "heartbeat_lock_materialization_rebuild");
  }
}
