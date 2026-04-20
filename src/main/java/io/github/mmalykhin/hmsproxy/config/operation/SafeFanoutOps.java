package io.github.mmalykhin.hmsproxy.config.operation;

/** Reads that are safe to fan out across all backends. */
final class SafeFanoutOps {
  private SafeFanoutOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.safeFanout(), "get_all_databases", "get_databases", "get_table_meta");
  }
}
