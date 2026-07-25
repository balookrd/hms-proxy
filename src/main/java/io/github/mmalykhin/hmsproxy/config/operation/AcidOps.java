package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;

/** ACID-related RPCs: namespace-bound writes + id-bound txn/lock lifecycle. */
final class AcidOps {
  private AcidOps() {
  }

  static void contribute(OperationRegistry r) {
    // Namespace-bound writes: routed by namespace extracted from args.
    r.all(o -> o.cls(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE).trace(),
        "get_valid_write_ids", "allocate_table_write_ids", "lock");
    r.all(o -> o.cls(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE).mutating(),
        "compact", "compact2", "fire_listener_event", "repl_tbl_writeid_state");
    r.op("add_dynamic_partitions", o -> o.cls(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE));

    // Id-bound lifecycle: all share TXN_AND_LOCK_LIFECYCLE default backend.
    r.all(o -> o.cls(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE)
            .backend(Policy.TXN_AND_LOCK_LIFECYCLE).trace(),
        "open_txns", "commit_txn", "abort_txn", "unlock",
        "heartbeat", "heartbeat_txn_range");
    // TxnHandler.checkLock heartbeats the txn/lock (UPDATE in TXNS/HIVE_LOCKS),
    // so it is mutating even though the name does not match a write prefix.
    r.op("check_lock", o -> o.cls(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE)
        .backend(Policy.TXN_AND_LOCK_LIFECYCLE).trace().mutating());
    r.op("abort_txns",
        o -> o.cls(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE).backend(Policy.TXN_AND_LOCK_LIFECYCLE));
  }
}
