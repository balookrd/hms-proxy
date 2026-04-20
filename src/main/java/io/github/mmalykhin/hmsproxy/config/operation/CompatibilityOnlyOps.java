package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;

/** Compatibility-only RPCs proxied through the default backend without namespace routing. */
final class CompatibilityOnlyOps {
  private CompatibilityOnlyOps() {
  }

  static void contribute(OperationRegistry r) {
    r.op("set_ugi", o -> o.cls(HmsOperationClass.COMPATIBILITY_ONLY_RPC)
        .backend(Policy.SESSION_COMPATIBILITY).nonMutating());
    r.all(o -> o.cls(HmsOperationClass.COMPATIBILITY_ONLY_RPC),
        "get_delegation_token", "renew_delegation_token", "cancel_delegation_token",
        "add_token", "remove_token", "get_token", "get_all_token_identifiers",
        "add_master_key", "update_master_key", "remove_master_key", "get_master_keys");
    r.op("add_write_notification_log",
        o -> o.cls(HmsOperationClass.COMPATIBILITY_ONLY_RPC).trace().hdp());
    r.op("get_tables_ext", o -> o.cls(HmsOperationClass.COMPATIBILITY_ONLY_RPC).hdp());
    r.op("get_all_materialized_view_objects_for_rewriting",
        o -> o.cls(HmsOperationClass.COMPATIBILITY_ONLY_RPC).hdp());
  }
}
