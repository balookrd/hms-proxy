package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;

/** Administrative introspection RPCs: no namespace routing, non-mutating service endpoints. */
final class AdminIntrospectionOps {
  private AdminIntrospectionOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION).nonMutating(),
        "getName", "getVersion", "aliveSince", "getStatus",
        "get_catalogs", "get_catalog", "get_config_value");
    r.all(o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION),
        "reinitialize", "shutdown");
    r.op("flushCache",
        o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION).backend(Policy.SESSION_COMPATIBILITY).mutating());
    r.all(o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION)
            .backend(Policy.NAMESPACELESS_VALIDATION).nonMutating(),
        "partition_name_has_valid_characters",
        "partition_name_to_spec",
        "partition_name_to_vals");
  }
}
