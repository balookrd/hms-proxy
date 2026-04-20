package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;

/** Administrative introspection RPCs: no namespace routing, non-mutating service endpoints. */
final class AdminIntrospectionOps {
  private AdminIntrospectionOps() {
  }

  static void contribute(OperationRegistry r) {
    r.all(o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION),
        "getName", "getVersion", "aliveSince", "getStatus", "reinitialize", "shutdown",
        "get_catalogs", "get_catalog", "get_config_value");
    r.op("flushCache",
        o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION).backend(Policy.SESSION_COMPATIBILITY));
    r.op("partition_name_has_valid_characters",
        o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION).backend(Policy.NAMESPACELESS_VALIDATION));
  }
}
