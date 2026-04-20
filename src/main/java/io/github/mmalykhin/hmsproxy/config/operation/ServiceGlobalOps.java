package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;

/** Service-global reads and writes (no namespace scope). */
final class ServiceGlobalOps {
  private ServiceGlobalOps() {
  }

  static void contribute(OperationRegistry r) {
    // Reads that share the SERVICE_READS default backend policy.
    r.all(o -> o.cls(HmsOperationClass.SERVICE_GLOBAL_READ).backend(Policy.SERVICE_READS),
        "getMetaConf",
        "get_current_notificationEventId",
        "get_next_notification",
        "get_notification_events_count",
        "get_all_functions",
        "get_metastore_db_uuid",
        "get_open_txns",
        "get_open_txns_info",
        "show_locks",
        "show_compact",
        "get_active_resource_plan",
        "get_all_resource_plans",
        "get_runtime_stats");

    // Role/privilege queries are classed but do not need a backend override.
    r.all(o -> o.cls(HmsOperationClass.SERVICE_GLOBAL_READ),
        "get_role_names", "list_privileges", "get_principals_in_role",
        "get_role_grants_for_principal", "get_privilege_set", "refresh_privileges");

    r.all(o -> o.cls(HmsOperationClass.SERVICE_GLOBAL_WRITE),
        "setMetaConf", "create_role", "drop_role", "grant_role", "revoke_role");
  }
}
