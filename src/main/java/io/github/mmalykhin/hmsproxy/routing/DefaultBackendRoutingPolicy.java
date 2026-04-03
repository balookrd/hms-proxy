package io.github.mmalykhin.hmsproxy.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class DefaultBackendRoutingPolicy {
  private static final Map<String, Policy> ROUTES = buildRoutes();

  private DefaultBackendRoutingPolicy() {
  }

  public static boolean routesToDefaultBackend(String methodName) {
    return policyFor(methodName).isPresent();
  }

  public static Optional<Policy> policyFor(String methodName) {
    return Optional.ofNullable(ROUTES.get(methodName));
  }

  private static Map<String, Policy> buildRoutes() {
    Map<String, Policy> routes = new LinkedHashMap<>();
    register(routes, Policy.SESSION_COMPATIBILITY, List.of("set_ugi", "flushCache"));
    register(
        routes,
        Policy.SERVICE_READS,
        List.of(
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
            "get_runtime_stats"));
    register(
        routes,
        Policy.TXN_AND_LOCK_LIFECYCLE,
        List.of(
            "open_txns",
            "commit_txn",
            "abort_txn",
            "abort_txns",
            "check_lock",
            "unlock",
            "heartbeat",
            "heartbeat_txn_range"));
    register(routes, Policy.NAMESPACELESS_VALIDATION, List.of("partition_name_has_valid_characters"));
    return Map.copyOf(routes);
  }

  private static void register(Map<String, Policy> routes, Policy policy, List<String> methods) {
    for (String method : methods) {
      routes.put(method, policy);
    }
  }

  public enum Policy {
    SESSION_COMPATIBILITY,
    SERVICE_READS,
    TXN_AND_LOCK_LIFECYCLE,
    NAMESPACELESS_VALIDATION
  }
}
