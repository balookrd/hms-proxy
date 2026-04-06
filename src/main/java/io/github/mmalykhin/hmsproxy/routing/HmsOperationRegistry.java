package io.github.mmalykhin.hmsproxy.routing;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class HmsOperationRegistry {
  private static final Set<String> READ_PREFIXES = Set.of("get_", "list_", "show_");
  private static final Set<String> WRITE_PREFIXES = Set.of(
      "create_",
      "alter_",
      "drop_",
      "truncate_",
      "append_",
      "add_",
      "set_",
      "update_",
      "delete_",
      "remove_",
      "grant_",
      "revoke_",
      "rename_",
      "exchange_",
      "open_",
      "commit_",
      "abort_",
      "rollback_",
      "allocate_",
      "lock",
      "unlock",
      "heartbeat",
      "compact_",
      "mark_");

  private static final Map<String, OperationClass> OPERATION_CLASS_OVERRIDES = buildOperationClassOverrides();
  private static final Map<String, Boolean> MUTATION_OVERRIDES = buildMutationOverrides();
  private static final Map<String, NamespaceStrategy> NAMESPACE_STRATEGY_OVERRIDES = buildNamespaceStrategyOverrides();
  private static final Map<String, TableExposureMode> TABLE_EXPOSURE_OVERRIDES = buildTableExposureOverrides();
  private static final Map<String, ReadResultFilterKind> RESULT_FILTER_OVERRIDES = buildResultFilterOverrides();
  private static final Map<String, DefaultBackendRoutingPolicy.Policy> DEFAULT_BACKEND_POLICY_OVERRIDES =
      buildDefaultBackendPolicyOverrides();
  private static final ConcurrentMap<String, OperationMetadata> DESCRIBE_CACHE = new ConcurrentHashMap<>();
  private static final Set<String> TRACE_METHOD_OVERRIDES = Set.of(
      "get_database",
      "get_table",
      "get_table_req",
      "get_valid_write_ids",
      "open_txns",
      "allocate_table_write_ids",
      "lock",
      "check_lock",
      "commit_txn",
      "rollback_txn",
      "abort_txn",
      "add_write_notification_log",
      "heartbeat",
      "heartbeat_txn_range",
      "truncate_table",
      "alter_table",
      "alter_table_with_environment_context",
      "add_partition",
      "add_partitions",
      "add_partitions_req",
      "append_partition",
      "append_partition_by_name",
      "alter_partition",
      "alter_partitions",
      "rename_partition",
      "set_aggr_stats_for",
      "update_table_column_statistics",
      "update_partition_column_statistics");

  private HmsOperationRegistry() {
  }

  public static OperationMetadata describe(String methodName) {
    return DESCRIBE_CACHE.computeIfAbsent(methodName == null ? "" : methodName, HmsOperationRegistry::compute);
  }

  private static OperationMetadata compute(String methodName) {
    String normalizedMethod = normalizeMethod(methodName);
    OperationClass operationClass = OPERATION_CLASS_OVERRIDES.getOrDefault(
        normalizedMethod,
        deriveOperationClass(normalizedMethod));
    boolean mutating = MUTATION_OVERRIDES.getOrDefault(normalizedMethod, deriveMutation(normalizedMethod));
    DefaultBackendRoutingPolicy.Policy defaultBackendPolicy = DEFAULT_BACKEND_POLICY_OVERRIDES.get(normalizedMethod);
    NamespaceStrategy namespaceStrategy = NAMESPACE_STRATEGY_OVERRIDES.getOrDefault(
        normalizedMethod,
        deriveNamespaceStrategy(operationClass, defaultBackendPolicy));
    TableExposureMode tableExposureMode = TABLE_EXPOSURE_OVERRIDES.getOrDefault(
        normalizedMethod,
        TableExposureMode.NONE);
    ReadResultFilterKind resultFilterKind = RESULT_FILTER_OVERRIDES.getOrDefault(
        normalizedMethod,
        ReadResultFilterKind.NONE);
    boolean trace = TRACE_METHOD_OVERRIDES.contains(normalizedMethod);
    return new OperationMetadata(
        normalizedMethod,
        operationClass,
        mutating,
        trace,
        namespaceStrategy,
        tableExposureMode,
        resultFilterKind,
        defaultBackendPolicy);
  }

  private static OperationClass deriveOperationClass(String methodName) {
    if (methodName == null || methodName.isBlank()) {
      return OperationClass.ADMIN_INTROSPECTION;
    }
    String normalized = canonicalizeForPrefixMatch(methodName);
    for (String prefix : READ_PREFIXES) {
      if (normalized.startsWith(prefix)) {
        return OperationClass.METADATA_READ;
      }
    }
    for (String prefix : WRITE_PREFIXES) {
      if (normalized.startsWith(prefix)) {
        return OperationClass.METADATA_WRITE;
      }
    }
    return OperationClass.ADMIN_INTROSPECTION;
  }

  private static boolean deriveMutation(String methodName) {
    if (methodName == null || methodName.isBlank()) {
      return false;
    }
    String normalized = canonicalizeForPrefixMatch(methodName);
    if ("add_write_notification_log".equals(normalized)) {
      return true;
    }
    for (String prefix : READ_PREFIXES) {
      if (normalized.startsWith(prefix)) {
        return false;
      }
    }
    for (String prefix : WRITE_PREFIXES) {
      if (normalized.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static NamespaceStrategy deriveNamespaceStrategy(
      OperationClass operationClass,
      DefaultBackendRoutingPolicy.Policy defaultBackendPolicy
  ) {
    if (defaultBackendPolicy != null) {
      return NamespaceStrategy.NONE;
    }
    return switch (operationClass) {
      case SERVICE_GLOBAL_READ,
          SERVICE_GLOBAL_WRITE,
          ADMIN_INTROSPECTION,
          COMPATIBILITY_ONLY_RPC -> NamespaceStrategy.NONE;
      case METADATA_READ,
          METADATA_WRITE,
          ACID_NAMESPACE_BOUND_WRITE,
          ACID_ID_BOUND_LIFECYCLE -> NamespaceStrategy.EXTRACT_FROM_ARGS;
    };
  }

  private static Map<String, OperationClass> buildOperationClassOverrides() {
    Map<String, OperationClass> overrides = new LinkedHashMap<>();
    register(overrides, OperationClass.SERVICE_GLOBAL_READ, List.of(
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
        "get_runtime_stats",
        "get_role_names",
        "list_privileges",
        "get_principals_in_role",
        "get_role_grants_for_principal",
        "get_privilege_set",
        "refresh_privileges"));
    register(overrides, OperationClass.SERVICE_GLOBAL_WRITE, List.of(
        "setMetaConf",
        "create_role",
        "drop_role",
        "grant_role",
        "revoke_role"));
    register(overrides, OperationClass.ACID_NAMESPACE_BOUND_WRITE, List.of(
        "get_valid_write_ids",
        "allocate_table_write_ids",
        "lock",
        "compact",
        "compact2",
        "add_dynamic_partitions",
        "fire_listener_event",
        "repl_tbl_writeid_state"));
    register(overrides, OperationClass.ACID_ID_BOUND_LIFECYCLE, List.of(
        "open_txns",
        "commit_txn",
        "abort_txn",
        "abort_txns",
        "check_lock",
        "unlock",
        "heartbeat",
        "heartbeat_txn_range"));
    register(overrides, OperationClass.ADMIN_INTROSPECTION, List.of(
        "getName",
        "getVersion",
        "aliveSince",
        "getStatus",
        "reinitialize",
        "shutdown",
        "get_catalogs",
        "get_catalog",
        "get_config_value",
        "flushCache",
        "partition_name_has_valid_characters"));
    register(overrides, OperationClass.COMPATIBILITY_ONLY_RPC, List.of(
        "set_ugi",
        "get_delegation_token",
        "renew_delegation_token",
        "cancel_delegation_token",
        "add_token",
        "remove_token",
        "get_token",
        "get_all_token_identifiers",
        "add_master_key",
        "update_master_key",
        "remove_master_key",
        "get_master_keys",
        "add_write_notification_log",
        "get_tables_ext",
        "get_all_materialized_view_objects_for_rewriting"));
    return Map.copyOf(overrides);
  }

  private static Map<String, Boolean> buildMutationOverrides() {
    Map<String, Boolean> overrides = new LinkedHashMap<>();
    register(overrides, false, List.of(
        "set_ugi",
        "flushCache",
        "partition_name_has_valid_characters",
        "get_valid_write_ids",
        "get_tables_ext",
        "get_all_materialized_view_objects_for_rewriting"));
    register(overrides, true, List.of(
        "add_write_notification_log",
        "compact",
        "compact2",
        "add_dynamic_partitions",
        "fire_listener_event",
        "repl_tbl_writeid_state"));
    return Map.copyOf(overrides);
  }

  private static Map<String, NamespaceStrategy> buildNamespaceStrategyOverrides() {
    Map<String, NamespaceStrategy> overrides = new LinkedHashMap<>();
    register(overrides, NamespaceStrategy.DB_STRING_ARG0, List.of(
        "get_database",
        "drop_database",
        "alter_database",
        "get_all_tables",
        "get_tables",
        "get_tables_by_type",
        "get_materialized_views_for_rewriting",
        "get_table",
        "get_table_objects_by_name",
        "truncate_table",
        "drop_table",
        "drop_table_with_environment_context",
        "get_fields",
        "get_fields_with_environment_context",
        "get_schema",
        "get_schema_with_environment_context",
        "get_table_names_by_filter"));
    register(overrides, NamespaceStrategy.DB_FIRST_STRING_ARG0, List.of(
        "update_creation_metadata",
        "append_partition",
        "append_partition_with_environment_context",
        "append_partition_by_name",
        "append_partition_by_name_with_environment_context",
        "drop_partition",
        "drop_partition_with_environment_context",
        "drop_partition_by_name",
        "drop_partition_by_name_with_environment_context",
        "get_partition",
        "get_partition_with_auth",
        "get_partition_by_name",
        "get_partitions",
        "get_partitions_with_auth",
        "get_partitions_pspec",
        "get_partition_names",
        "get_partitions_ps",
        "get_partitions_ps_with_auth",
        "get_partition_names_ps",
        "get_partitions_by_filter",
        "get_part_specs_by_filter",
        "get_num_partitions_by_filter",
        "get_partitions_by_names",
        "markPartitionForEvent",
        "isPartitionMarkedForEvent",
        "get_table_column_statistics",
        "get_partition_column_statistics",
        "delete_partition_column_statistics",
        "delete_table_column_statistics",
        "drop_function",
        "alter_function",
        "get_functions",
        "get_function",
        "get_lock_materialization_rebuild",
        "heartbeat_lock_materialization_rebuild"));
    return Map.copyOf(overrides);
  }

  private static Map<String, TableExposureMode> buildTableExposureOverrides() {
    Map<String, TableExposureMode> overrides = new LinkedHashMap<>();
    register(overrides, TableExposureMode.TABLE_REQUEST, List.of("get_table_req"));
    register(overrides, TableExposureMode.TABLE_ARG1, List.of(
        "get_table",
        "get_fields",
        "get_fields_with_environment_context",
        "get_schema",
        "get_schema_with_environment_context",
        "get_partition",
        "get_partition_with_auth",
        "get_partition_by_name",
        "get_partitions",
        "get_partitions_with_auth",
        "get_partitions_pspec",
        "get_partition_names",
        "get_partitions_ps",
        "get_partitions_ps_with_auth",
        "get_partition_names_ps",
        "get_partitions_by_filter",
        "get_part_specs_by_filter",
        "get_num_partitions_by_filter",
        "get_partitions_by_names",
        "get_table_column_statistics",
        "get_partition_column_statistics"));
    return Map.copyOf(overrides);
  }

  private static Map<String, ReadResultFilterKind> buildResultFilterOverrides() {
    Map<String, ReadResultFilterKind> overrides = new LinkedHashMap<>();
    register(overrides, ReadResultFilterKind.TABLE_NAME_LIST, List.of(
        "get_all_tables",
        "get_tables",
        "get_tables_by_type",
        "get_materialized_views_for_rewriting",
        "get_table_names_by_filter"));
    register(overrides, ReadResultFilterKind.SINGLE_TABLE, List.of("get_table", "get_table_req"));
    register(overrides, ReadResultFilterKind.TABLE_COLLECTION, List.of(
        "get_table_objects_by_name",
        "get_table_objects_by_name_req"));
    return Map.copyOf(overrides);
  }

  private static Map<String, DefaultBackendRoutingPolicy.Policy> buildDefaultBackendPolicyOverrides() {
    Map<String, DefaultBackendRoutingPolicy.Policy> overrides = new LinkedHashMap<>();
    register(overrides, DefaultBackendRoutingPolicy.Policy.SESSION_COMPATIBILITY, List.of("set_ugi", "flushCache"));
    register(overrides, DefaultBackendRoutingPolicy.Policy.SERVICE_READS, List.of(
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
    register(overrides, DefaultBackendRoutingPolicy.Policy.TXN_AND_LOCK_LIFECYCLE, List.of(
        "open_txns",
        "commit_txn",
        "abort_txn",
        "abort_txns",
        "check_lock",
        "unlock",
        "heartbeat",
        "heartbeat_txn_range"));
    register(overrides, DefaultBackendRoutingPolicy.Policy.NAMESPACELESS_VALIDATION,
        List.of("partition_name_has_valid_characters"));
    return Map.copyOf(overrides);
  }

  private static <T> void register(Map<String, T> target, T value, List<String> methods) {
    for (String method : methods) {
      target.put(method, value);
    }
  }

  private static String normalizeMethod(String methodName) {
    return methodName == null ? "" : methodName.trim();
  }

  private static String canonicalizeForPrefixMatch(String methodName) {
    String normalized = normalizeMethod(methodName);
    StringBuilder builder = new StringBuilder(normalized.length() + 8);
    for (int i = 0; i < normalized.length(); i++) {
      char current = normalized.charAt(i);
      if (Character.isUpperCase(current) && i > 0 && builder.charAt(builder.length() - 1) != '_') {
        builder.append('_');
      }
      builder.append(Character.toLowerCase(current));
    }
    return builder.toString();
  }

  public enum OperationClass {
    METADATA_READ,
    METADATA_WRITE,
    SERVICE_GLOBAL_READ,
    SERVICE_GLOBAL_WRITE,
    ACID_NAMESPACE_BOUND_WRITE,
    ACID_ID_BOUND_LIFECYCLE,
    ADMIN_INTROSPECTION,
    COMPATIBILITY_ONLY_RPC;

    public String wireName() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  public enum NamespaceStrategy {
    NONE,
    DB_STRING_ARG0,
    DB_FIRST_STRING_ARG0,
    EXTRACT_FROM_ARGS
  }

  public enum TableExposureMode {
    NONE,
    TABLE_ARG1,
    TABLE_REQUEST
  }

  public enum ReadResultFilterKind {
    NONE,
    TABLE_NAME_LIST,
    SINGLE_TABLE,
    TABLE_COLLECTION
  }

  public record OperationMetadata(
      String methodName,
      OperationClass operationClass,
      boolean mutating,
      boolean trace,
      NamespaceStrategy namespaceStrategy,
      TableExposureMode tableExposureMode,
      ReadResultFilterKind readResultFilterKind,
      DefaultBackendRoutingPolicy.Policy defaultBackendPolicy
  ) {
    public Optional<DefaultBackendRoutingPolicy.Policy> defaultBackendPolicyOptional() {
      return Optional.ofNullable(defaultBackendPolicy);
    }
  }
}
