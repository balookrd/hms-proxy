package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.config.DefaultBackendRoutingPolicy.Policy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public final class HmsOperationPolicy {
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

  private static final Map<String, OperationMetadata> REGISTRY = buildRegistry();
  private static final ConcurrentMap<String, OperationMetadata> DERIVED_CACHE = new ConcurrentHashMap<>();

  private HmsOperationPolicy() {
  }

  public static OperationMetadata describe(String methodName) {
    String normalized = normalizeMethod(methodName);
    OperationMetadata registered = REGISTRY.get(normalized);
    if (registered != null) {
      return registered;
    }
    return DERIVED_CACHE.computeIfAbsent(normalized, HmsOperationPolicy::deriveOnly);
  }

  private static OperationMetadata deriveOnly(String methodName) {
    HmsOperationClass operationClass = deriveHmsOperationClass(methodName);
    boolean mutating = deriveMutation(methodName);
    NamespaceStrategy namespaceStrategy = deriveNamespaceStrategy(operationClass, null);
    return new OperationMetadata(
        methodName,
        operationClass,
        mutating,
        false,
        namespaceStrategy,
        TableExposureMode.NONE,
        ReadResultFilterKind.NONE,
        null,
        false,
        false);
  }

  private static Map<String, OperationMetadata> buildRegistry() {
    Registry r = new Registry();

    // Administrative introspection RPCs: no namespace routing, non-mutating service endpoints.
    r.all(o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION),
        "getName", "getVersion", "aliveSince", "getStatus", "reinitialize", "shutdown",
        "get_catalogs", "get_catalog", "get_config_value");
    r.op("flushCache", o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION).backend(Policy.SESSION_COMPATIBILITY));
    r.op("partition_name_has_valid_characters",
        o -> o.cls(HmsOperationClass.ADMIN_INTROSPECTION).backend(Policy.NAMESPACELESS_VALIDATION));

    // Service-global reads. The leading group share the SERVICE_READS default backend policy;
    // the role/privilege queries are classed but do not need a backend override.
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
    r.all(o -> o.cls(HmsOperationClass.SERVICE_GLOBAL_READ),
        "get_role_names", "list_privileges", "get_principals_in_role",
        "get_role_grants_for_principal", "get_privilege_set", "refresh_privileges");

    // Service-global writes.
    r.all(o -> o.cls(HmsOperationClass.SERVICE_GLOBAL_WRITE),
        "setMetaConf", "create_role", "drop_role", "grant_role", "revoke_role");

    // Compatibility-only RPCs: proxied through the default backend without namespace routing.
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

    // ACID namespace-bound writes: routed by namespace extracted from args.
    r.all(o -> o.cls(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE).trace(),
        "get_valid_write_ids", "allocate_table_write_ids", "lock");
    r.all(o -> o.cls(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE).mutating(),
        "compact", "compact2", "fire_listener_event", "repl_tbl_writeid_state");
    r.op("add_dynamic_partitions", o -> o.cls(HmsOperationClass.ACID_NAMESPACE_BOUND_WRITE));

    // ACID id-bound lifecycle: all share TXN_AND_LOCK_LIFECYCLE default backend.
    r.all(o -> o.cls(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE).backend(Policy.TXN_AND_LOCK_LIFECYCLE).trace(),
        "open_txns", "commit_txn", "abort_txn", "check_lock", "unlock",
        "heartbeat", "heartbeat_txn_range");
    r.op("abort_txns",
        o -> o.cls(HmsOperationClass.ACID_ID_BOUND_LIFECYCLE).backend(Policy.TXN_AND_LOCK_LIFECYCLE));

    // Database-level metadata (namespace is the db name in arg0).
    r.op("get_database", o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).trace());
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0),
        "drop_database", "alter_database");

    // Table-level metadata (db name in arg0). Some carry table exposure and/or result filtering.
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).filter(ReadResultFilterKind.TABLE_NAME_LIST),
        "get_all_tables", "get_tables", "get_tables_by_type",
        "get_materialized_views_for_rewriting", "get_table_names_by_filter");
    r.op("get_table", o -> o.ns(NamespaceStrategy.DB_STRING_ARG0)
        .expose(TableExposureMode.TABLE_ARG1)
        .filter(ReadResultFilterKind.SINGLE_TABLE)
        .trace());
    r.op("get_table_objects_by_name",
        o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).filter(ReadResultFilterKind.TABLE_COLLECTION));
    r.op("truncate_table", o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).trace());
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0),
        "drop_table", "drop_table_with_environment_context");
    r.all(o -> o.ns(NamespaceStrategy.DB_STRING_ARG0).expose(TableExposureMode.TABLE_ARG1),
        "get_fields", "get_fields_with_environment_context",
        "get_schema", "get_schema_with_environment_context");

    // Table-request RPCs that don't follow DB_STRING_ARG0 shape.
    r.op("get_table_req", o -> o.expose(TableExposureMode.TABLE_REQUEST)
        .filter(ReadResultFilterKind.SINGLE_TABLE)
        .trace());
    r.op("get_table_objects_by_name_req", o -> o.filter(ReadResultFilterKind.TABLE_COLLECTION));

    // Partition-scoped reads (db name is the first string arg, table name in arg1 for exposure).
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).expose(TableExposureMode.TABLE_ARG1),
        "get_partition", "get_partition_with_auth", "get_partition_by_name",
        "get_partitions", "get_partitions_with_auth", "get_partitions_pspec",
        "get_partition_names", "get_partitions_ps", "get_partitions_ps_with_auth",
        "get_partition_names_ps", "get_partitions_by_filter", "get_part_specs_by_filter",
        "get_num_partitions_by_filter", "get_partitions_by_names",
        "get_table_column_statistics", "get_partition_column_statistics");

    // Partition-scoped writes and bookkeeping (db name is the first string arg).
    r.op("append_partition", o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).trace());
    r.op("append_partition_by_name", o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0).trace());
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0),
        "update_creation_metadata",
        "append_partition_with_environment_context",
        "append_partition_by_name_with_environment_context",
        "drop_partition", "drop_partition_with_environment_context",
        "drop_partition_by_name", "drop_partition_by_name_with_environment_context",
        "markPartitionForEvent", "isPartitionMarkedForEvent",
        "delete_partition_column_statistics", "delete_table_column_statistics");

    // Functions and materialization rebuild locks are db-scoped (first string arg).
    r.all(o -> o.ns(NamespaceStrategy.DB_FIRST_STRING_ARG0),
        "drop_function", "alter_function", "get_functions", "get_function",
        "get_lock_materialization_rebuild", "heartbeat_lock_materialization_rebuild");

    // Reads that are safe to fan out across all backends.
    r.all(o -> o.safeFanout(), "get_all_databases", "get_databases", "get_table_meta");

    // HDP-adapted request RPCs (argument-envelope translation only).
    r.all(o -> o.hdp(),
        "get_database_req", "create_table_req", "truncate_table_req",
        "alter_table_req", "alter_partitions_req", "rename_partition_req",
        "update_table_column_statistics_req", "update_partition_column_statistics_req",
        "get_partitions_by_names_req");

    // Writes we trace for audit/observability but have no other policy overrides.
    r.all(o -> o.trace(),
        "rollback_txn",
        "alter_table", "alter_table_with_environment_context",
        "add_partition", "add_partitions", "add_partitions_req",
        "alter_partition", "alter_partitions", "rename_partition",
        "set_aggr_stats_for",
        "update_table_column_statistics", "update_partition_column_statistics");

    return r.freeze();
  }

  private static HmsOperationClass deriveHmsOperationClass(String methodName) {
    if (methodName == null || methodName.isBlank()) {
      return HmsOperationClass.ADMIN_INTROSPECTION;
    }
    String normalized = canonicalizeForPrefixMatch(methodName);
    for (String prefix : READ_PREFIXES) {
      if (normalized.startsWith(prefix)) {
        return HmsOperationClass.METADATA_READ;
      }
    }
    for (String prefix : WRITE_PREFIXES) {
      if (normalized.startsWith(prefix)) {
        return HmsOperationClass.METADATA_WRITE;
      }
    }
    return HmsOperationClass.ADMIN_INTROSPECTION;
  }

  private static boolean deriveMutation(String methodName) {
    if (methodName == null || methodName.isBlank()) {
      return false;
    }
    String normalized = canonicalizeForPrefixMatch(methodName);
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
      HmsOperationClass operationClass,
      Policy defaultBackendPolicy
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

  private static final class Registry {
    private final Map<String, OperationMetadata> entries = new LinkedHashMap<>();

    void op(String method, Consumer<OpBuilder> configurator) {
      OpBuilder builder = new OpBuilder(method);
      configurator.accept(builder);
      if (entries.put(method, builder.build()) != null) {
        throw new IllegalStateException("Duplicate operation registered: " + method);
      }
    }

    void all(Consumer<OpBuilder> configurator, String... methods) {
      for (String method : methods) {
        op(method, configurator);
      }
    }

    Map<String, OperationMetadata> freeze() {
      return Map.copyOf(entries);
    }
  }

  private static final class OpBuilder {
    private final String method;
    private HmsOperationClass operationClass;
    private Boolean mutatingOverride;
    private NamespaceStrategy namespaceStrategy;
    private TableExposureMode tableExposureMode = TableExposureMode.NONE;
    private ReadResultFilterKind readResultFilterKind = ReadResultFilterKind.NONE;
    private Policy defaultBackendPolicy;
    private boolean safeFanout;
    private boolean hdpAdapted;
    private boolean trace;

    OpBuilder(String method) {
      this.method = method;
    }

    OpBuilder cls(HmsOperationClass value) {
      this.operationClass = value;
      return this;
    }

    OpBuilder ns(NamespaceStrategy value) {
      this.namespaceStrategy = value;
      return this;
    }

    OpBuilder expose(TableExposureMode value) {
      this.tableExposureMode = value;
      return this;
    }

    OpBuilder filter(ReadResultFilterKind value) {
      this.readResultFilterKind = value;
      return this;
    }

    OpBuilder backend(Policy value) {
      this.defaultBackendPolicy = value;
      return this;
    }

    OpBuilder mutating() {
      this.mutatingOverride = Boolean.TRUE;
      return this;
    }

    OpBuilder nonMutating() {
      this.mutatingOverride = Boolean.FALSE;
      return this;
    }

    OpBuilder trace() {
      this.trace = true;
      return this;
    }

    OpBuilder safeFanout() {
      this.safeFanout = true;
      return this;
    }

    OpBuilder hdp() {
      this.hdpAdapted = true;
      return this;
    }

    OperationMetadata build() {
      HmsOperationClass resolvedClass = operationClass != null
          ? operationClass
          : deriveHmsOperationClass(method);
      boolean resolvedMutating = mutatingOverride != null
          ? mutatingOverride
          : deriveMutation(method);
      NamespaceStrategy resolvedNamespace = namespaceStrategy != null
          ? namespaceStrategy
          : deriveNamespaceStrategy(resolvedClass, defaultBackendPolicy);
      return new OperationMetadata(
          method,
          resolvedClass,
          resolvedMutating,
          trace,
          resolvedNamespace,
          tableExposureMode,
          readResultFilterKind,
          defaultBackendPolicy,
          safeFanout,
          hdpAdapted);
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
      HmsOperationClass operationClass,
      boolean mutating,
      boolean trace,
      NamespaceStrategy namespaceStrategy,
      TableExposureMode tableExposureMode,
      ReadResultFilterKind readResultFilterKind,
      Policy defaultBackendPolicy,
      boolean safeFanout,
      boolean hdpAdapted
  ) {
    public Optional<Policy> defaultBackendPolicyOptional() {
      return Optional.ofNullable(defaultBackendPolicy);
    }
  }
}
