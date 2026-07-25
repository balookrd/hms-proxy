package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;

public final class MetastoreCompatibility {
  private static final String FRONT_DOOR_TOKEN_ERROR =
      "Delegation tokens require Kerberos/SASL on the proxy front door";
  private static final Pattern BACKEND_VISIBLE_CONFIG_PATTERN =
      Pattern.compile("(hive|hdfs|mapred|metastore).*");
  private static final Map<String, LocalMethodHandler> LOCAL_HANDLERS = buildLocalHandlers();
  private static final Map<String, CompatibilityFallback> FALLBACKS = buildFallbacks();
  private static final Map<String, CompatibleConfigKey> CONFIG_KEY_EXACT_INDEX;
  private static final Map<String, List<CompatibleConfigKey>> CONFIG_KEY_SUFFIX_INDEX;

  static {
    Map<String, CompatibleConfigKey> exact = new HashMap<>();
    Map<String, List<CompatibleConfigKey>> suffix = new HashMap<>();
    for (MetastoreConf.ConfVars confVar : MetastoreConf.ConfVars.values()) {
      CompatibleConfigKey key = new CompatibleConfigKey(
          confVar.getVarname(),
          confVar.getHiveName(),
          defaultValueAsString(confVar.getDefaultVal()));
      exact.put(key.metastoreName(), key);
      exact.put(key.hiveName(), key);
      // Index every trailing suffix of both names, but count each ConfVar at most once
      // per suffix to preserve the "unique match" semantics of the original scan.
      Set<String> seenSuffixes = new HashSet<>();
      indexAllSuffixes(suffix, key.metastoreName(), key, seenSuffixes);
      indexAllSuffixes(suffix, key.hiveName(), key, seenSuffixes);
    }
    CONFIG_KEY_EXACT_INDEX = Map.copyOf(exact);
    CONFIG_KEY_SUFFIX_INDEX = Map.copyOf(suffix);
  }

  private static void indexAllSuffixes(
      Map<String, List<CompatibleConfigKey>> index,
      String name,
      CompatibleConfigKey key,
      Set<String> seenSuffixes
  ) {
    int dot = name.indexOf('.');
    while (dot >= 0 && dot + 1 < name.length()) {
      String sfx = name.substring(dot + 1);
      if (seenSuffixes.add(sfx)) {
        index.computeIfAbsent(sfx, ignored -> new ArrayList<>()).add(key);
      }
      dot = name.indexOf('.', dot + 1);
    }
  }

  private MetastoreCompatibility() {
  }

  public static boolean handlesLocally(String methodName) {
    return LOCAL_HANDLERS.containsKey(methodName);
  }

  public static Object handleLocally(String methodName, Object[] args, FrontDoorSecurity frontDoorSecurity) throws Exception {
    LocalMethodHandler handler = LOCAL_HANDLERS.get(methodName);
    if (handler == null) {
      throw new IllegalArgumentException("Unsupported local compatibility method: " + methodName);
    }
    return handler.handle(args, frontDoorSecurity);
  }

  public static boolean hasFallback(String methodName) {
    return FALLBACKS.containsKey(methodName);
  }

  public static boolean shouldUseFallback(String methodName, Throwable cause) {
    CompatibilityFallback fallback = FALLBACKS.get(methodName);
    if (fallback == null) {
      return false;
    }
    if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
      return true;
    }
    if (fallback.trigger() == FallbackTrigger.MISSING_METHOD_ONLY) {
      // An empty ACID/lock/privilege/notification answer is indistinguishable from real state,
      // so a transient backend failure must surface as an error instead of a synthetic success.
      return false;
    }
    return cause instanceof TApplicationException
        || cause instanceof TTransportException
        || cause instanceof MetaException;
  }

  public static Optional<Object> fallback(String methodName, Throwable cause) {
    if (!shouldUseFallback(methodName, cause)) {
      return Optional.empty();
    }
    return Optional.of(FALLBACKS.get(methodName).response().get());
  }

  public static Optional<String> compatibleConfigValue(
      String requestedName,
      String defaultValue,
      Map<String, String> hiveConf
  ) {
    if (requestedName == null) {
      return Optional.ofNullable(defaultValue);
    }
    if (BACKEND_VISIBLE_CONFIG_PATTERN.matcher(requestedName).matches()) {
      return Optional.empty();
    }

    CompatibleConfigKey compatibleKey = resolveCompatibleConfigKey(requestedName).orElse(null);
    if (compatibleKey == null) {
      return Optional.empty();
    }

    String configuredValue = hiveConf.get(requestedName);
    if (configuredValue != null) {
      return Optional.of(configuredValue);
    }
    configuredValue = hiveConf.get(compatibleKey.metastoreName());
    if (configuredValue != null) {
      return Optional.of(configuredValue);
    }
    configuredValue = hiveConf.get(compatibleKey.hiveName());
    if (configuredValue != null) {
      return Optional.of(configuredValue);
    }

    String fallbackValue = defaultValue != null ? defaultValue : compatibleKey.defaultValue();
    return Optional.of(fallbackValue == null ? "" : fallbackValue);
  }

  private static Map<String, LocalMethodHandler> buildLocalHandlers() {
    Map<String, LocalMethodHandler> handlers = new LinkedHashMap<>();
    handlers.put("get_delegation_token", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).issueDelegationToken((String) args[0], (String) args[1]));
    handlers.put("renew_delegation_token", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).renewDelegationToken((String) args[0]));
    handlers.put("cancel_delegation_token", (args, frontDoorSecurity) -> {
      requireFrontDoorSecurity(frontDoorSecurity).cancelDelegationToken((String) args[0]);
      return null;
    });
    handlers.put("add_token", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).addToken((String) args[0], (String) args[1]));
    handlers.put("remove_token", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).removeToken((String) args[0]));
    handlers.put("get_token", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).getToken((String) args[0]));
    handlers.put("get_all_token_identifiers", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).getAllTokenIdentifiers());
    handlers.put("add_master_key", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).addMasterKey((String) args[0]));
    handlers.put("update_master_key", (args, frontDoorSecurity) -> {
      requireFrontDoorSecurity(frontDoorSecurity).updateMasterKey((Integer) args[0], (String) args[1]);
      return null;
    });
    handlers.put("remove_master_key", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).removeMasterKey((Integer) args[0]));
    handlers.put("get_master_keys", (args, frontDoorSecurity) ->
        requireFrontDoorSecurity(frontDoorSecurity).getMasterKeys());
    return Map.copyOf(handlers);
  }

  private static Map<String, CompatibilityFallback> buildFallbacks() {
    Map<String, CompatibilityFallback> fallbacks = new LinkedHashMap<>();
    // Notification state: an empty answer makes a replication client believe it is caught up.
    putMissingMethodOnly(fallbacks, "get_current_notificationEventId", () -> new CurrentNotificationEventId(0L));
    putMissingMethodOnly(fallbacks, "get_next_notification",
        () -> new NotificationEventResponse(Collections.emptyList()));
    putMissingMethodOnly(fallbacks, "get_notification_events_count", () -> new NotificationEventsCountResponse(0L));
    // Privilege/role state: an empty or successful answer is an authorization statement.
    putMissingMethodOnly(fallbacks, "refresh_privileges", () -> {
      GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
      response.setSuccess(true);
      return response;
    });
    putMissingMethodOnly(fallbacks, "get_role_names", Collections::emptyList);
    putMissingMethodOnly(fallbacks, "list_privileges", Collections::emptyList);
    putMissingMethodOnly(fallbacks, "get_all_token_identifiers", Collections::emptyList);
    putMissingMethodOnly(fallbacks, "get_master_keys", Collections::emptyList);
    putMissingMethodOnly(fallbacks, "get_principals_in_role",
        () -> new GetPrincipalsInRoleResponse(Collections.emptyList()));
    putMissingMethodOnly(fallbacks, "get_role_grants_for_principal",
        () -> new GetRoleGrantsForPrincipalResponse(Collections.emptyList()));
    putMissingMethodOnly(fallbacks, "get_privilege_set",
        () -> new PrincipalPrivilegeSet(Map.of(), Map.of(), Map.of()));
    // ACID/lock state: "no open transactions" turns uncommitted or aborted deltas into valid data.
    putMissingMethodOnly(fallbacks, "get_open_txns",
        () -> new GetOpenTxnsResponse(0L, Collections.emptyList(), ByteBuffer.allocate(0)));
    putMissingMethodOnly(fallbacks, "get_open_txns_info",
        () -> new GetOpenTxnsInfoResponse(0L, Collections.emptyList()));
    putMissingMethodOnly(fallbacks, "show_locks", () -> new ShowLocksResponse(Collections.emptyList()));
    putMissingMethodOnly(fallbacks, "show_compact", () -> new ShowCompactResponse(Collections.emptyList()));
    // Optional service features: an empty answer only hides diagnostics, never data semantics.
    putBackendFailure(fallbacks, "get_runtime_stats", Collections::emptyList);
    putBackendFailure(fallbacks, "get_active_resource_plan", WMGetActiveResourcePlanResponse::new);
    putBackendFailure(fallbacks, "get_all_resource_plans", () -> {
      WMGetAllResourcePlanResponse response = new WMGetAllResourcePlanResponse();
      response.setResourcePlans(Collections.emptyList());
      return response;
    });
    return Map.copyOf(fallbacks);
  }

  private static void putMissingMethodOnly(
      Map<String, CompatibilityFallback> fallbacks,
      String methodName,
      Supplier<Object> response
  ) {
    fallbacks.put(methodName, new CompatibilityFallback(FallbackTrigger.MISSING_METHOD_ONLY, response));
  }

  private static void putBackendFailure(
      Map<String, CompatibilityFallback> fallbacks,
      String methodName,
      Supplier<Object> response
  ) {
    fallbacks.put(methodName, new CompatibilityFallback(FallbackTrigger.BACKEND_FAILURE, response));
  }

  private static FrontDoorSecurity requireFrontDoorSecurity(FrontDoorSecurity frontDoorSecurity) throws MetaException {
    if (frontDoorSecurity == null) {
      throw new MetaException(FRONT_DOOR_TOKEN_ERROR);
    }
    return frontDoorSecurity;
  }

  private static Optional<CompatibleConfigKey> resolveCompatibleConfigKey(String requestedName) {
    CompatibleConfigKey exact = CONFIG_KEY_EXACT_INDEX.get(requestedName);
    if (exact != null) {
      return Optional.of(exact);
    }
    List<CompatibleConfigKey> suffixMatches = CONFIG_KEY_SUFFIX_INDEX.get(requestedName);
    if (suffixMatches != null && suffixMatches.size() == 1) {
      return Optional.of(suffixMatches.get(0));
    }
    return Optional.empty();
  }

  private static String defaultValueAsString(Object defaultValue) {
    return defaultValue == null ? null : String.valueOf(defaultValue);
  }

  @FunctionalInterface
  private interface LocalMethodHandler {
    Object handle(Object[] args, FrontDoorSecurity frontDoorSecurity) throws Exception;
  }

  private enum FallbackTrigger {
    /** Synthetic response only when the backend does not implement the method at all. */
    MISSING_METHOD_ONLY,
    /** Synthetic response is also safe when the backend call fails or the backend is unavailable. */
    BACKEND_FAILURE
  }

  private record CompatibilityFallback(FallbackTrigger trigger, Supplier<Object> response) {
  }

  public enum BackendProfile {
    MODERN_REQUESTS,
    HORTONWORKS_3_1_0_LEGACY_REQUESTS
  }

  private record CompatibleConfigKey(String metastoreName, String hiveName, String defaultValue) {
  }
}
