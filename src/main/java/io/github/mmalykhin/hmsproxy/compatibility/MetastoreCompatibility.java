package io.github.mmalykhin.hmsproxy.compatibility;

import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  private static final List<String> DEFAULT_BACKEND_GLOBAL_METHODS = List.of(
      "set_ugi",
      "get_current_notificationEventId",
      "get_next_notification",
      "get_notification_events_count",
      "get_open_txns",
      "get_open_txns_info",
      "open_txns",
      "commit_txn",
      "abort_txn",
      "abort_txns",
      "check_lock",
      "unlock",
      "heartbeat",
      "heartbeat_txn_range",
      "show_locks",
      "show_compact",
      "get_active_resource_plan",
      "get_all_resource_plans",
      "get_runtime_stats",
      "flushCache"
  );
  private static final Map<String, LocalMethodHandler> LOCAL_HANDLERS = buildLocalHandlers();
  private static final Map<String, Supplier<Object>> FALLBACKS = buildFallbacks();

  private MetastoreCompatibility() {
  }

  public static boolean routesToDefaultBackend(String methodName) {
    return DEFAULT_BACKEND_GLOBAL_METHODS.contains(methodName);
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
    if (!hasFallback(methodName)) {
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
    return Optional.of(FALLBACKS.get(methodName).get());
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
    return Map.copyOf(handlers);
  }

  private static Map<String, Supplier<Object>> buildFallbacks() {
    Map<String, Supplier<Object>> fallbacks = new LinkedHashMap<>();
    fallbacks.put("get_current_notificationEventId", () -> new CurrentNotificationEventId(0L));
    fallbacks.put("get_next_notification", () -> new NotificationEventResponse(Collections.emptyList()));
    fallbacks.put("get_notification_events_count", () -> new NotificationEventsCountResponse(0L));
    fallbacks.put("refresh_privileges", () -> {
      GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
      response.setSuccess(true);
      return response;
    });
    fallbacks.put("get_role_names", Collections::emptyList);
    fallbacks.put("list_privileges", Collections::emptyList);
    fallbacks.put("get_all_token_identifiers", Collections::emptyList);
    fallbacks.put("get_master_keys", Collections::emptyList);
    fallbacks.put("get_runtime_stats", Collections::emptyList);
    fallbacks.put("get_principals_in_role", () -> new GetPrincipalsInRoleResponse(Collections.emptyList()));
    fallbacks.put("get_role_grants_for_principal", () -> new GetRoleGrantsForPrincipalResponse(Collections.emptyList()));
    fallbacks.put("get_privilege_set", () -> new PrincipalPrivilegeSet(Map.of(), Map.of(), Map.of()));
    fallbacks.put("get_open_txns", () -> new GetOpenTxnsResponse(0L, Collections.emptyList(), ByteBuffer.allocate(0)));
    fallbacks.put("get_open_txns_info", () -> new GetOpenTxnsInfoResponse(0L, Collections.emptyList()));
    fallbacks.put("show_locks", () -> new ShowLocksResponse(Collections.emptyList()));
    fallbacks.put("show_compact", () -> new ShowCompactResponse(Collections.emptyList()));
    fallbacks.put("get_active_resource_plan", WMGetActiveResourcePlanResponse::new);
    fallbacks.put("get_all_resource_plans", () -> {
      WMGetAllResourcePlanResponse response = new WMGetAllResourcePlanResponse();
      response.setResourcePlans(Collections.emptyList());
      return response;
    });
    return Map.copyOf(fallbacks);
  }

  private static FrontDoorSecurity requireFrontDoorSecurity(FrontDoorSecurity frontDoorSecurity) throws MetaException {
    if (frontDoorSecurity == null) {
      throw new MetaException(FRONT_DOOR_TOKEN_ERROR);
    }
    return frontDoorSecurity;
  }

  private static Optional<CompatibleConfigKey> resolveCompatibleConfigKey(String requestedName) {
    List<CompatibleConfigKey> suffixMatches = new ArrayList<>();
    for (MetastoreConf.ConfVars confVar : MetastoreConf.ConfVars.values()) {
      CompatibleConfigKey candidate = new CompatibleConfigKey(
          confVar.getVarname(),
          confVar.getHiveName(),
          defaultValueAsString(confVar.getDefaultVal()));
      if (requestedName.equals(candidate.metastoreName()) || requestedName.equals(candidate.hiveName())) {
        return Optional.of(candidate);
      }
      if (candidate.metastoreName().endsWith("." + requestedName)
          || candidate.hiveName().endsWith("." + requestedName)) {
        suffixMatches.add(candidate);
      }
    }

    return suffixMatches.size() == 1 ? Optional.of(suffixMatches.get(0)) : Optional.empty();
  }

  private static String defaultValueAsString(Object defaultValue) {
    return defaultValue == null ? null : String.valueOf(defaultValue);
  }

  @FunctionalInterface
  private interface LocalMethodHandler {
    Object handle(Object[] args, FrontDoorSecurity frontDoorSecurity) throws Exception;
  }

  public enum BackendProfile {
    MODERN_REQUESTS,
    HORTONWORKS_3_1_0_LEGACY_REQUESTS
  }

  private record CompatibleConfigKey(String metastoreName, String hiveName, String defaultValue) {
  }
}
