package io.github.mmalykhin.hmsproxy.config.operation;

import io.github.mmalykhin.hmsproxy.config.catalog.NamespaceStrategy;
import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy.Policy;
import java.util.Set;

final class HmsMethodNameHeuristics {
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

  private HmsMethodNameHeuristics() {
  }

  static HmsOperationClass deriveOperationClass(String methodName) {
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

  static boolean deriveMutation(String methodName) {
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

  static NamespaceStrategy deriveNamespaceStrategy(
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

  static String normalizeMethod(String methodName) {
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
}
