package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class CatalogAccessModeGuard {
  private static final List<String> READ_PREFIXES = List.of("get_", "list_", "show_");
  private static final List<String> WRITE_PREFIXES = List.of(
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

  private CatalogAccessModeGuard() {
  }

  static void validate(
      ProxyConfig.CatalogConfig catalogConfig,
      String methodName,
      String backendDbName
  ) throws MetaException {
    if (!isWriteOperation(methodName)) {
      return;
    }

    ProxyConfig.CatalogAccessMode accessMode = catalogConfig.accessMode();
    switch (accessMode) {
      case READ_WRITE -> {
        return;
      }
      case READ_ONLY -> throw new MetaException(
          "Catalog '" + catalogConfig.name() + "' is READ_ONLY; write operation '" + methodName + "' is not allowed");
      case READ_WRITE_DB_WHITELIST -> {
        String normalizedDbName = normalizeDbName(backendDbName);
        if (normalizedDbName == null || normalizedDbName.isBlank()) {
          throw new MetaException(
              "Catalog '" + catalogConfig.name() + "' allows writes only for databases from catalog."
                  + catalogConfig.name()
                  + ".write-db-whitelist; operation '" + methodName + "' does not carry a database name");
        }
        if (!catalogConfig.writeDbWhitelist().contains(normalizedDbName)) {
          throw new MetaException(
              "Catalog '" + catalogConfig.name() + "' allows writes only for whitelisted databases; database '"
                  + normalizedDbName
                  + "' is not allowed for operation '" + methodName + "'");
        }
      }
    }
  }

  static boolean isWriteOperation(String methodName) {
    if (methodName == null || methodName.isBlank()) {
      return false;
    }
    String normalized = methodName.toLowerCase(Locale.ROOT);
    if (normalized.equals("addwritenotificationlog")) {
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

  private static String normalizeDbName(String backendDbName) {
    if (backendDbName == null) {
      return null;
    }
    String normalized = backendDbName.trim();
    return normalized.isEmpty() ? null : normalized;
  }
}
