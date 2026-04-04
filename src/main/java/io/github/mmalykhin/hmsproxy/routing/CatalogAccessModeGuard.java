package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class CatalogAccessModeGuard {
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
    return HmsOperationRegistry.describe(methodName).mutating();
  }

  private static String normalizeDbName(String backendDbName) {
    if (backendDbName == null) {
      return null;
    }
    String normalized = backendDbName.trim();
    return normalized.isEmpty() ? null : normalized;
  }
}
