package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;

final class TransactionalTableMutationGuard {

  private final TransactionalDdlGuardConfig config;
  private final List<ClientAddressMatcher> clientAddressMatchers;

  TransactionalTableMutationGuard(ProxyConfig proxyConfig) {
    this.config = proxyConfig.transactionalDdlGuard();
    this.clientAddressMatchers = ClientAddressMatcher.parseAll(config.clientAddressRules());
  }

  void validate(String methodName, Object[] args) throws MetaException {
    if (!config.enabled()
        || !isGuardedMethod(methodName)
        || !matchesClientAddress()) {
      return;
    }

    Table table = findTable(args);
    if (table == null || !isManagedTable(table)) {
      return;
    }

    if (config.rewriteManagedToExternalEnabled()) {
      rewriteToExternal(table);
      return;
    }

    if (!isBlockedTransactionalMutation(table.getParameters())) {
      return;
    }

    if (config.rewriteTransactionalToExternalEnabled()) {
      rewriteToExternal(table);
      return;
    }

    if (config.rewriteToNonTransactionalEnabled()) {
      rewriteToNonTransactional(table);
      return;
    }

    String remoteAddress = ClientRequestContext.remoteAddress().orElse("<unknown>");
    throw new MetaException(
        "Blocking " + methodName + " for transactional table "
            + qualifiedName(table)
            + " from client " + remoteAddress
            + " by guard.transactional-ddl.* policy");
  }

  // Prefix match covers every positional create/alter variant of the supported Ifaces
  // (create_table_with_environment_context is the RPC HiveMetaStoreClient 3.1.x actually sends
  // for createTable, plus create_table_with_constraints and alter_table_with_cascade). The
  // *_req wrappers are unwrapped into these positional RPCs by the frontend bridges before
  // this guard runs, so findTable(args) always sees the Table argument.
  private static boolean isGuardedMethod(String methodName) {
    return methodName != null
        && (methodName.startsWith("create_table") || methodName.startsWith("alter_table"));
  }

  private boolean matchesClientAddress() {
    if (clientAddressMatchers.isEmpty()) {
      return true;
    }
    byte[] remoteAddress = ClientAddressMatcher.decodeAddress(
        ClientRequestContext.remoteAddress().orElse(null));
    if (remoteAddress == null) {
      return false;
    }
    for (ClientAddressMatcher matcher : clientAddressMatchers) {
      if (matcher.matches(remoteAddress)) {
        return true;
      }
    }
    return false;
  }

  private static Table findTable(Object[] args) {
    if (args == null) {
      return null;
    }
    for (Object argument : args) {
      if (argument instanceof Table table) {
        return table;
      }
    }
    return null;
  }

  private static boolean isBlockedTransactionalMutation(Map<String, String> parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return false;
    }
    String transactional = parameters.get("transactional");
    if (transactional != null && "true".equalsIgnoreCase(transactional.trim())) {
      return true;
    }
    String transactionalProperties = parameters.get("transactional_properties");
    return transactionalProperties != null && !transactionalProperties.isBlank();
  }

  private static boolean isManagedTable(Table table) {
    String tableType = table.getTableType();
    return tableType != null && "MANAGED_TABLE".equalsIgnoreCase(tableType.trim());
  }

  private static void rewriteToNonTransactional(Table table) {
    Map<String, String> parameters = new LinkedHashMap<>();
    if (table.getParameters() != null) {
      parameters.putAll(table.getParameters());
    }
    parameters.remove("transactional");
    parameters.remove("transactional_properties");
    table.setParameters(parameters);
  }

  private static void rewriteToExternal(Table table) {
    table.setTableType("EXTERNAL_TABLE");
    Map<String, String> parameters = new LinkedHashMap<>();
    if (table.getParameters() != null) {
      parameters.putAll(table.getParameters());
    }
    parameters.remove("transactional");
    parameters.remove("transactional_properties");
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("external.table.purge", "true");
    table.setParameters(parameters);
  }

  private static String qualifiedName(Table table) {
    String dbName = table.getDbName() == null || table.getDbName().isBlank() ? "<unknown_db>" : table.getDbName();
    String tableName =
        table.getTableName() == null || table.getTableName().isBlank() ? "<unknown_table>" : table.getTableName();
    return dbName + "." + tableName;
  }
}
