package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.MetaException;
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

    TableObjectView table = TableObjectView.findInArgs(args);
    if (table == null || !isManagedTable(table)) {
      return;
    }

    if (config.rewriteManagedToExternalEnabled()) {
      rewriteToExternal(table);
      return;
    }

    if (!isBlockedTransactionalMutation(table.parameters())) {
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

  // Prefix match covers every positional and *_req create/alter variant of the supported Ifaces
  // (including CreateTableRequest/AlterTableRequest envelopes carrying parent or child-CL Table).
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

  private static boolean isManagedTable(TableObjectView table) {
    String tableType = table.tableType();
    if (tableType == null || tableType.isBlank()) {
      Map<String, String> parameters = table.parameters();
      String external = parameters == null ? null : parameters.get("EXTERNAL");
      return external == null || !"TRUE".equalsIgnoreCase(external.trim());
    }
    return "MANAGED_TABLE".equalsIgnoreCase(tableType.trim());
  }

  private static void rewriteToNonTransactional(TableObjectView table) {
    Map<String, String> parameters = new LinkedHashMap<>();
    if (table.parameters() != null) {
      parameters.putAll(table.parameters());
    }
    parameters.remove("transactional");
    parameters.remove("transactional_properties");
    table.setParameters(parameters);
  }

  private static void rewriteToExternal(TableObjectView table) {
    table.setTableType("EXTERNAL_TABLE");
    Map<String, String> parameters = new LinkedHashMap<>();
    if (table.parameters() != null) {
      parameters.putAll(table.parameters());
    }
    parameters.remove("transactional");
    parameters.remove("transactional_properties");
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("external.table.purge", "true");
    table.setParameters(parameters);
  }

  private static String qualifiedName(TableObjectView table) {
    String dbName = table.dbName() == null || table.dbName().isBlank() ? "<unknown_db>" : table.dbName();
    String tableName =
        table.tableName() == null || table.tableName().isBlank() ? "<unknown_table>" : table.tableName();
    return dbName + "." + tableName;
  }
}
