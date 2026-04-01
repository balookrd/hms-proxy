package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.security.ClientAddressMatcher;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

final class TransactionalTableMutationGuard {
  private static final Set<String> GUARDED_METHODS = Set.of(
      "create_table",
      "alter_table",
      "alter_table_with_environment_context");

  private final ProxyConfig.TransactionalDdlGuardConfig config;
  private final List<ClientAddressMatcher> clientAddressMatchers;

  TransactionalTableMutationGuard(ProxyConfig proxyConfig) {
    this.config = proxyConfig.transactionalDdlGuard();
    this.clientAddressMatchers = ClientAddressMatcher.parseAll(config.clientAddressRules());
  }

  void validate(String methodName, Object[] args) throws MetaException {
    if (!config.enabled() || !GUARDED_METHODS.contains(methodName) || !matchesClientAddress()) {
      return;
    }

    Table table = findTable(args);
    if (table == null || !isBlockedTransactionalMutation(table.getParameters())) {
      return;
    }

    String remoteAddress = ClientRequestContext.remoteAddress().orElse("<unknown>");
    throw new MetaException(
        "Blocking " + methodName + " for transactional table "
            + qualifiedName(table)
            + " from client " + remoteAddress
            + " by guard.transactional-ddl.* policy");
  }

  private boolean matchesClientAddress() {
    if (clientAddressMatchers.isEmpty()) {
      return true;
    }
    String remoteAddress = ClientRequestContext.remoteAddress().orElse(null);
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

  private static String qualifiedName(Table table) {
    String dbName = table.getDbName() == null || table.getDbName().isBlank() ? "<unknown_db>" : table.getDbName();
    String tableName =
        table.getTableName() == null || table.getTableName().isBlank() ? "<unknown_table>" : table.getTableName();
    return dbName + "." + tableName;
  }
}
