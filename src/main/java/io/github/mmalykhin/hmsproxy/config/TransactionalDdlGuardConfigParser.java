package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import java.util.Arrays;
import java.util.Locale;

final class TransactionalDdlGuardConfigParser {
  private TransactionalDdlGuardConfigParser() {
  }

  static TransactionalDdlGuardConfig parse(PropertyReader reader) {
    TransactionalDdlGuardMode mode = parseMode(reader.getOrNull("guard.transactional-ddl.mode"));
    String[] clientAddresses = PropertyReader.splitCsv(reader.get("guard.transactional-ddl.client-addresses", ""));
    ClientAddressMatcher.parseAll(Arrays.asList(clientAddresses));
    return new TransactionalDdlGuardConfig(mode, Arrays.asList(clientAddresses));
  }

  private static TransactionalDdlGuardMode parseMode(String value) {
    if (value == null) {
      return TransactionalDdlGuardMode.DISABLED;
    }
    try {
      return TransactionalDdlGuardMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for guard.transactional-ddl.mode: " + value
              + ". Expected one of: REJECT_TRANSACTIONAL, REWRITE_TRANSACTIONAL_TO_EXTERNAL,"
              + " REWRITE_TO_NON_TRANSACTIONAL, REWRITE_MANAGED_TO_EXTERNAL", e);
    }
  }
}
