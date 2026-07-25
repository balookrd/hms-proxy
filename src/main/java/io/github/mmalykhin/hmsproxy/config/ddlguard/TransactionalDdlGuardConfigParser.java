package io.github.mmalykhin.hmsproxy.config.ddlguard;

import io.github.mmalykhin.hmsproxy.util.ClientAddressMatcher;
import java.util.Arrays;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
public final class TransactionalDdlGuardConfigParser {
  private TransactionalDdlGuardConfigParser() {
  }

  public static TransactionalDdlGuardConfig parse(PropertyReader reader) {
    TransactionalDdlGuardMode mode = parseMode(reader.getOrNull("guard.transactional-ddl.mode"));
    String[] clientAddresses = PropertyReader.splitCsv(reader.get("guard.transactional-ddl.client-addresses", ""));
    ClientAddressMatcher.parseAll(Arrays.asList(clientAddresses));
    return new TransactionalDdlGuardConfig(mode, Arrays.asList(clientAddresses));
  }

  private static TransactionalDdlGuardMode parseMode(String value) {
    return ConfigParsing.parseEnum(
        TransactionalDdlGuardMode.class,
        value,
        "guard.transactional-ddl.mode",
        TransactionalDdlGuardMode.DISABLED);
  }
}
