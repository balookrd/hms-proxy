package io.github.mmalykhin.hmsproxy.config;

import java.util.List;

public record TransactionalDdlGuardConfig(
    TransactionalDdlGuardMode mode,
    List<String> clientAddressRules
) {
  public TransactionalDdlGuardConfig {
    mode = mode == null ? TransactionalDdlGuardMode.DISABLED : mode;
    clientAddressRules = List.copyOf(clientAddressRules);
  }

  public boolean enabled() {
    return mode != TransactionalDdlGuardMode.DISABLED;
  }

  public boolean rewriteTransactionalToExternalEnabled() {
    return mode == TransactionalDdlGuardMode.REWRITE_TRANSACTIONAL_TO_EXTERNAL;
  }

  public boolean rewriteToNonTransactionalEnabled() {
    return mode == TransactionalDdlGuardMode.REWRITE_TO_NON_TRANSACTIONAL;
  }

  public boolean rewriteManagedToExternalEnabled() {
    return mode == TransactionalDdlGuardMode.REWRITE_MANAGED_TO_EXTERNAL;
  }

  public boolean rejectTransactionalEnabled() {
    return mode == TransactionalDdlGuardMode.REJECT_TRANSACTIONAL;
  }
}
