package io.github.mmalykhin.hmsproxy.config.ddlguard;

public enum TransactionalDdlGuardMode {
  DISABLED,
  REJECT_TRANSACTIONAL,
  REWRITE_TRANSACTIONAL_TO_EXTERNAL,
  REWRITE_TO_NON_TRANSACTIONAL,
  REWRITE_MANAGED_TO_EXTERNAL
}
