package io.github.mmalykhin.hmsproxy.config.catalog;

public enum ExternalTableLocationRewriteMode {
  DISABLED,
  QUALIFY_UNQUALIFIED,
  REWRITE_IF_SOURCE_DEFAULT_FS
}
