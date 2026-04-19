package io.github.mmalykhin.hmsproxy.config;

import java.util.Locale;

public enum HmsOperationClass {
  METADATA_READ,
  METADATA_WRITE,
  SERVICE_GLOBAL_READ,
  SERVICE_GLOBAL_WRITE,
  ACID_NAMESPACE_BOUND_WRITE,
  ACID_ID_BOUND_LIFECYCLE,
  ADMIN_INTROSPECTION,
  COMPATIBILITY_ONLY_RPC;

  public String wireName() {
    return name().toLowerCase(Locale.ROOT);
  }
}
