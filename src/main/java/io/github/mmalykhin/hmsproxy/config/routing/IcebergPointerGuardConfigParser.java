package io.github.mmalykhin.hmsproxy.config.routing;

import io.github.mmalykhin.hmsproxy.config.PropertyReader;

public final class IcebergPointerGuardConfigParser {
  private IcebergPointerGuardConfigParser() {
  }

  public static IcebergPointerGuardConfig parse(PropertyReader reader) {
    return new IcebergPointerGuardConfig(
        reader.getBoolean("routing.iceberg-pointer-guard.enabled", true),
        // Zero is a documented value - it disables the cache so every alter reads the record.
        reader.getNonNegativeLong("routing.iceberg-pointer-guard.table-cache-ttl-ms", 30_000L),
        reader.getPositiveInt("routing.iceberg-pointer-guard.table-cache-max-entries", 10_000),
        reader.getBoolean("routing.iceberg-pointer-guard.lock-enabled", true),
        // Zero is a documented value - one lock attempt and no waiting.
        reader.getNonNegativeLong("routing.iceberg-pointer-guard.lock-acquire-timeout-ms", 10_000L));
  }
}
