package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Settings of the guard that keeps a Hive client's {@code alter_table} from erasing an Iceberg
 * table's state.
 *
 * <p>The guard decides whether a request concerns an Iceberg table from the metastore's own
 * record, which costs a {@code get_table} per {@code alter_table}. {@code tableCacheTtlMs} bounds
 * that cost by remembering, for that long, the names the metastore answered are not Iceberg
 * tables - ordinary Hive tables, which is where the volume is. Iceberg tables are never cached:
 * their current pointer has to be read fresh on every alter.
 */
public record IcebergPointerGuardConfig(
    boolean enabled,
    long tableCacheTtlMs,
    int tableCacheMaxEntries
) {
  public IcebergPointerGuardConfig {
    if (tableCacheTtlMs < 0L) {
      throw new IllegalArgumentException(
          "routing.iceberg-pointer-guard.table-cache-ttl-ms must be >= 0, got: " + tableCacheTtlMs);
    }
    if (tableCacheMaxEntries < 1) {
      throw new IllegalArgumentException(
          "routing.iceberg-pointer-guard.table-cache-max-entries must be >= 1, got: " + tableCacheMaxEntries);
    }
  }

  public static IcebergPointerGuardConfig defaults() {
    return new IcebergPointerGuardConfig(true, 30_000L, 10_000);
  }
}
