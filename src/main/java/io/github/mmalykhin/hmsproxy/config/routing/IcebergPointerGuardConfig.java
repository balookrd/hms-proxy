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
 *
 * <p>{@code lockEnabled} and {@code lockAcquireTimeoutMs} govern the table lock the guard holds
 * across a repair, so that the read it repairs from and the write the backend applies cannot be
 * separated by another writer's commit. {@code lockAcquireTimeoutMs} is a wait budget, never a
 * refusal: a lock that is not granted in time leaves the alter merged but unprotected, because
 * refusing it would fail an ordinary Hive write whenever the metastore's lock table hiccups.
 */
public record IcebergPointerGuardConfig(
    boolean enabled,
    long tableCacheTtlMs,
    int tableCacheMaxEntries,
    boolean lockEnabled,
    long lockAcquireTimeoutMs
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
    if (lockAcquireTimeoutMs < 0L) {
      throw new IllegalArgumentException(
          "routing.iceberg-pointer-guard.lock-acquire-timeout-ms must be >= 0, got: "
              + lockAcquireTimeoutMs);
    }
  }

  public static IcebergPointerGuardConfig defaults() {
    return new IcebergPointerGuardConfig(true, 30_000L, 10_000, true, 10_000L);
  }
}
