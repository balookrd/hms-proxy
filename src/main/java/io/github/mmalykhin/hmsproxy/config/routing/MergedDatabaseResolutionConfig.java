package io.github.mmalykhin.hmsproxy.config.routing;

/**
 * Negative cache of the on-demand database lookup behind {@code catalog.<name>.merge-with-default}.
 *
 * <p>An unprefixed name that is in no catalog is probed on every backend; remembering the miss for
 * {@code negativeTtlMs} keeps repeated lookups of the same unknown name off the backends. The names
 * come from clients, so the cache is capped at {@code negativeMaxEntries}.
 */
public record MergedDatabaseResolutionConfig(
    long negativeTtlMs,
    int negativeMaxEntries
) {
  public static final long DEFAULT_NEGATIVE_TTL_MS = 5_000L;
  public static final int DEFAULT_NEGATIVE_MAX_ENTRIES = 10_000;

  public MergedDatabaseResolutionConfig {
    if (negativeTtlMs < 0L) {
      throw new IllegalArgumentException(
          "routing.merge-with-default.negative-cache-ttl-ms must be >= 0, got: " + negativeTtlMs);
    }
    if (negativeMaxEntries < 1) {
      throw new IllegalArgumentException(
          "routing.merge-with-default.negative-cache-max-entries must be >= 1, got: " + negativeMaxEntries);
    }
  }

  public static MergedDatabaseResolutionConfig defaultConfig() {
    return new MergedDatabaseResolutionConfig(DEFAULT_NEGATIVE_TTL_MS, DEFAULT_NEGATIVE_MAX_ENTRIES);
  }
}
