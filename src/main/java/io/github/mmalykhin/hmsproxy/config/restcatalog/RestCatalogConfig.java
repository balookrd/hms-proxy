package io.github.mmalykhin.hmsproxy.config.restcatalog;

import java.util.List;

/**
 * Configuration for the Iceberg REST catalog front door.
 *
 * <p>{@code hiveEngineDescriptor} makes the proxy's REST commits write the Hive-engine storage
 * descriptor. Iceberg's HiveTableOperations otherwise rewrites the table with
 * {@code FileInputFormat}, {@code FileOutputFormat} and {@code LazySimpleSerDe} and drops
 * {@code storage_handler}, which leaves a Hive-created Iceberg table unreadable by the 3.1 line
 * after a single REST append. A table that sets {@code engine.hive.enabled} itself keeps its own
 * choice: the table property takes precedence over this configuration.
 */
public record RestCatalogConfig(
    boolean enabled,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads,
    String kerberosPrincipal,
    String kerberosKeytab,
    RestCatalogPurgeMode purgeMode,
    List<String> purgeAllowedPrefixes,
    boolean hiveEngineDescriptor,
    long maxRequestBodyBytes,
    List<String> fileIoAllowedPrefixes,
    long maxMetadataFileBytes
) {
  // Request bodies are parsed into memory whole, one per worker thread, so the cap bounds what a
  // single client can make the proxy hold; Iceberg commits are far below it.
  public static final long DEFAULT_MAX_REQUEST_BODY_BYTES = 16L * 1024L * 1024L;
  // Cap on metadata/manifest/table-file reads initiated by Iceberg REST operations (64 MiB; 0 disables).
  public static final long DEFAULT_MAX_METADATA_FILE_BYTES = 64L * 1024L * 1024L;

  public RestCatalogConfig(
      boolean enabled,
      String bindHost,
      int port,
      int minWorkerThreads,
      int maxWorkerThreads,
      String kerberosPrincipal,
      String kerberosKeytab,
      RestCatalogPurgeMode purgeMode,
      List<String> purgeAllowedPrefixes,
      boolean hiveEngineDescriptor,
      long maxRequestBodyBytes
  ) {
    this(enabled, bindHost, port, minWorkerThreads, maxWorkerThreads, kerberosPrincipal, kerberosKeytab,
        purgeMode, purgeAllowedPrefixes, hiveEngineDescriptor, maxRequestBodyBytes,
        List.of(), DEFAULT_MAX_METADATA_FILE_BYTES);
  }

  public RestCatalogConfig(
      boolean enabled,
      String bindHost,
      int port,
      int minWorkerThreads,
      int maxWorkerThreads,
      String kerberosPrincipal,
      String kerberosKeytab,
      RestCatalogPurgeMode purgeMode,
      List<String> purgeAllowedPrefixes,
      boolean hiveEngineDescriptor
  ) {
    this(enabled, bindHost, port, minWorkerThreads, maxWorkerThreads, kerberosPrincipal, kerberosKeytab,
        purgeMode, purgeAllowedPrefixes, hiveEngineDescriptor, DEFAULT_MAX_REQUEST_BODY_BYTES,
        List.of(), DEFAULT_MAX_METADATA_FILE_BYTES);
  }

  public RestCatalogConfig {
    if (maxRequestBodyBytes <= 0L) {
      throw new IllegalArgumentException(
          "rest-catalog.max-request-body-bytes must be > 0, got: " + maxRequestBodyBytes);
    }
    if (maxMetadataFileBytes < 0L) {
      throw new IllegalArgumentException(
          "rest-catalog.max-metadata-file-bytes must be >= 0, got: " + maxMetadataFileBytes);
    }
    purgeMode = purgeMode == null ? RestCatalogPurgeMode.ALLOW : purgeMode;
    purgeAllowedPrefixes = purgeAllowedPrefixes == null ? List.of() : List.copyOf(purgeAllowedPrefixes);
    fileIoAllowedPrefixes = fileIoAllowedPrefixes == null ? List.of() : List.copyOf(fileIoAllowedPrefixes);
  }

  public static RestCatalogConfig disabled() {
    return new RestCatalogConfig(
        false, "0.0.0.0", 8181, 8, 64, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true);
  }

  public boolean kerberosEnabled() {
    return kerberosPrincipal != null && kerberosKeytab != null;
  }
}
