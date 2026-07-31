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
    boolean hiveEngineDescriptor
) {
  public RestCatalogConfig {
    purgeMode = purgeMode == null ? RestCatalogPurgeMode.ALLOW : purgeMode;
    purgeAllowedPrefixes = purgeAllowedPrefixes == null ? List.of() : List.copyOf(purgeAllowedPrefixes);
  }

  public static RestCatalogConfig disabled() {
    return new RestCatalogConfig(
        false, "0.0.0.0", 8181, 8, 64, null, null, RestCatalogPurgeMode.ALLOW, List.of(), true);
  }

  public boolean kerberosEnabled() {
    return kerberosPrincipal != null && kerberosKeytab != null;
  }
}
