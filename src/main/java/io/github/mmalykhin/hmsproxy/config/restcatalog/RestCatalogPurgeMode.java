package io.github.mmalykhin.hmsproxy.config.restcatalog;

/** What the REST front door does with {@code DELETE ...?purgeRequested=true}. */
public enum RestCatalogPurgeMode {
  /** Delete whatever the table's metadata and manifests point at - the Iceberg REST default. */
  ALLOW,
  /** Delete only under rest-catalog.purge.allowed-prefixes; refuse or skip anything else. */
  ALLOWLIST,
  /** Refuse every purge request with 403; a drop without the parameter still works. */
  REFUSE
}
