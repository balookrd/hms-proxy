package io.github.mmalykhin.hmsproxy.config;

import java.util.Locale;

final class FederationConfigParser {
  private FederationConfigParser() {
  }

  static ProxyConfig.FederationConfig parse(
      PropertyReader reader,
      ProxyConfig.CatalogConfig defaultCatalogConfig
  ) {
    boolean preserveBackendCatalogName = reader.getBoolean("federation.preserve-backend-catalog-name", false);
    ProxyConfig.ViewTextRewriteMode viewTextRewriteMode = parseViewTextRewriteMode(
        reader.getOrNull("federation.view-text-rewrite.mode"));
    boolean preserveOriginalViewText = reader.getBoolean(
        "federation.view-text-rewrite.preserve-original-text", false);
    ProxyConfig.ExternalTableLocationRewriteMode externalTableLocationRewriteMode =
        parseExternalTableLocationRewriteMode(reader.getOrNull("federation.external-table-location-rewrite.mode"));
    String externalTableLocationRewriteSourceDefaultFs =
        reader.getOrNull("federation.external-table-location-rewrite.source-default-fs");
    if (externalTableLocationRewriteSourceDefaultFs == null && defaultCatalogConfig != null) {
      externalTableLocationRewriteSourceDefaultFs =
          PropertyReader.trimToNull(defaultCatalogConfig.hiveConf().get("fs.defaultFS"));
    }
    ProxyConfig.ExternalTableDropPurgeMode externalTableDropPurgeMode = parseExternalTableDropPurgeMode(
        reader.getOrNull("federation.external-table-drop-purge.mode"));
    if (externalTableLocationRewriteMode
        == ProxyConfig.ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS
        && externalTableLocationRewriteSourceDefaultFs == null) {
      throw new IllegalArgumentException(
          "Missing required property: federation.external-table-location-rewrite.source-default-fs"
              + " (or catalog." + (defaultCatalogConfig != null ? defaultCatalogConfig.name() : "<default>")
              + ".conf.fs.defaultFS)");
    }
    return new ProxyConfig.FederationConfig(
        preserveBackendCatalogName,
        viewTextRewriteMode,
        preserveOriginalViewText,
        externalTableLocationRewriteMode,
        externalTableLocationRewriteSourceDefaultFs,
        externalTableDropPurgeMode);
  }

  private static ProxyConfig.ViewTextRewriteMode parseViewTextRewriteMode(String value) {
    if (value == null) {
      return ProxyConfig.ViewTextRewriteMode.DISABLED;
    }
    try {
      return ProxyConfig.ViewTextRewriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.view-text-rewrite.mode: " + value
              + ". Expected one of: DISABLED, REWRITE",
          e);
    }
  }

  private static ProxyConfig.ExternalTableLocationRewriteMode parseExternalTableLocationRewriteMode(String value) {
    if (value == null) {
      return ProxyConfig.ExternalTableLocationRewriteMode.DISABLED;
    }
    try {
      return ProxyConfig.ExternalTableLocationRewriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.external-table-location-rewrite.mode: " + value
              + ". Expected one of: DISABLED, QUALIFY_UNQUALIFIED, REWRITE_IF_SOURCE_DEFAULT_FS",
          e);
    }
  }

  private static ProxyConfig.ExternalTableDropPurgeMode parseExternalTableDropPurgeMode(String value) {
    if (value == null) {
      return ProxyConfig.ExternalTableDropPurgeMode.DISABLED;
    }
    try {
      return ProxyConfig.ExternalTableDropPurgeMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.external-table-drop-purge.mode: " + value
              + ". Expected one of: DISABLED, BEST_EFFORT",
          e);
    }
  }
}
