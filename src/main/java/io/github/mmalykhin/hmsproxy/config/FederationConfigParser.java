package io.github.mmalykhin.hmsproxy.config;

import java.util.Locale;

final class FederationConfigParser {
  private FederationConfigParser() {
  }

  static FederationConfig parse(
      PropertyReader reader,
      CatalogConfig defaultCatalogConfig
  ) {
    boolean preserveBackendCatalogName = reader.getBoolean("federation.preserve-backend-catalog-name", false);
    ViewTextRewriteMode viewTextRewriteMode = parseViewTextRewriteMode(
        reader.getOrNull("federation.view-text-rewrite.mode"));
    boolean preserveOriginalViewText = reader.getBoolean(
        "federation.view-text-rewrite.preserve-original-text", false);
    ExternalTableLocationRewriteMode externalTableLocationRewriteMode =
        parseExternalTableLocationRewriteMode(reader.getOrNull("federation.external-table-location-rewrite.mode"));
    String externalTableLocationRewriteSourceDefaultFs =
        reader.getOrNull("federation.external-table-location-rewrite.source-default-fs");
    if (externalTableLocationRewriteSourceDefaultFs == null && defaultCatalogConfig != null) {
      externalTableLocationRewriteSourceDefaultFs =
          PropertyReader.trimToNull(defaultCatalogConfig.hiveConf().get("fs.defaultFS"));
    }
    ExternalTableDropPurgeMode externalTableDropPurgeMode = parseExternalTableDropPurgeMode(
        reader.getOrNull("federation.external-table-drop-purge.mode"));
    if (externalTableLocationRewriteMode
        == ExternalTableLocationRewriteMode.REWRITE_IF_SOURCE_DEFAULT_FS
        && externalTableLocationRewriteSourceDefaultFs == null) {
      throw new IllegalArgumentException(
          "Missing required property: federation.external-table-location-rewrite.source-default-fs"
              + " (or catalog." + (defaultCatalogConfig != null ? defaultCatalogConfig.name() : "<default>")
              + ".conf.fs.defaultFS)");
    }
    return new FederationConfig(
        preserveBackendCatalogName,
        viewTextRewriteMode,
        preserveOriginalViewText,
        externalTableLocationRewriteMode,
        externalTableLocationRewriteSourceDefaultFs,
        externalTableDropPurgeMode);
  }

  private static ViewTextRewriteMode parseViewTextRewriteMode(String value) {
    if (value == null) {
      return ViewTextRewriteMode.DISABLED;
    }
    try {
      return ViewTextRewriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.view-text-rewrite.mode: " + value
              + ". Expected one of: DISABLED, REWRITE",
          e);
    }
  }

  private static ExternalTableLocationRewriteMode parseExternalTableLocationRewriteMode(String value) {
    if (value == null) {
      return ExternalTableLocationRewriteMode.DISABLED;
    }
    try {
      return ExternalTableLocationRewriteMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.external-table-location-rewrite.mode: " + value
              + ". Expected one of: DISABLED, QUALIFY_UNQUALIFIED, REWRITE_IF_SOURCE_DEFAULT_FS",
          e);
    }
  }

  private static ExternalTableDropPurgeMode parseExternalTableDropPurgeMode(String value) {
    if (value == null) {
      return ExternalTableDropPurgeMode.DISABLED;
    }
    try {
      return ExternalTableDropPurgeMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid value for federation.external-table-drop-purge.mode: " + value
              + ". Expected one of: DISABLED, BEST_EFFORT",
          e);
    }
  }
}
