package io.github.mmalykhin.hmsproxy.config.federation;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableDropPurgeMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
public final class FederationConfigParser {
  private FederationConfigParser() {
  }

  public static FederationConfig parse(
      PropertyReader reader,
      CatalogConfig defaultCatalogConfig
  ) {
    boolean preserveBackendCatalogName = reader.getBoolean("federation.preserve-backend-catalog-name", false);
    ViewTextRewriteMode viewTextRewriteMode = parseViewTextRewriteMode(
        reader.getOrNull("federation.view-text-rewrite.mode"));
    // Conservative default: the client-facing view definition is never mutated unless asked for.
    boolean preserveOriginalViewText = reader.getBoolean(
        "federation.view-text-rewrite.preserve-original-text", true);
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
    return ConfigParsing.parseEnum(
        ViewTextRewriteMode.class,
        value,
        "federation.view-text-rewrite.mode",
        ViewTextRewriteMode.DISABLED);
  }

  private static ExternalTableLocationRewriteMode parseExternalTableLocationRewriteMode(String value) {
    return ConfigParsing.parseEnum(
        ExternalTableLocationRewriteMode.class,
        value,
        "federation.external-table-location-rewrite.mode",
        ExternalTableLocationRewriteMode.DISABLED);
  }

  private static ExternalTableDropPurgeMode parseExternalTableDropPurgeMode(String value) {
    return ConfigParsing.parseEnum(
        ExternalTableDropPurgeMode.class,
        value,
        "federation.external-table-drop-purge.mode",
        ExternalTableDropPurgeMode.DISABLED);
  }
}
