package io.github.mmalykhin.hmsproxy.config.federation;


import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableDropPurgeMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
public record FederationConfig(
    boolean preserveBackendCatalogName,
    ViewTextRewriteMode viewTextRewriteMode,
    boolean preserveOriginalViewText,
    ExternalTableLocationRewriteMode externalTableLocationRewriteMode,
    String externalTableLocationRewriteSourceDefaultFs,
    ExternalTableDropPurgeMode externalTableDropPurgeMode
) {
  public FederationConfig {
    viewTextRewriteMode = viewTextRewriteMode == null ? ViewTextRewriteMode.DISABLED : viewTextRewriteMode;
    externalTableLocationRewriteMode = externalTableLocationRewriteMode == null
        ? ExternalTableLocationRewriteMode.DISABLED
        : externalTableLocationRewriteMode;
    externalTableDropPurgeMode = externalTableDropPurgeMode == null
        ? ExternalTableDropPurgeMode.DISABLED
        : externalTableDropPurgeMode;
    if (externalTableLocationRewriteSourceDefaultFs != null) {
      externalTableLocationRewriteSourceDefaultFs = externalTableLocationRewriteSourceDefaultFs.trim();
      if (externalTableLocationRewriteSourceDefaultFs.isEmpty()) {
        externalTableLocationRewriteSourceDefaultFs = null;
      }
    }
  }

  public FederationConfig(
      boolean preserveBackendCatalogName,
      ViewTextRewriteMode viewTextRewriteMode,
      boolean preserveOriginalViewText
  ) {
    this(
        preserveBackendCatalogName,
        viewTextRewriteMode,
        preserveOriginalViewText,
        ExternalTableLocationRewriteMode.DISABLED,
        null,
        ExternalTableDropPurgeMode.DISABLED);
  }

  public FederationConfig(
      boolean preserveBackendCatalogName,
      ViewTextRewriteMode viewTextRewriteMode,
      boolean preserveOriginalViewText,
      ExternalTableLocationRewriteMode externalTableLocationRewriteMode,
      String externalTableLocationRewriteSourceDefaultFs
  ) {
    this(
        preserveBackendCatalogName,
        viewTextRewriteMode,
        preserveOriginalViewText,
        externalTableLocationRewriteMode,
        externalTableLocationRewriteSourceDefaultFs,
        ExternalTableDropPurgeMode.DISABLED);
  }

  public boolean viewTextRewriteEnabled() {
    return viewTextRewriteMode == ViewTextRewriteMode.REWRITE;
  }

  public boolean externalTableDropPurgeEnabled() {
    return externalTableDropPurgeMode == ExternalTableDropPurgeMode.BEST_EFFORT;
  }
}
