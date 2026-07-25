package io.github.mmalykhin.hmsproxy.config.routing;

import java.util.Map;

/**
 * Raw {@code backend.conf.*} defaults, kept for config cloning (additional frontend listeners) and
 * diagnostics. It carries no runtime behavior of its own: these values are already merged into every
 * {@code CatalogConfig.hiveConf}, which is what backends actually read.
 */
public record BackendConfig(
    Map<String, String> hiveConf
) {
  public BackendConfig {
    hiveConf = Map.copyOf(hiveConf);
  }
}
