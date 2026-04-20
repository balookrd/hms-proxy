package io.github.mmalykhin.hmsproxy.config.routing;

import java.util.Map;

public record BackendConfig(
    Map<String, String> hiveConf
) {
  public BackendConfig {
    hiveConf = Map.copyOf(hiveConf);
  }
}
