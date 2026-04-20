package io.github.mmalykhin.hmsproxy.config;

import java.util.Map;

public record BackendConfig(
    Map<String, String> hiveConf
) {
  public BackendConfig {
    hiveConf = Map.copyOf(hiveConf);
  }
}
