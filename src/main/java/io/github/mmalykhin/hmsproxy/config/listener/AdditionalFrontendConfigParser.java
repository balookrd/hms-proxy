package io.github.mmalykhin.hmsproxy.config.listener;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public final class AdditionalFrontendConfigParser {
  private static final String LIST_KEY = "additional-frontends";
  private static final String PREFIX = LIST_KEY + ".";

  private AdditionalFrontendConfigParser() {
  }

  public static List<AdditionalFrontendConfig> parse(PropertyReader reader, ServerConfig primary) {
    String raw = reader.getOrNull(LIST_KEY);
    if (raw == null) {
      return List.of();
    }
    String[] names = PropertyReader.splitCsv(raw);
    Set<String> seenNames = new LinkedHashSet<>();
    Set<String> seenBindings = new LinkedHashSet<>();
    List<AdditionalFrontendConfig> result = new ArrayList<>();
    for (String name : names) {
      if (!seenNames.add(name)) {
        throw new IllegalArgumentException("Duplicate additional-frontends entry: " + name);
      }
      AdditionalFrontendConfig entry = parseOne(reader, primary, name);
      if (entry.port() == primary.port() && entry.bindHost().equals(primary.bindHost())) {
        throw new IllegalArgumentException(
            "additional-frontends." + name + " uses the same bindHost:port as the primary listener ("
                + primary.bindHost() + ":" + primary.port() + ")");
      }
      String binding = entry.bindHost() + ":" + entry.port();
      if (!seenBindings.add(binding)) {
        throw new IllegalArgumentException(
            "additional-frontends." + name + " duplicates bindHost:port " + binding
                + " of another listener");
      }
      result.add(entry);
    }
    return List.copyOf(result);
  }

  private static AdditionalFrontendConfig parseOne(PropertyReader reader, ServerConfig primary, String name) {
    String scope = PREFIX + name + ".";
    int port = reader.getInt(scope + "port", -1);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException(
          "additional-frontends." + name + ".port must be set to a value in 1..65535, got: " + port);
    }
    String bindHost = reader.get(scope + "bind-host", primary.bindHost());
    int minThreads = reader.getPositiveInt(scope + "min-worker-threads", primary.minWorkerThreads());
    int maxThreads = reader.getInt(scope + "max-worker-threads", primary.maxWorkerThreads());
    if (maxThreads < minThreads) {
      throw new IllegalArgumentException(
          "additional-frontends." + name + ".max-worker-threads (" + maxThreads
              + ") must be >= min-worker-threads (" + minThreads + ")");
    }
    String profileName = reader.require(scope + "frontend-profile");
    FrontendProfile profile;
    try {
      profile = FrontendProfile.valueOf(profileName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "additional-frontends." + name + ".frontend-profile is not a known FrontendProfile: "
              + profileName);
    }
    String jar = reader.getOrNull(scope + "standalone-metastore-jar");
    if (profile != FrontendProfile.APACHE_3_1_3) {
      if (jar == null) {
        throw new IllegalArgumentException(
            "additional-frontends." + name + ".standalone-metastore-jar is required for profile "
                + profile);
      }
      ConfigParsing.requireReadableFile(jar, "additional-frontends." + name + ".standalone-metastore-jar");
    }
    return new AdditionalFrontendConfig(name, bindHost, port, minThreads, maxThreads, profile, jar);
  }
}
