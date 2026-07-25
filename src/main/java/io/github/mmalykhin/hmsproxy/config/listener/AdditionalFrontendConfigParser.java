package io.github.mmalykhin.hmsproxy.config.listener;

import io.github.mmalykhin.hmsproxy.config.ConfigParsing;
import io.github.mmalykhin.hmsproxy.config.PropertyReader;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
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

  public static List<AdditionalFrontendConfig> parse(
      PropertyReader reader,
      ServerConfig primary,
      ManagementConfig management
  ) {
    String raw = reader.getOrNull(LIST_KEY);
    if (raw == null) {
      return List.of();
    }
    String[] names = PropertyReader.splitCsv(raw);
    Set<String> seenNames = new LinkedHashSet<>();
    List<AdditionalFrontendConfig> result = new ArrayList<>();
    for (String name : names) {
      if (!seenNames.add(name)) {
        throw new IllegalArgumentException("Duplicate additional-frontends entry: " + name);
      }
      AdditionalFrontendConfig entry = parseOne(reader, primary, name);
      requireFreeBinding(name, entry, primary.bindHost(), primary.port(), "the primary listener");
      if (management.enabled()) {
        requireFreeBinding(
            name, entry, management.bindHost(), management.port(), "the management listener");
      }
      for (AdditionalFrontendConfig other : result) {
        requireFreeBinding(
            name, entry, other.bindHost(), other.port(), "additional-frontends." + other.name());
      }
      result.add(entry);
    }
    return List.copyOf(result);
  }

  private static void requireFreeBinding(
      String name,
      AdditionalFrontendConfig entry,
      String otherBindHost,
      int otherPort,
      String otherDescription
  ) {
    if (ConfigParsing.bindingsConflict(entry.bindHost(), entry.port(), otherBindHost, otherPort)) {
      throw new IllegalArgumentException(
          "additional-frontends." + name + " binds "
              + ConfigParsing.describeBinding(entry.bindHost(), entry.port())
              + ", which conflicts with " + otherDescription + " on "
              + ConfigParsing.describeBinding(otherBindHost, otherPort));
    }
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
    FrontendProfile profile = ConfigParsing.parseEnum(
        FrontendProfile.class,
        reader.require(scope + "frontend-profile"),
        scope + "frontend-profile");
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
