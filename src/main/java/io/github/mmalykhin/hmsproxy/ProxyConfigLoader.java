package io.github.mmalykhin.hmsproxy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

public final class ProxyConfigLoader {
  private ProxyConfigLoader() {
  }

  public static ProxyConfig load(Path path) throws IOException {
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(path)) {
      properties.load(input);
    }

    int port = getInt(properties, "server.port", 9083);
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("server.port must be between 1 and 65535, got: " + port);
    }
    int minWorkerThreads = getInt(properties, "server.min-worker-threads", 16);
    if (minWorkerThreads < 1) {
      throw new IllegalArgumentException("server.min-worker-threads must be >= 1, got: " + minWorkerThreads);
    }
    int maxWorkerThreads = getInt(properties, "server.max-worker-threads", 256);
    if (maxWorkerThreads < minWorkerThreads) {
      throw new IllegalArgumentException(
          "server.max-worker-threads (" + maxWorkerThreads
              + ") must be >= server.min-worker-threads (" + minWorkerThreads + ")");
    }
    ProxyConfig.ServerConfig server = new ProxyConfig.ServerConfig(
        get(properties, "server.name", "hms-proxy"),
        get(properties, "server.bind-host", "0.0.0.0"),
        port,
        minWorkerThreads,
        maxWorkerThreads);

    ProxyConfig.SecurityMode securityMode = ProxyConfig.SecurityMode.valueOf(
        get(properties, "security.mode", "NONE").trim().toUpperCase());
    String serverPrincipal = trimToNull(properties.getProperty("security.server-principal"));
    String clientPrincipal = trimToNull(properties.getProperty("security.client-principal"));
    String keytab = trimToNull(properties.getProperty("security.keytab"));
    ProxyConfig.SecurityConfig security =
        new ProxyConfig.SecurityConfig(securityMode, serverPrincipal, clientPrincipal, keytab);

    String catalogsValue = require(properties, "catalogs");
    Map<String, ProxyConfig.CatalogConfig> catalogs = new LinkedHashMap<>();
    for (String catalogName : splitCsv(catalogsValue)) {
      String prefix = "catalog." + catalogName + ".";
      Map<String, String> hiveConf = properties.stringPropertyNames().stream()
          .filter(name -> name.startsWith(prefix + "conf."))
          .sorted()
          .collect(Collectors.toMap(
              name -> name.substring((prefix + "conf.").length()),
              properties::getProperty,
              (left, right) -> right,
              LinkedHashMap::new));
      if (!hiveConf.containsKey("hive.metastore.uris")) {
        throw new IllegalArgumentException(
            "Missing " + prefix + "conf.hive.metastore.uris for catalog " + catalogName);
      }
      catalogs.put(catalogName, new ProxyConfig.CatalogConfig(
          catalogName,
          get(properties, prefix + "description", catalogName),
          get(properties, prefix + "location-uri", "file:///warehouse/" + catalogName),
          hiveConf));
    }

    String defaultCatalog = trimToNull(properties.getProperty("routing.default-catalog"));
    if (defaultCatalog == null) {
      if (catalogs.size() == 1) {
        defaultCatalog = catalogs.keySet().iterator().next();
      } else {
        throw new IllegalArgumentException(
            "routing.default-catalog is required when more than one catalog is configured");
      }
    }
    if (!catalogs.containsKey(defaultCatalog)) {
      throw new IllegalArgumentException(
          "Unknown routing.default-catalog: " + defaultCatalog);
    }

    if (security.kerberosEnabled()) {
      requireNonBlank(serverPrincipal, "security.server-principal");
      requireNonBlank(keytab, "security.keytab");
      if (!Files.isReadable(Path.of(keytab))) {
        throw new IllegalArgumentException("Keytab file not found or not readable: " + keytab);
      }
      if (clientPrincipal == null) {
        clientPrincipal = serverPrincipal;
        security = new ProxyConfig.SecurityConfig(securityMode, serverPrincipal, clientPrincipal, keytab);
      }
    }

    return new ProxyConfig(server, security, defaultCatalog, catalogs);
  }

  private static String[] splitCsv(String value) {
    return Arrays.stream(value.split(","))
        .map(String::trim)
        .filter(token -> !token.isEmpty())
        .toArray(String[]::new);
  }

  private static String get(Properties properties, String key, String defaultValue) {
    return Objects.requireNonNullElse(trimToNull(properties.getProperty(key)), defaultValue);
  }

  private static String require(Properties properties, String key) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      throw new IllegalArgumentException("Missing required property: " + key);
    }
    return value;
  }

  private static int getInt(Properties properties, String key, int defaultValue) {
    String value = trimToNull(properties.getProperty(key));
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer value for property " + key + ": " + value, e);
    }
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static void requireNonBlank(String value, String name) {
    if (trimToNull(value) == null) {
      throw new IllegalArgumentException("Missing required property: " + name);
    }
  }
}
