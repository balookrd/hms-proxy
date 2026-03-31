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
    String catalogDbSeparator = trimToNull(properties.getProperty("routing.catalog-db-separator"));
    if (properties.containsKey("routing.catalog-db-separator") && catalogDbSeparator == null) {
      throw new IllegalArgumentException("routing.catalog-db-separator must not be blank");
    }
    if (catalogDbSeparator == null) {
      catalogDbSeparator = ".";
    }
    ProxyConfig.FrontendProfile frontendProfile = ProxyConfig.FrontendProfile.valueOf(
        get(properties, "compatibility.frontend-profile", "APACHE_3_1_3").trim().toUpperCase());
    String frontendStandaloneMetastoreJar =
        trimToNull(properties.getProperty("compatibility.frontend-standalone-metastore-jar"));
    if (frontendStandaloneMetastoreJar == null) {
      frontendStandaloneMetastoreJar =
          trimToNull(properties.getProperty("compatibility.hortonworks-standalone-metastore-jar"));
    }
    String backendStandaloneMetastoreJar =
        trimToNull(properties.getProperty("compatibility.backend-standalone-metastore-jar"));
    boolean preserveBackendCatalogName =
        Boolean.parseBoolean(get(properties, "compatibility.preserve-backend-catalog-name", "false"));

    ProxyConfig.SecurityMode securityMode = ProxyConfig.SecurityMode.valueOf(
        get(properties, "security.mode", "NONE").trim().toUpperCase());
    String serverPrincipal = trimToNull(properties.getProperty("security.server-principal"));
    String clientPrincipal = trimToNull(properties.getProperty("security.client-principal"));
    String keytab = trimToNull(properties.getProperty("security.keytab"));
    String clientKeytab = trimToNull(properties.getProperty("security.client-keytab"));
    boolean impersonationEnabled =
        Boolean.parseBoolean(get(properties, "security.impersonation-enabled", "false"));
    Map<String, String> frontDoorConf = properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith("security.front-door-conf."))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring("security.front-door-conf.".length()),
            properties::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));
    Map<String, String> backendConf = properties.stringPropertyNames().stream()
        .filter(name -> name.startsWith("backend.conf."))
        .sorted()
        .collect(Collectors.toMap(
            name -> name.substring("backend.conf.".length()),
            properties::getProperty,
            (left, right) -> right,
            LinkedHashMap::new));

    String catalogsValue = require(properties, "catalogs");
    Map<String, ProxyConfig.CatalogConfig> catalogs = new LinkedHashMap<>();
    for (String catalogName : splitCsv(catalogsValue)) {
      String prefix = "catalog." + catalogName + ".";
      boolean catalogImpersonationEnabled =
          Boolean.parseBoolean(get(properties, prefix + "impersonation-enabled", Boolean.toString(impersonationEnabled)));
      MetastoreRuntimeProfile catalogRuntimeProfile = parseRuntimeProfile(
          trimToNull(properties.getProperty(prefix + "runtime-profile")));
      String catalogBackendStandaloneMetastoreJar =
          trimToNull(properties.getProperty(prefix + "backend-standalone-metastore-jar"));
      Map<String, String> hiveConfOverrides = properties.stringPropertyNames().stream()
          .filter(name -> name.startsWith(prefix + "conf."))
          .sorted()
          .collect(Collectors.toMap(
              name -> name.substring((prefix + "conf.").length()),
              properties::getProperty,
              (left, right) -> right,
              LinkedHashMap::new));
      Map<String, String> hiveConf = new LinkedHashMap<>(backendConf);
      hiveConf.putAll(hiveConfOverrides);
      if (!hiveConf.containsKey("hive.metastore.uris")) {
        throw new IllegalArgumentException(
            "Missing backend.conf.hive.metastore.uris or " + prefix + "conf.hive.metastore.uris for catalog "
                + catalogName);
      }
      catalogs.put(catalogName, new ProxyConfig.CatalogConfig(
          catalogName,
          get(properties, prefix + "description", catalogName),
          get(properties, prefix + "location-uri", "file:///warehouse/" + catalogName),
          catalogImpersonationEnabled,
          catalogRuntimeProfile,
          catalogBackendStandaloneMetastoreJar,
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

    if (clientPrincipal == null && serverPrincipal != null) {
      clientPrincipal = serverPrincipal;
    }
    if (clientKeytab == null && keytab != null) {
      clientKeytab = keytab;
    }

    if (securityMode == ProxyConfig.SecurityMode.KERBEROS) {
      requireNonBlank(serverPrincipal, "security.server-principal");
      requireNonBlank(keytab, "security.keytab");
      requireReadableFile(keytab, "security.keytab");
    }

    if (catalogs.values().stream().anyMatch(ProxyConfig.CatalogConfig::impersonationEnabled)
        && securityMode != ProxyConfig.SecurityMode.KERBEROS) {
      throw new IllegalArgumentException(
          "security.impersonation-enabled and catalog.<name>.impersonation-enabled "
              + "require security.mode=KERBEROS so the proxy can derive the caller identity from SASL");
    }

    if (catalogs.values().stream().anyMatch(catalog -> backendKerberosEnabled(catalog.hiveConf()))) {
      requireNonBlank(clientPrincipal, "security.client-principal");
      requireNonBlank(clientKeytab, "security.client-keytab");
      requireReadableFile(clientKeytab, "security.client-keytab");
    }

    ProxyConfig.SecurityConfig security = new ProxyConfig.SecurityConfig(
        securityMode,
        serverPrincipal,
        clientPrincipal,
        keytab,
        clientKeytab,
        impersonationEnabled,
        frontDoorConf);
    ProxyConfig.BackendConfig backend = new ProxyConfig.BackendConfig(backendConf);
    ProxyConfig.CompatibilityConfig compatibility =
        new ProxyConfig.CompatibilityConfig(
            frontendProfile,
            frontendStandaloneMetastoreJar,
            backendStandaloneMetastoreJar,
            preserveBackendCatalogName);
    return new ProxyConfig(server, security, catalogDbSeparator, defaultCatalog, catalogs, backend, compatibility);
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

  private static void requireReadableFile(String path, String propertyName) {
    if (!Files.isReadable(Path.of(path))) {
      throw new IllegalArgumentException(
          "File not found or not readable for " + propertyName + ": " + path);
    }
  }

  private static boolean backendKerberosEnabled(Map<String, String> hiveConf) {
    return Boolean.parseBoolean(trimToNull(hiveConf.get("hive.metastore.sasl.enabled")));
  }

  private static MetastoreRuntimeProfile parseRuntimeProfile(String value) {
    if (value == null) {
      return null;
    }
    return MetastoreRuntimeProfile.valueOf(value.trim().toUpperCase());
  }
}
