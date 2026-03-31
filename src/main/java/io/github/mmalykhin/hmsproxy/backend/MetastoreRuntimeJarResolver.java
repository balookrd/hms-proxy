package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfileResolver;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.nio.file.Files;
import java.nio.file.Path;

public final class MetastoreRuntimeJarResolver {
  private MetastoreRuntimeJarResolver() {
  }

  public static Path resolveFrontendJar(ProxyConfig config) {
    MetastoreRuntimeProfile runtimeProfile =
        MetastoreRuntimeProfileResolver.forFrontendProfile(config.compatibility().frontendProfile());
    String configuredJar = config.compatibility().frontendStandaloneMetastoreJar();
    Path jarPath = configuredJar == null
        ? Path.of(runtimeProfile.defaultStandaloneMetastoreJar())
        : Path.of(configuredJar);
    if (!Files.isReadable(jarPath)) {
      throw new IllegalArgumentException(
          "Frontend runtime profile " + runtimeProfile
              + " requires a readable standalone-metastore jar. Checked: "
              + jarPath.toAbsolutePath());
    }
    return jarPath.toAbsolutePath().normalize();
  }

  public static Path resolveBackendJar(
      ProxyConfig config,
      ProxyConfig.CatalogConfig catalogConfig,
      MetastoreRuntimeProfile runtimeProfile
  ) {
    String configuredJar = catalogConfig.backendStandaloneMetastoreJar() != null
        ? catalogConfig.backendStandaloneMetastoreJar()
        : config.compatibility().backendStandaloneMetastoreJar();
    Path jarPath = configuredJar == null
        ? Path.of(runtimeProfile.defaultStandaloneMetastoreJar())
        : Path.of(configuredJar);
    if (!Files.isReadable(jarPath)) {
      throw new IllegalArgumentException(
          "Backend runtime profile " + runtimeProfile
              + " requires a readable standalone-metastore jar. Checked: "
              + jarPath.toAbsolutePath());
    }
    return jarPath.toAbsolutePath().normalize();
  }
}
