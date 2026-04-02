package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MetastoreRuntimeJarResolverTest {
  @Test
  public void backendJarResolverPrefersConfiguredOverride() throws Exception {
    Path jar = Files.createTempFile("hms-proxy-backend-runtime", ".jar");
    try {
      ProxyConfig config = new ProxyConfig(
          new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
          new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
          "__",
          "catalog1",
          Map.of("catalog1", new ProxyConfig.CatalogConfig(
              "catalog1", "c1", "file:///c1", false, ProxyConfig.CatalogAccessMode.READ_WRITE, java.util.List.of(),
              null, jar.toString(), Map.of("hive.metastore.uris", "thrift://one"))),
          new ProxyConfig.CompatibilityConfig(
              ProxyConfig.FrontendProfile.APACHE_3_1_3,
              null,
              jar.toString(),
              false));

      Assert.assertEquals(
          jar.toAbsolutePath().normalize(),
          MetastoreRuntimeJarResolver.resolveBackendJar(
              config,
              config.catalogs().get("catalog1"),
              MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78));
    } finally {
      Files.deleteIfExists(jar);
    }
  }

  @Test
  public void backendJarResolverUsesNewerHortonworksDefaultJar() throws Exception {
    Path jar = Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar").toAbsolutePath();
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", new ProxyConfig.CatalogConfig(
            "catalog1", "c1", "file:///c1", false, ProxyConfig.CatalogAccessMode.READ_WRITE, java.util.List.of(),
            MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.APACHE_3_1_3,
            null,
            jar.toString(),
            false));

    Assert.assertEquals(
        jar.normalize(),
        MetastoreRuntimeJarResolver.resolveBackendJar(
            config,
            config.catalogs().get("catalog1"),
            MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1));
  }
}
