package io.github.mmalykhin.hmsproxy.config;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ConfigReloadScopeTest {
  private static final Path STAND_CONFIGS = Path.of("smoke-stand", "proxy");

  /**
   * The comparison must not report a change when nothing changed, or every reload would warn: two
   * loads of the same file have to compare equal component by component.
   */
  @Test
  public void reloadingAnUnchangedStandConfigReportsNothing() throws Exception {
    Path hostDir = Files.createTempDirectory("hms-proxy-stand-configs");
    Path keytab = Files.createFile(hostDir.resolve("proxy.keytab"));
    int compared = 0;
    try (DirectoryStream<Path> files = Files.newDirectoryStream(STAND_CONFIGS, "*.properties")) {
      for (Path file : files) {
        // The stand configs name container paths; the loader insists they exist, so point them at
        // their host counterparts.
        Path hostCopy = hostDir.resolve(file.getFileName());
        Files.writeString(hostCopy, Files.readString(file)
            .replace("/opt/hms-proxy/hive-metastore/", Path.of("hive-metastore").toAbsolutePath() + "/")
            .replace("/opt/hms-proxy/ranger-cache", hostDir.toString())
            .replace("/keytabs/proxy.keytab", keytab.toString()));
        Assert.assertEquals(file.toString(), List.of(),
            ConfigReloadScope.changesRequiringRestart(ProxyConfigLoader.load(hostCopy), ProxyConfigLoader.load(hostCopy)));
        compared++;
      }
    }
    Assert.assertTrue("expected every stand config to be compared, got " + compared, compared >= 10);
  }

  @Test
  public void namesAddedCatalogsAndBackendSettingsButNotRoutingOnes() throws Exception {
    ProxyConfig running = load("""
        catalogs=c1,c2
        routing.default-catalog=c1
        catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
        catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
        synthetic-read-lock.store.mode=IN_MEMORY
        """);
    ProxyConfig next = load("""
        catalogs=c1,c2,c3
        routing.default-catalog=c1
        catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
        catalog.c1.impersonation-pool-max-size=8
        catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
        catalog.c2.access-mode=READ_ONLY
        catalog.c2.merge-with-default=true
        catalog.c3.conf.hive.metastore.uris=thrift://hms3:9083
        federation.external-table-drop-purge.mode=BEST_EFFORT
        synthetic-read-lock.store.mode=IN_MEMORY
        """);

    List<String> changes = ConfigReloadScope.changesRequiringRestart(running, next);

    Assert.assertTrue(changes.toString(), changes.contains("catalogs (catalog 'c3' added)"));
    Assert.assertTrue(changes.toString(), changes.contains("catalog.c1.impersonationPoolMaxSize"));
    Assert.assertTrue(changes.toString(), changes.contains("federation.externalTableDropPurgeMode"));
    Assert.assertFalse(changes.toString(), changes.stream().anyMatch(c -> c.startsWith("catalog.c2.")));
  }

  private static ProxyConfig load(String properties) throws Exception {
    Path file = Files.createTempFile("hms-proxy-scope", ".properties");
    try {
      Files.writeString(file, properties);
      return ProxyConfigLoader.load(file);
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
