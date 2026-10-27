package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class ConfigReloadManagerTest {

  @Test
  public void reloadManuallyAndViaListener() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test", ".properties");
    try {
      Files.writeString(file, """
          server.port=9083
          catalogs=c1,c2
          routing.default-catalog=c1
          catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
          synthetic-read-lock.store.mode=IN_MEMORY
          config.reload.poll-interval-seconds=0
          """);

      ProxyConfig initialConfig = ProxyConfigLoader.load(file);
      try (ConfigReloadManager reloadManager = new ConfigReloadManager(file, initialConfig)) {
        AtomicReference<ProxyConfig> updated = new AtomicReference<>();
        reloadManager.addListener(updated::set);

        Assert.assertEquals(0, reloadManager.currentConfig().catalogs().get("c2").unprefixedDatabases().size());

        // Update file with unprefixed-databases
        Files.writeString(file, """
            server.port=9083
            catalogs=c1,c2
            routing.default-catalog=c1
            catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
            catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
            catalog.c2.unprefixed-databases=beemetrics
            synthetic-read-lock.store.mode=IN_MEMORY
            config.reload.poll-interval-seconds=0
            """);

        ConfigReloadManager.ReloadResult result = reloadManager.reload();
        Assert.assertTrue(result.success());
        Assert.assertNotNull(updated.get());
        Assert.assertEquals(List.of("beemetrics"), updated.get().catalogs().get("c2").unprefixedDatabases());
        Assert.assertEquals(List.of("beemetrics"), reloadManager.currentConfig().catalogs().get("c2").unprefixedDatabases());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void safeReloadKeepsPreviousConfigOnSyntaxOrValidationError() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test", ".properties");
    try {
      Files.writeString(file, """
          server.port=9083
          catalogs=c1,c2
          routing.default-catalog=c1
          catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
          synthetic-read-lock.store.mode=IN_MEMORY
          """);

      ProxyConfig initialConfig = ProxyConfigLoader.load(file);
      try (ConfigReloadManager reloadManager = new ConfigReloadManager(file, initialConfig)) {
        // Write invalid configuration (unprefixed-databases on default catalog is forbidden)
        Files.writeString(file, """
            server.port=9083
            catalogs=c1,c2
            routing.default-catalog=c1
            catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
            catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
            catalog.c1.unprefixed-databases=invalid_on_default
            synthetic-read-lock.store.mode=IN_MEMORY
            """);

        ConfigReloadManager.ReloadResult result = reloadManager.reload();
        Assert.assertFalse("Safe reload must fail on invalid configuration", result.success());
        Assert.assertTrue("Error message must contain explanation",
            result.message().contains("default-catalog"));
        // Current config must remain unchanged
        Assert.assertSame(initialConfig, reloadManager.currentConfig());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void reloadsOnFileChangeViaPoller() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test", ".properties");
    try {
      Files.writeString(file, """
          server.port=9083
          catalogs=c1,c2
          routing.default-catalog=c1
          catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
          synthetic-read-lock.store.mode=IN_MEMORY
          config.reload.poll-interval-seconds=0
          """);

      ProxyConfig initialConfig = ProxyConfigLoader.load(file);
      try (ConfigReloadManager reloadManager = new ConfigReloadManager(file, initialConfig)) {
        AtomicReference<ProxyConfig> updated = new AtomicReference<>();
        reloadManager.addListener(updated::set);

        // Update file and advance its lastModified time
        Files.writeString(file, """
            server.port=9083
            catalogs=c1,c2
            routing.default-catalog=c1
            catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
            catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
            catalog.c2.unprefixed-databases=polled_db
            synthetic-read-lock.store.mode=IN_MEMORY
            config.reload.poll-interval-seconds=0
            """);
        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 5000L));

        reloadManager.checkFileModified();

        Assert.assertNotNull(updated.get());
        Assert.assertEquals(List.of("polled_db"), updated.get().catalogs().get("c2").unprefixedDatabases());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  private static final String BASE_CONFIG = """
      server.port=9083
      catalogs=c1,c2
      routing.default-catalog=c1
      catalog.c1.conf.hive.metastore.uris=thrift://hms1:9083
      catalog.c2.conf.hive.metastore.uris=thrift://hms2:9083
      synthetic-read-lock.store.mode=IN_MEMORY
      config.reload.poll-interval-seconds=0
      """;

  @Test
  public void aReloadNamesTheSettingsItCannotApply() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test", ".properties");
    try {
      Files.writeString(file, BASE_CONFIG);
      try (ConfigReloadManager reloadManager = new ConfigReloadManager(file, ProxyConfigLoader.load(file))) {
        Files.writeString(file, BASE_CONFIG
            .replace("thrift://hms1:9083", "thrift://hms1-new:9083")
            + "catalog.c2.unprefixed-databases=beemetrics\n"
            + "routing.circuit-breaker.enabled=true\n");

        ConfigReloadManager.ReloadResult result = reloadManager.reload();

        Assert.assertTrue("the reloadable part is still applied", result.success());
        Assert.assertEquals(List.of("beemetrics"),
            reloadManager.currentConfig().catalogs().get("c2").unprefixedDatabases());
        Assert.assertTrue(result.message(), result.message().contains("catalog.c1.hiveConf"));
        Assert.assertTrue(result.message(), result.message().contains("latencyRouting"));
        Assert.assertFalse("unprefixed-databases is applied by a reload",
            result.message().contains("unprefixedDatabases"));

        // Reverting the restart-only settings clears the warning: the comparison is against what runs.
        Files.writeString(file, BASE_CONFIG + "catalog.c2.unprefixed-databases=beemetrics\n");
        Assert.assertEquals("Configuration reloaded successfully", reloadManager.reload().message());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void aBrokenFileIsNotReparsedOnEveryPollUntilItChangesAgain() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test", ".properties");
    try {
      Files.writeString(file, BASE_CONFIG);
      try (ConfigReloadManager reloadManager = new ConfigReloadManager(file, ProxyConfigLoader.load(file))) {
        Files.writeString(file, BASE_CONFIG + "catalog.c1.unprefixed-databases=forbidden_on_default\n");
        FileTime broken = FileTime.fromMillis(System.currentTimeMillis() + 60_000L);
        Files.setLastModifiedTime(file, broken);

        reloadManager.checkFileModified();

        Assert.assertEquals("the failed file's mtime is remembered, so the next poll does not retry it",
            broken, reloadManager.lastModifiedTime());
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
