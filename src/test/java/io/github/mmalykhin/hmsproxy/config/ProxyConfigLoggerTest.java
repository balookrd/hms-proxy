package io.github.mmalykhin.hmsproxy.config;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

public class ProxyConfigLoggerTest {

  @Test
  public void formatsAllKeySectionsInConfigurationOutput() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test", ".properties");
    try {
      Files.writeString(file, """
          synthetic-read-lock.store.mode=IN_MEMORY
          server.name=custom-proxy
          server.port=9083
          catalogs=main,shadow
          routing.default-catalog=main
          catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.shadow.conf.hive.metastore.uris=thrift://hms2:9083
          catalog.shadow.access-mode=READ_ONLY
          catalog.shadow.startup-mode=LENIENT
          catalog.shadow.required-for-readiness=false
          management.enabled=true
          management.port=19083
          rest-catalog.enabled=true
          rest-catalog.port=9183
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);
      String output = ProxyConfigLogger.formatConfiguration(config, file);

      Assert.assertTrue(output.contains("HMS Proxy Configuration Loaded"));
      Assert.assertTrue(output.contains(file.toAbsolutePath().toString()));
      Assert.assertTrue(output.contains("[Server]"));
      Assert.assertTrue(output.contains("Name: custom-proxy"));
      Assert.assertTrue(output.contains("Listen: 0.0.0.0:9083"));
      Assert.assertTrue(output.contains("[Security]"));
      Assert.assertTrue(output.contains("[Routing & Federation]"));
      Assert.assertTrue(output.contains("Default Catalog: main"));
      Assert.assertTrue(output.contains("[Catalogs] (total: 2)"));
      Assert.assertTrue(output.contains("Catalog 'main'"));
      Assert.assertTrue(output.contains("Catalog 'shadow'"));
      Assert.assertTrue(output.contains("Access Mode: READ_ONLY"));
      Assert.assertTrue(output.contains("Startup Mode: LENIENT"));
      Assert.assertTrue(output.contains("[REST Catalog]"));
      Assert.assertTrue(output.contains("Listen: 0.0.0.0:9183"));
      Assert.assertTrue(output.contains("[Management HTTP]"));
      Assert.assertTrue(output.contains("Listen: 0.0.0.0:19083"));
      Assert.assertTrue(output.contains("[Resilience & Latency Routing]"));
      Assert.assertTrue(output.contains("[Iceberg Pointer Guard]"));
      Assert.assertTrue(output.contains("[Synthetic Read Lock Store]"));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void masksSensitiveParametersInConfigurationOutput() throws Exception {
    Path file = Files.createTempFile("hms-proxy-test-secret", ".properties");
    try {
      Files.writeString(file, """
          synthetic-read-lock.store.mode=IN_MEMORY
          catalogs=main
          routing.default-catalog=main
          catalog.main.conf.hive.metastore.uris=thrift://hms1:9083
          catalog.main.conf.javax.jdo.option.ConnectionPassword=SuperSecretPassword123
          catalog.main.conf.fs.s3a.secret.key=TopSecretS3KeyABC
          catalog.main.conf.custom.api.token=TokenXYZ
          security.front-door-conf.secret.token=FrontDoorSecretToken
          """);

      ProxyConfig config = ProxyConfigLoader.load(file);
      String output = ProxyConfigLogger.formatConfiguration(config, file);

      Assert.assertFalse("Password must be masked", output.contains("SuperSecretPassword123"));
      Assert.assertFalse("S3 secret must be masked", output.contains("TopSecretS3KeyABC"));
      Assert.assertFalse("Token must be masked", output.contains("TokenXYZ"));
      Assert.assertFalse("Front-door secret must be masked", output.contains("FrontDoorSecretToken"));

      Assert.assertTrue(output.contains("ConnectionPassword = ******"));
      Assert.assertTrue(output.contains("secret.key = ******"));
      Assert.assertTrue(output.contains("custom.api.token = ******"));
      Assert.assertTrue(output.contains("secret.token = ******"));
    } finally {
      Files.deleteIfExists(file);
    }
  }

  @Test
  public void maskSensitiveUtilityMethod() {
    Assert.assertEquals("******", ProxyConfigLogger.maskSensitive("db.password", "my_pass"));
    Assert.assertEquals("******", ProxyConfigLogger.maskSensitive("aws.secret.key", "my_secret"));
    Assert.assertEquals("******", ProxyConfigLogger.maskSensitive("auth.token", "token123"));
    Assert.assertEquals("******", ProxyConfigLogger.maskSensitive("private.key", "pem_data"));
    Assert.assertEquals("******", ProxyConfigLogger.maskSensitive("user.credentials", "creds"));
    Assert.assertEquals("thrift://localhost:9083", ProxyConfigLogger.maskSensitive("hive.metastore.uris", "thrift://localhost:9083"));
    Assert.assertNull(ProxyConfigLogger.maskSensitive("db.password", null));
  }
}
