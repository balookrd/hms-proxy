package io.github.mmalykhin.hmsproxy.config.listener;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class AdditionalFrontendConfigParserTest {
  private static final String BASE_PROPS = """
      synthetic-read-lock.store.mode=IN_MEMORY
      server.port=9083
      server.bind-host=0.0.0.0
      catalogs=catalog1
      catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083
      """;

  @Test
  public void defaultsToEmptyListWhenUnset() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS);
    Assert.assertTrue(extras.isEmpty());
  }

  @Test
  public void singleApacheListenerReadsBareMinimum() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "additional-frontends=apache2\n"
        + "additional-frontends.apache2.port=9084\n"
        + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
    Assert.assertEquals(1, extras.size());
    AdditionalFrontendConfig extra = extras.get(0);
    Assert.assertEquals("apache2", extra.name());
    Assert.assertEquals(9084, extra.port());
    Assert.assertEquals("0.0.0.0", extra.bindHost());
    Assert.assertEquals(FrontendProfile.APACHE_3_1_3, extra.frontendProfile());
    Assert.assertNull(extra.standaloneMetastoreJar());
  }

  @Test
  public void inheritsBindHostAndThreadsFromPrimary() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "server.bind-host=127.0.0.1\n"
        + "server.min-worker-threads=8\n"
        + "server.max-worker-threads=128\n"
        + "additional-frontends=apache2\n"
        + "additional-frontends.apache2.port=9084\n"
        + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
    AdditionalFrontendConfig extra = extras.get(0);
    Assert.assertEquals("127.0.0.1", extra.bindHost());
    Assert.assertEquals(8, extra.minWorkerThreads());
    Assert.assertEquals(128, extra.maxWorkerThreads());
  }

  @Test
  public void overridesBindHostAndThreadsPerListener() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "additional-frontends=apache2\n"
        + "additional-frontends.apache2.port=9084\n"
        + "additional-frontends.apache2.bind-host=10.0.0.1\n"
        + "additional-frontends.apache2.min-worker-threads=4\n"
        + "additional-frontends.apache2.max-worker-threads=16\n"
        + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
    AdditionalFrontendConfig extra = extras.get(0);
    Assert.assertEquals("10.0.0.1", extra.bindHost());
    Assert.assertEquals(4, extra.minWorkerThreads());
    Assert.assertEquals(16, extra.maxWorkerThreads());
  }

  @Test
  public void parsesMultipleListeners() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "additional-frontends=a,b\n"
        + "additional-frontends.a.port=9084\n"
        + "additional-frontends.a.frontend-profile=APACHE_3_1_3\n"
        + "additional-frontends.b.port=9085\n"
        + "additional-frontends.b.frontend-profile=APACHE_3_1_3\n");
    Assert.assertEquals(2, extras.size());
    Assert.assertEquals("a", extras.get(0).name());
    Assert.assertEquals("b", extras.get(1).name());
  }

  @Test
  public void rejectsListenerOnPrimaryPort() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=apache2\n"
          + "additional-frontends.apache2.port=9083\n"
          + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
      Assert.fail("expected IllegalArgumentException for primary port collision");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("conflicts with the primary listener"));
    }
  }

  @Test
  public void rejectsDuplicateBindingBetweenAdditionalListeners() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=a,b\n"
          + "additional-frontends.a.port=9084\n"
          + "additional-frontends.a.frontend-profile=APACHE_3_1_3\n"
          + "additional-frontends.b.port=9084\n"
          + "additional-frontends.b.frontend-profile=APACHE_3_1_3\n");
      Assert.fail("expected IllegalArgumentException for duplicate port");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("conflicts with additional-frontends.a"));
    }
  }

  @Test
  public void rejectsDuplicateListenerName() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=apache2,apache2\n"
          + "additional-frontends.apache2.port=9084\n"
          + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
      Assert.fail("expected IllegalArgumentException for duplicate name");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("Duplicate"));
    }
  }

  @Test
  public void rejectsMissingPort() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=apache2\n"
          + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
      Assert.fail("expected IllegalArgumentException for missing port");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("apache2.port"));
    }
  }

  @Test
  public void rejectsMaxThreadsLessThanMin() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=apache2\n"
          + "additional-frontends.apache2.port=9084\n"
          + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n"
          + "additional-frontends.apache2.min-worker-threads=16\n"
          + "additional-frontends.apache2.max-worker-threads=4\n");
      Assert.fail("expected IllegalArgumentException for max < min threads");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(), e.getMessage().contains("max-worker-threads"));
    }
  }

  @Test
  public void rejectsUnknownFrontendProfile() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=apache2\n"
          + "additional-frontends.apache2.port=9084\n"
          + "additional-frontends.apache2.frontend-profile=UNKNOWN_PROFILE\n");
      Assert.fail("expected IllegalArgumentException for unknown profile");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("UNKNOWN_PROFILE")
              && e.getMessage().contains("frontend-profile"));
    }
  }

  @Test
  public void rejectsHortonworksProfileWithoutJar() throws Exception {
    try {
      load(BASE_PROPS
          + "additional-frontends=hdp\n"
          + "additional-frontends.hdp.port=9084\n"
          + "additional-frontends.hdp.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78\n");
      Assert.fail("expected IllegalArgumentException for missing jar");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage(),
          e.getMessage().contains("standalone-metastore-jar is required"));
    }
  }

  @Test
  public void acceptsHortonworksProfileWithJar() throws Exception {
    Path jar = Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();
    if (!Files.isReadable(jar)) {
      return; // jar not present in the workspace — skip
    }
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "additional-frontends=hdp\n"
        + "additional-frontends.hdp.port=9084\n"
        + "additional-frontends.hdp.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78\n"
        + "additional-frontends.hdp.standalone-metastore-jar=" + jar.toString() + "\n");
    Assert.assertEquals(1, extras.size());
    Assert.assertEquals(FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, extras.get(0).frontendProfile());
    Assert.assertEquals(jar.toString(), extras.get(0).standaloneMetastoreJar());
  }

  @Test
  public void inheritsFrontDoorSocketSettingsFromPrimary() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "server.client-socket-timeout-ms=120000\n"
        + "server.tcp-keepalive-idle-seconds=45\n"
        + "additional-frontends=apache2\n"
        + "additional-frontends.apache2.port=9084\n"
        + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n");
    ClientSocketConfig clientSocket = extras.get(0).clientSocket();
    Assert.assertEquals(120_000, clientSocket.clientTimeoutMs());
    Assert.assertEquals(45, clientSocket.keepAliveIdleSeconds());
    Assert.assertTrue(clientSocket.tcpKeepAlive());
  }

  @Test
  public void overridesFrontDoorSocketSettingsPerListener() throws Exception {
    List<AdditionalFrontendConfig> extras = load(BASE_PROPS
        + "server.client-socket-timeout-ms=120000\n"
        + "additional-frontends=apache2\n"
        + "additional-frontends.apache2.port=9084\n"
        + "additional-frontends.apache2.frontend-profile=APACHE_3_1_3\n"
        + "additional-frontends.apache2.client-socket-timeout-ms=0\n"
        + "additional-frontends.apache2.tcp-keepalive=false\n"
        + "additional-frontends.apache2.tcp-keepalive-count=7\n");
    ClientSocketConfig clientSocket = extras.get(0).clientSocket();
    Assert.assertEquals(0, clientSocket.clientTimeoutMs());
    Assert.assertFalse(clientSocket.clientTimeoutEnabled());
    Assert.assertFalse(clientSocket.tcpKeepAlive());
    Assert.assertEquals(7, clientSocket.keepAliveCount());
  }

  private static List<AdditionalFrontendConfig> load(String body) throws Exception {
    Path file = Files.createTempFile("hms-proxy-extra-fe", ".properties");
    try {
      Files.writeString(file, body);
      ProxyConfig config = ProxyConfigLoader.load(file);
      return config.additionalFrontends();
    } finally {
      Files.deleteIfExists(file);
    }
  }
}
