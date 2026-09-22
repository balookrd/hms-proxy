package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogStartupMode;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreMode;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Assert;
import org.junit.Test;

public class ManagementHttpServerReadinessTest {

  @Test
  public void secondaryNotRequiredForReadinessDoesNotDegradeProxyWhenDown() throws Exception {
    int port = freePort();
    ProxyConfig config = buildConfig(port, true, false, true);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = CatalogRouter.createForTest(config, Collections.emptyMap());

    try (ManagementHttpServer server = ManagementHttpServer.open(config, router, observability)) {
      Assert.assertNotNull(server);

      // Primary is healthy, Secondary is down
      observability.runtimeState().recordBackendSuccess("primary", 10L, config.latencyRouting());
      observability.runtimeState().recordBackendFailure(
          "secondary", new org.apache.thrift.transport.TTransportException("Connection refused"), 500L, config.latencyRouting());

      int statusCode = statusOf(port, "/readyz");
      String body = get(port, "/readyz");

      Assert.assertEquals(200, statusCode);
      Assert.assertTrue(body, body.contains("\"status\":\"ready\""));
      Assert.assertTrue(body, body.contains("\"backendConnectivity\":true"));
      Assert.assertTrue(body, body.contains("\"backend\":\"primary\",\"requiredForReadiness\":true,\"connected\":true"));
      Assert.assertTrue(body, body.contains("\"backend\":\"secondary\",\"requiredForReadiness\":false,\"connected\":false"));
    }
  }

  @Test
  public void secondaryRequiredForReadinessDegradesProxyWhenDown() throws Exception {
    int port = freePort();
    ProxyConfig config = buildConfig(port, true, true, true);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = CatalogRouter.createForTest(config, Collections.emptyMap());

    try (ManagementHttpServer server = ManagementHttpServer.open(config, router, observability)) {
      Assert.assertNotNull(server);

      observability.runtimeState().recordBackendSuccess("primary", 10L, config.latencyRouting());
      observability.runtimeState().recordBackendFailure(
          "secondary", new org.apache.thrift.transport.TTransportException("Connection refused"), 500L, config.latencyRouting());

      int statusCode = statusOf(port, "/readyz");
      String body = get(port, "/readyz");

      Assert.assertEquals(503, statusCode);
      Assert.assertTrue(body, body.contains("\"status\":\"degraded\""));
      Assert.assertTrue(body, body.contains("\"backendConnectivity\":false"));
      Assert.assertTrue(body, body.contains("\"backend\":\"secondary\",\"requiredForReadiness\":true,\"connected\":false"));
    }
  }

  @Test
  public void requireAllCatalogsFalseAllowsReadinessEvenWhenRequiredSecondaryIsDown() throws Exception {
    int port = freePort();
    // requireAllCatalogs = false, secondary has requiredForReadiness = true
    ProxyConfig config = buildConfig(port, true, true, false);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = CatalogRouter.createForTest(config, Collections.emptyMap());

    try (ManagementHttpServer server = ManagementHttpServer.open(config, router, observability)) {
      Assert.assertNotNull(server);

      observability.runtimeState().recordBackendSuccess("primary", 10L, config.latencyRouting());
      observability.runtimeState().recordBackendFailure(
          "secondary", new org.apache.thrift.transport.TTransportException("Connection refused"), 500L, config.latencyRouting());

      int statusCode = statusOf(port, "/readyz");
      String body = get(port, "/readyz");

      Assert.assertEquals(200, statusCode);
      Assert.assertTrue(body, body.contains("\"status\":\"ready\""));
      Assert.assertTrue(body, body.contains("\"backendConnectivity\":true"));
      Assert.assertTrue(body, body.contains("\"backend\":\"secondary\",\"requiredForReadiness\":false,\"connected\":false"));
    }
  }

  @Test
  public void primaryCatalogDownAlwaysDegradesProxyEvenWithRequireAllFalse() throws Exception {
    int port = freePort();
    ProxyConfig config = buildConfig(port, true, false, false);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = CatalogRouter.createForTest(config, Collections.emptyMap());

    try (ManagementHttpServer server = ManagementHttpServer.open(config, router, observability)) {
      Assert.assertNotNull(server);

      // Primary is down, secondary is healthy
      observability.runtimeState().recordBackendFailure(
          "primary", new org.apache.thrift.transport.TTransportException("Primary DB failure"), 500L, config.latencyRouting());
      observability.runtimeState().recordBackendSuccess("secondary", 10L, config.latencyRouting());

      int statusCode = statusOf(port, "/readyz");
      String body = get(port, "/readyz");

      Assert.assertEquals(503, statusCode);
      Assert.assertTrue(body, body.contains("\"status\":\"degraded\""));
      Assert.assertTrue(body, body.contains("\"backendConnectivity\":false"));
      Assert.assertTrue(body, body.contains("\"backend\":\"primary\",\"requiredForReadiness\":true,\"connected\":false"));
    }
  }

  private static ProxyConfig buildConfig(
      int managementPort,
      boolean primaryRequired,
      boolean secondaryRequired,
      boolean requireAllCatalogs
  ) {
    CatalogConfig primaryConfig = new CatalogConfig(
        "primary",
        "primary catalog",
        "thrift://primary:9083",
        false,
        CatalogAccessMode.READ_WRITE,
        Collections.emptyList(),
        CatalogExposureMode.ALLOW_ALL,
        Collections.emptyList(),
        Collections.emptyMap(),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        null,
        Collections.emptyMap(),
        0L,
        128,
        0L,
        1,
        4,
        0L,
        null,
        CatalogStartupMode.STRICT,
        primaryRequired,
        0,
        10000L,
        null,
        false
    );

    CatalogConfig secondaryConfig = new CatalogConfig(
        "secondary",
        "secondary remote catalog",
        "thrift://secondary:9083",
        false,
        CatalogAccessMode.READ_WRITE,
        Collections.emptyList(),
        CatalogExposureMode.ALLOW_ALL,
        Collections.emptyList(),
        Collections.emptyMap(),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        null,
        Collections.emptyMap(),
        0L,
        128,
        0L,
        1,
        4,
        0L,
        null,
        CatalogStartupMode.LENIENT,
        secondaryRequired,
        0,
        10000L,
        null,
        false
    );

    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put("primary", primaryConfig);
    catalogs.put("secondary", secondaryConfig);

    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("primary")
        .catalogs(catalogs)
        .management(new ManagementConfig(
            true, "127.0.0.1", managementPort, ManagementConfig.DEFAULT_THREADS, 0L, requireAllCatalogs))
        .syntheticReadLockStore(
            new SyntheticReadLockStoreConfig(SyntheticReadLockStoreMode.IN_MEMORY, null))
        .build();
  }

  private static int statusOf(int port, String path) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path).toURL().openConnection();
    connection.setRequestMethod("GET");
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(10_000);
    try {
      return connection.getResponseCode();
    } finally {
      connection.disconnect();
    }
  }

  private static String get(int port, String path) throws Exception {
    HttpURLConnection connection =
        (HttpURLConnection) URI.create("http://127.0.0.1:" + port + path).toURL().openConnection();
    connection.setConnectTimeout(5_000);
    connection.setReadTimeout(10_000);
    try (InputStream input = connection.getInputStream()) {
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (Exception e) {
      if (connection.getErrorStream() != null) {
        return new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
      }
      throw e;
    } finally {
      connection.disconnect();
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
