package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

public class AdditionalFrontendThriftServersTest {

  @Test
  public void noAdditionalFrontendsReturnsEmptyContainer() throws Exception {
    ProxyConfig config = buildConfig(List.of());
    try (AdditionalFrontendThriftServers extras =
        AdditionalFrontendThriftServers.open(config, noopHandler(), null)) {
      Assert.assertNotNull(extras);
      Assert.assertTrue(extras.running().isEmpty());
    }
  }

  @Test
  public void startsTwoListenersOnDifferentPortsAndStopsThemOnClose() throws Exception {
    int portA = freePort();
    int portB = freePort();
    Assert.assertNotEquals("free ports must differ", portA, portB);
    List<AdditionalFrontendConfig> extras = List.of(
        new AdditionalFrontendConfig("a", "127.0.0.1", portA, 1, 4, FrontendProfile.APACHE_3_1_3, null),
        new AdditionalFrontendConfig("b", "127.0.0.1", portB, 1, 4, FrontendProfile.APACHE_3_1_3, null));
    ProxyConfig config = buildConfig(extras);

    AdditionalFrontendThriftServers servers =
        AdditionalFrontendThriftServers.open(config, noopHandler(), null);
    try {
      Assert.assertEquals(2, servers.running().size());
      Thread.sleep(150);
      Assert.assertTrue("port a must accept connections",
          tcpConnect("127.0.0.1", portA));
      Assert.assertTrue("port b must accept connections",
          tcpConnect("127.0.0.1", portB));
    } finally {
      servers.close();
    }
    // After close, both sockets should refuse connections (within a short retry window).
    Assert.assertTrue("port a must be released",
        awaitClosed("127.0.0.1", portA));
    Assert.assertTrue("port b must be released",
        awaitClosed("127.0.0.1", portB));
  }

  @Test
  public void failureOnSecondListenerCleansUpFirst() throws Exception {
    int sharedPort = freePort();
    // Hog the port so the second listener fails to bind, then verify the first is stopped.
    try (ServerSocket hog = new ServerSocket()) {
      hog.bind(new InetSocketAddress("127.0.0.1", sharedPort), 1);
      int firstPort = freePort();
      List<AdditionalFrontendConfig> extras = List.of(
          new AdditionalFrontendConfig("ok", "127.0.0.1", firstPort, 1, 4, FrontendProfile.APACHE_3_1_3, null),
          new AdditionalFrontendConfig("bad", "127.0.0.1", sharedPort, 1, 4, FrontendProfile.APACHE_3_1_3, null));
      ProxyConfig config = buildConfig(extras);

      try {
        AdditionalFrontendThriftServers.open(config, noopHandler(), null);
        Assert.fail("expected bind failure for occupied port " + sharedPort);
      } catch (Exception expected) {
        // Bound listener for "ok" must have been stopped during cleanup so the port is released.
        Assert.assertTrue("ok listener port must be released after partial-start cleanup",
            awaitClosed("127.0.0.1", firstPort));
      }
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static boolean tcpConnect(String host, int port) {
    for (int attempt = 0; attempt < 20; attempt++) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(host, port), 200);
        return true;
      } catch (Exception e) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return false;
  }

  private static boolean awaitClosed(String host, int port) throws Exception {
    for (int attempt = 0; attempt < 30; attempt++) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(host, port), 100);
        // Still accepting connections — wait and retry.
        Thread.sleep(100);
      } catch (Exception e) {
        return true; // port released
      }
    }
    return false;
  }

  private static ProxyConfig buildConfig(List<AdditionalFrontendConfig> extras) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .catalogDbSeparator(".")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1", null, null, false, CatalogAccessMode.READ_WRITE, List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://hms-test:9083"))))
        .backend(new BackendConfig(Map.of()))
        .additionalFrontends(extras)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static ThriftHiveMetastore.Iface noopHandler() {
    return (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[]{ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if (method.getDeclaringClass() == Object.class) {
            return method.invoke(proxy, args);
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

  // Reference to silence unused-import warnings for the Method type used in the noop proxy.
  @SuppressWarnings("unused")
  private static final Class<Method> METHOD_REF = Method.class;
}
