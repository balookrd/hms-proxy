package io.github.mmalykhin.hmsproxy.config;

import io.github.mmalykhin.hmsproxy.security.MetastoreThriftServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
public class MetastoreThriftServerTest {
  @Test
  public void frontDoorAlwaysUsesServerPrincipalForInboundKerberos() {
    SecurityConfig security = new SecurityConfig(
        SecurityMode.KERBEROS,
        "hive/proxy-host.example.com@EXAMPLE.COM",
        "hive/backend-host.example.com@EXAMPLE.COM",
        "/tmp/proxy.keytab",
        "/tmp/backend.keytab",
        false,
        java.util.Map.of());

    Assert.assertEquals(
        "hive/proxy-host.example.com@EXAMPLE.COM",
        MetastoreThriftServer.frontDoorClientPrincipal(security));
  }

  @Test
  public void frontDoorDoesNotExposeBackendClientPrincipal() {
    SecurityConfig security = new SecurityConfig(
        SecurityMode.KERBEROS,
        "hive/proxy-host.example.com@EXAMPLE.COM",
        "hive/backend-host.example.com@EXAMPLE.COM",
        "/tmp/proxy.keytab",
        "/tmp/backend.keytab",
        false,
        java.util.Map.of());

    Assert.assertNotEquals(
        security.clientPrincipal(),
        MetastoreThriftServer.frontDoorClientPrincipal(security));
  }

  @Test
  public void stopBeforeServeReleasesThePortAndKeepsTheListenerFromStarting() throws Exception {
    int port = freePort();
    MetastoreThriftServer server = newServer(port);

    server.stop();
    Assert.assertTrue("stop() before serve() must release the port", awaitClosed(port));

    Thread listener = startServing(server, "stop-before-serve");
    listener.join(5_000L);
    Assert.assertFalse("serve() must not enter the accept loop after stop()", listener.isAlive());
    Assert.assertTrue("port must stay released", awaitClosed(port));
  }

  @Test
  public void stopWhileServingIsIdempotentAndReleasesThePort() throws Exception {
    int port = freePort();
    MetastoreThriftServer server = newServer(port);
    Thread listener = startServing(server, "stop-while-serving");
    Assert.assertTrue("listener must accept connections before it is stopped", awaitAccepting(port));

    server.stop();
    server.stop();

    listener.join(10_000L);
    Assert.assertFalse("listener thread must terminate", listener.isAlive());
    Assert.assertTrue("port must be released", awaitClosed(port));
  }

  /**
   * libthrift 0.9.3 clears its own stopped_ flag inside serve(), so a stop() that lands in that
   * window used to be erased and leave the accept loop spinning on a closed socket forever.
   */
  @Test
  public void stopRacingServeStartAlwaysTerminatesTheListener() throws Exception {
    for (int attempt = 0; attempt < 10; attempt++) {
      int port = freePort();
      MetastoreThriftServer server = newServer(port);
      Thread listener = startServing(server, "stop-race-" + attempt);

      server.stop();

      listener.join(15_000L);
      Assert.assertFalse("listener thread must terminate on attempt " + attempt, listener.isAlive());
      Assert.assertTrue("port must be released on attempt " + attempt, awaitClosed(port));
    }
  }

  private static MetastoreThriftServer newServer(int port) throws Exception {
    return new MetastoreThriftServer(configForPort(port), noopHandler(), null);
  }

  private static Thread startServing(MetastoreThriftServer server, String name) {
    Thread thread = new Thread(server::serve, "metastore-thrift-server-test-" + name);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  private static ProxyConfig configForPort(int port) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", port, 1, 4))
        .catalogDbSeparator(".")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", new CatalogConfig(
            "catalog1", null, null, false, CatalogAccessMode.READ_WRITE, List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://hms-test:9083"))))
        .backend(new BackendConfig(Map.of()))
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

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static boolean awaitAccepting(int port) throws Exception {
    for (int attempt = 0; attempt < 40; attempt++) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 200);
        return true;
      } catch (Exception e) {
        Thread.sleep(50L);
      }
    }
    return false;
  }

  private static boolean awaitClosed(int port) throws Exception {
    for (int attempt = 0; attempt < 40; attempt++) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 100);
        Thread.sleep(100L);
      } catch (Exception e) {
        return true;
      }
    }
    return false;
  }
}
