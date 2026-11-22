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

  @Test
  public void listenerThreadsAreDaemonThreads() throws Exception {
    List<AdditionalFrontendConfig> extras = List.of(
        new AdditionalFrontendConfig("daemoncheck", "127.0.0.1", freePort(), 1, 4,
            FrontendProfile.APACHE_3_1_3, null));

    try (AdditionalFrontendThriftServers servers =
        AdditionalFrontendThriftServers.open(buildConfig(extras), noopHandler(), null)) {
      Assert.assertEquals(1, servers.running().size());
      List<Thread> threads = listenerThreads("hms-proxy-fe-daemoncheck");
      Assert.assertEquals("listener thread must exist while serving", 1, threads.size());
      Assert.assertTrue("a listener thread must never keep a failed JVM alive",
          threads.get(0).isDaemon());
    }
  }

  @Test
  public void failureOnSecondListenerLeavesNoLiveListenerThreads() throws Exception {
    int sharedPort = freePort();
    try (ServerSocket hog = new ServerSocket()) {
      hog.bind(new InetSocketAddress("127.0.0.1", sharedPort), 1);
      List<AdditionalFrontendConfig> extras = List.of(
          new AdditionalFrontendConfig("leakcheck", "127.0.0.1", freePort(), 1, 4,
              FrontendProfile.APACHE_3_1_3, null),
          new AdditionalFrontendConfig("leakcheckbad", "127.0.0.1", sharedPort, 1, 4,
              FrontendProfile.APACHE_3_1_3, null));

      try {
        AdditionalFrontendThriftServers.open(buildConfig(extras), noopHandler(), null);
        Assert.fail("expected bind failure for occupied port " + sharedPort);
      } catch (Exception expected) {
        Assert.assertTrue("partial-start cleanup must not leave a listener thread running",
            awaitNoListenerThreads("hms-proxy-fe-leakcheck"));
      }
    }
  }

  @Test
  public void compatibilityLayerUsesActiveListenerFrontendProfileFromRequestContext() {
    ProxyConfig config = buildConfig(List.of());
    io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer compatibilityLayer =
        new io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer(config, null);

    Assert.assertEquals(FrontendProfile.APACHE_3_1_3.metastoreVersion(), compatibilityLayer.frontendVersion());

    FrontendProfile previous = io.github.mmalykhin.hmsproxy.security.ClientRequestContext
        .setFrontendProfile(FrontendProfile.APACHE_4_1_0);
    try {
      Assert.assertEquals(FrontendProfile.APACHE_4_1_0.metastoreVersion(), compatibilityLayer.frontendVersion());
      Assert.assertEquals(
          io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile.APACHE_4_1_0,
          compatibilityLayer.frontendRuntimeProfile());
    } finally {
      io.github.mmalykhin.hmsproxy.security.ClientRequestContext.restoreFrontendProfile(previous);
    }
    Assert.assertEquals(FrontendProfile.APACHE_3_1_3.metastoreVersion(), compatibilityLayer.frontendVersion());
  }

  @Test
  public void additionalListenerSetsFrontendProfileAndClearsThreadLocalMetaConf() throws Exception {
    java.nio.file.Path hdpJar = java.nio.file.Path.of(
        "hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar");
    org.junit.Assume.assumeTrue(java.nio.file.Files.isReadable(hdpJar));

    int port = freePort();
    List<AdditionalFrontendConfig> extras = List.of(new AdditionalFrontendConfig(
        "hdp",
        "127.0.0.1",
        port,
        1,
        1,
        FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78,
        hdpJar.toString()));
    ProxyConfig config = buildConfig(extras);
    io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer compatibilityLayer =
        new io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer(config, null);

    java.util.concurrent.atomic.AtomicReference<FrontendProfile> observedProfile =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicReference<String> observedVersion =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicReference<String> leakedThreadLocalMetaConf =
        new java.util.concurrent.atomic.AtomicReference<>();
    List<String> observedConnectionMetaConf = new java.util.concurrent.CopyOnWriteArrayList<>();

    ThriftHiveMetastore.Iface handler = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if (method.getDeclaringClass() == Object.class) {
            return method.invoke(proxy, args);
          }
          if ("set_ugi".equals(method.getName())) {
            return List.of();
          }
          if ("setMetaConf".equals(method.getName())) {
            io.github.mmalykhin.hmsproxy.security.ClientRequestContext.setSessionMetaConf(
                (String) args[0], (String) args[1]);
            org.apache.thrift.transport.TTransport previousTransport =
                io.github.mmalykhin.hmsproxy.security.ClientRequestContext.setCurrentTransport(null);
            try {
              io.github.mmalykhin.hmsproxy.security.ClientRequestContext.setSessionMetaConf(
                  "thread.local.leak.key", "leaked");
            } finally {
              io.github.mmalykhin.hmsproxy.security.ClientRequestContext.restoreCurrentTransport(previousTransport);
            }
            return null;
          }
          if ("get_databases".equals(method.getName()) || "get_all_databases".equals(method.getName())) {
            observedProfile.set(
                io.github.mmalykhin.hmsproxy.security.ClientRequestContext.frontendProfile().orElse(null));
            observedVersion.set(compatibilityLayer.frontendVersion());
            observedConnectionMetaConf.add(
                io.github.mmalykhin.hmsproxy.security.ClientRequestContext.sessionMetaConf("hive.metastore.try.direct.sql")
                    .orElse(null));
            org.apache.thrift.transport.TTransport previousTransport =
                io.github.mmalykhin.hmsproxy.security.ClientRequestContext.setCurrentTransport(null);
            try {
              leakedThreadLocalMetaConf.set(
                  io.github.mmalykhin.hmsproxy.security.ClientRequestContext.sessionMetaConf("thread.local.leak.key")
                      .orElse(null));
            } finally {
              io.github.mmalykhin.hmsproxy.security.ClientRequestContext.restoreCurrentTransport(previousTransport);
            }
            return List.of("default");
          }
          throw new UnsupportedOperationException(method.getName());
        });

    try (AdditionalFrontendThriftServers ignored = AdditionalFrontendThriftServers.open(config, handler, null)) {
      Assert.assertTrue(tcpConnect("127.0.0.1", port));
      org.apache.hadoop.hive.conf.HiveConf conf = new org.apache.hadoop.hive.conf.HiveConf();
      conf.setVar(org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS, "thrift://127.0.0.1:" + port);
      try (org.apache.hadoop.hive.metastore.HiveMetaStoreClient client1 =
               new org.apache.hadoop.hive.metastore.HiveMetaStoreClient(conf)) {
        client1.setMetaConf("hive.metastore.try.direct.sql", "false");
        Assert.assertEquals(List.of("default"), client1.getAllDatabases());
      }
      try (org.apache.hadoop.hive.metastore.HiveMetaStoreClient client2 =
               new org.apache.hadoop.hive.metastore.HiveMetaStoreClient(conf)) {
        Assert.assertEquals(List.of("default"), client2.getAllDatabases());
      }
    }
    Assert.assertEquals(FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, observedProfile.get());
    Assert.assertEquals(FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78.metastoreVersion(), observedVersion.get());
    Assert.assertNull("thread-local MetaConf must be cleared at end of request", leakedThreadLocalMetaConf.get());
    Assert.assertEquals(java.util.Arrays.asList("false", null), observedConnectionMetaConf);
  }

  private static List<Thread> listenerThreads(String namePrefix) {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .filter(thread -> thread.getName().startsWith(namePrefix))
        .toList();
  }

  private static boolean awaitNoListenerThreads(String namePrefix) throws Exception {
    for (int attempt = 0; attempt < 50; attempt++) {
      if (listenerThreads(namePrefix).isEmpty()) {
        return true;
      }
      Thread.sleep(100L);
    }
    return false;
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
