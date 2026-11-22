package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.frontend.FrontendProcessorFactory;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.junit.Assert;
import org.junit.Test;

public class MetastoreThriftServerDoSTest {
  @Test
  public void simpleModeCapturesRemoteAddressAndEnforcesStringLimit() throws Exception {
    int port = freePort();
    AtomicReference<String> capturedRemoteAddress = new AtomicReference<>();
    ThriftHiveMetastore.Iface handler = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[]{ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if ("get_database".equals(method.getName())) {
            capturedRemoteAddress.set(ClientRequestContext.remoteAddress().orElse(null));
            return new Database((String) args[0], "desc", "file:/tmp/db", Map.of());
          }
          return null;
        });

    ClientSocketConfig socketConfig = new ClientSocketConfig(
        5_000, true, 30, 10, 3, 64L, 100L, 5_000, 1024);
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("dos-test", "127.0.0.1", port, 1, 4, socketConfig, 5))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("c1")
        .catalogs(Map.of("c1", new CatalogConfig(
            "c1", null, null, false, CatalogAccessMode.READ_WRITE, List.of(), null, null,
            Map.of("hive.metastore.uris", "thrift://hms:9083"))))
        .backend(new BackendConfig(Map.of()))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    MetastoreThriftServer server =
        new MetastoreThriftServer(config, FrontendProcessorFactory.create(config, handler), null);
    Thread serverThread = new Thread(server::serve, "thrift-dos-test-server");
    serverThread.setDaemon(true);
    serverThread.start();
    try {
      Database db = callGetDatabaseWithRetry(port, "default");
      Assert.assertEquals("default", db.getName());
      Assert.assertEquals("127.0.0.1", capturedRemoteAddress.get());

      String oversized = "x".repeat(200);
      try {
        callGetDatabase(port, oversized);
        Assert.fail("Expected Thrift server to reject string exceeding server.thrift.max-string-length-bytes");
      } catch (Exception expected) {
        // Transport closed / protocol error from string length limit enforcement.
      }
    } finally {
      server.stop();
      serverThread.join(5_000L);
    }
  }

  private static Database callGetDatabaseWithRetry(int port, String name) throws Exception {
    long deadline = System.currentTimeMillis() + 5_000L;
    while (true) {
      try {
        return callGetDatabase(port, name);
      } catch (Exception e) {
        if (System.currentTimeMillis() >= deadline) {
          throw e;
        }
        Thread.sleep(25L);
      }
    }
  }

  private static Database callGetDatabase(int port, String name) throws Exception {
    try (TSocket socket = new TSocket("127.0.0.1", port, 3_000)) {
      socket.open();
      ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(socket));
      return client.get_database(name);
    }
  }

  private static int freePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
