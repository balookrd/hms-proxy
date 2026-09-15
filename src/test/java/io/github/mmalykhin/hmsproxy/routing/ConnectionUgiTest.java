package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransport;
import org.junit.Assert;
import org.junit.Test;

public class ConnectionUgiTest {

  @Test
  public void setUgiBindsIdentityToCurrentTransportAndResolves() throws Throwable {
    TTransport transport = new TMemoryBuffer(1024);
    String prevUser = ClientRequestContext.remoteUser().orElse(null);
    TTransport prevTransport = ClientRequestContext.setCurrentTransport(transport);
    try {
      Method setUgiMethod = ThriftHiveMetastore.Iface.class.getMethod("set_ugi", String.class, List.class);

      SetUgiHandler handler = new SetUgiHandler(
          new RoutingSupport(null, null, null, null, null, null, null, null, null),
          new NamespaceFallback() {
            @Override
            public Object invokeGlobal(Method method, Object[] args) {
              return List.of("hadoop");
            }

            @Override
            public Object routeByNamespaceOrFail(Method method, Object[] args) {
              return null;
            }
          }
      );

      // 1. Client calls set_ugi on this transport
      handler.handle(setUgiMethod, new Object[]{"iabunakov", List.of("hadoop")});

      // 2. Transport now has connection UGI
      Optional<ImpersonationContext> connUgi = ClientRequestContext.connectionUgi(transport);
      Assert.assertTrue(connUgi.isPresent());
      Assert.assertEquals("iabunakov", connUgi.get().userName());
      Assert.assertEquals(List.of("hadoop"), connUgi.get().groupNames());

      // 3. ImpersonationResolver resolves this connection UGI even if service principal is configured
      SecurityConfig security = new SecurityConfig(
          SecurityMode.KERBEROS,
          "hive/_HOST@EXAMPLE.COM",
          "hive/_HOST@EXAMPLE.COM",
          "/etc/security/keytabs/hive.keytab",
          "/etc/security/keytabs/hive.keytab",
          true,
          Map.of()
      );
      CatalogConfig catalog = new CatalogConfig(
          "hdp",
          "desc",
          "file:///desc",
          true,
          CatalogAccessMode.READ_WRITE,
          List.of(),
          null,
          null,
          Map.of("hive.metastore.uris", "thrift://localhost:9083")
      );
      ProxyConfig config = ProxyConfig.builder()
          .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
          .security(security)
          .defaultCatalog("hdp")
          .catalogs(Map.of("hdp", catalog))
          .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
          .build();

      ImpersonationResolver resolver = new ImpersonationResolver(config);
      Optional<ImpersonationContext> resolved = resolver.resolve();
      Assert.assertTrue(resolved.isPresent());
      Assert.assertEquals("iabunakov", resolved.get().userName());
      Assert.assertEquals(List.of("hadoop"), resolved.get().groupNames());
    } finally {
      ClientRequestContext.restoreCurrentTransport(prevTransport);
      ClientRequestContext.restoreRemoteUser(prevUser);
      ClientRequestContext.setConnectionUgi(transport, null);
    }
  }

  @Test
  public void setUgiPreservesUserAcrossFrontDoorRequestsForCreateTable() throws Exception {
    TTransport transport = new TMemoryBuffer(1024);
    org.apache.thrift.protocol.TProtocol protocol = new org.apache.thrift.protocol.TBinaryProtocol(transport);

    java.util.concurrent.atomic.AtomicReference<String> seenUserInCreateTable = new java.util.concurrent.atomic.AtomicReference<>();
    org.apache.thrift.TProcessor innerProcessor = (in, out) -> {
      seenUserInCreateTable.set(ClientRequestContext.remoteUser().orElse(null));
      return true;
    };

    java.util.function.Supplier<String> saslRemoteUser = () -> "hive";
    org.apache.thrift.TProcessor wrapped = io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity.wrapWithClientRequestContext(
        innerProcessor,
        p -> p,
        () -> "127.0.0.1",
        saslRemoteUser
    );

    try {
      // 1. Client calls set_ugi on this transport
      ImpersonationContext impersonation = new ImpersonationContext("iabunakov", List.of("hadoop"));
      ClientRequestContext.setConnectionUgi(transport, impersonation);

      // 2. Client calls create_table on the same connection.
      // Even though saslRemoteUser returns "hive", the effective user must be "iabunakov".
      wrapped.process(protocol, protocol);

      Assert.assertEquals("iabunakov", seenUserInCreateTable.get());
      Assert.assertEquals(Optional.empty(), ClientRequestContext.remoteUser());
    } finally {
      ClientRequestContext.setConnectionUgi(transport, null);
    }
  }
}
