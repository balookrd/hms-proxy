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

      // 3. In SIMPLE/NONE mode ImpersonationResolver resolves this connection UGI
      SecurityConfig security = new SecurityConfig(
          SecurityMode.NONE,
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

  @Test
  public void setUgiWithEmptyGroupsResolvesFromUgi() throws Throwable {
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
              return List.of();
            }

            @Override
            public Object routeByNamespaceOrFail(Method method, Object[] args) {
              return null;
            }
          }
      );

      org.apache.hadoop.security.UserGroupInformation ugi =
          org.apache.hadoop.security.UserGroupInformation.createUserForTesting("testuser", new String[]{"ad_group1", "ad_group2"});

      ugi.doAs((java.security.PrivilegedExceptionAction<Void>) () -> {
        try {
          @SuppressWarnings("unchecked")
          List<String> returnedGroups = (List<String>) handler.handle(setUgiMethod, new Object[]{"testuser", List.of()});

          Assert.assertEquals(List.of("ad_group1", "ad_group2"), returnedGroups);
          Optional<ImpersonationContext> connUgi = ClientRequestContext.connectionUgi(transport);
          Assert.assertTrue(connUgi.isPresent());
          Assert.assertEquals("testuser", connUgi.get().userName());
          Assert.assertEquals(List.of("ad_group1", "ad_group2"), connUgi.get().groupNames());
          return null;
        } catch (Throwable t) {
          if (t instanceof Exception e) {
            throw e;
          }
          throw new RuntimeException(t);
        }
      });
    } finally {
      ClientRequestContext.restoreCurrentTransport(prevTransport);
      ClientRequestContext.restoreRemoteUser(prevUser);
      ClientRequestContext.setConnectionUgi(transport, null);
    }
  }

  @Test
  public void setUgiUnderKerberosDoesNotOverrideAuthenticatedIdentityOrGroups() throws Throwable {
    TTransport transport = new TMemoryBuffer(1024);
    String prevUser = ClientRequestContext.remoteUser().orElse(null);
    String prevAuthUser = ClientRequestContext.authenticatedUser().orElse(null);
    TTransport prevTransport = ClientRequestContext.setCurrentTransport(transport);
    try {
      SecurityConfig security = new SecurityConfig(
          SecurityMode.KERBEROS,
          "hive/proxy@EXAMPLE.COM",
          "hive/proxy@EXAMPLE.COM",
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

      Method setUgiMethod = ThriftHiveMetastore.Iface.class.getMethod("set_ugi", String.class, List.class);
      ImpersonationResolver resolver = new ImpersonationResolver(config);
      RoutingSupport support = new RoutingSupport(config, null, null, null, null, resolver, null, null, null);
      SetUgiHandler handler = new SetUgiHandler(
          support,
          new NamespaceFallback() {
            @Override
            public Object invokeGlobal(Method method, Object[] args) {
              return List.of();
            }

            @Override
            public Object routeByNamespaceOrFail(Method method, Object[] args) {
              return null;
            }
          });

      org.apache.hadoop.security.UserGroupInformation aliceUgi =
          org.apache.hadoop.security.UserGroupInformation.createUserForTesting("alice", new String[]{"sales_grp"});

      aliceUgi.doAs((java.security.PrivilegedExceptionAction<Void>) () -> {
        try {
          ClientRequestContext.setAuthenticatedUser("alice@EXAMPLE.COM");
          ClientRequestContext.setRemoteUser("alice@EXAMPLE.COM");

          @SuppressWarnings("unchecked")
          List<String> returnedGroups = (List<String>) handler.handle(
              setUgiMethod, new Object[]{"admin", List.of("supergroup", "wheel")});

          Assert.assertEquals("set_ugi under Kerberos must return server groups of authenticated user",
              List.of("sales_grp"), returnedGroups);
          Assert.assertTrue("set_ugi under Kerberos must not bind connectionUgi",
              ClientRequestContext.connectionUgi(transport).isEmpty());

          Optional<ImpersonationContext> resolved = resolver.resolve();
          Assert.assertTrue(resolved.isPresent());
          Assert.assertEquals("alice", resolved.get().userName());
          Assert.assertEquals(List.of("sales_grp"), resolved.get().groupNames());
          return null;
        } catch (Throwable t) {
          if (t instanceof Exception e) {
            throw e;
          }
          throw new RuntimeException(t);
        }
      });

      // Even if connectionUgi was populated elsewhere, Kerberos processor wrapper must ignore it.
      ClientRequestContext.setConnectionUgi(transport, new ImpersonationContext("admin", List.of("supergroup")));
      java.util.concurrent.atomic.AtomicReference<String> seenUser = new java.util.concurrent.atomic.AtomicReference<>();
      org.apache.thrift.protocol.TProtocol protocol = new org.apache.thrift.protocol.TBinaryProtocol(transport);
      org.apache.thrift.TProcessor wrapped = io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity
          .wrapWithClientRequestContext(
              (in, out) -> {
                seenUser.set(ClientRequestContext.remoteUser().orElse(null));
                return true;
              },
              p -> p,
              () -> "127.0.0.1",
              () -> "alice@EXAMPLE.COM",
              () -> "GSSAPI",
              false);
      wrapped.process(protocol, protocol);
      Assert.assertEquals("alice@EXAMPLE.COM", seenUser.get());
    } finally {
      ClientRequestContext.restoreCurrentTransport(prevTransport);
      ClientRequestContext.restoreAuthenticatedUser(prevAuthUser);
      ClientRequestContext.restoreRemoteUser(prevUser);
      ClientRequestContext.setConnectionUgi(transport, null);
    }
  }
}
