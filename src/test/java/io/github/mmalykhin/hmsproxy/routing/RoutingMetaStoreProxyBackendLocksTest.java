package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.AbstractBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendInvocationSession;
import io.github.mmalykhin.hmsproxy.backend.BackendRuntime;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.IsolatedInvocationBridge;
import io.github.mmalykhin.hmsproxy.backend.IsolatedMetastoreClient;
import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableDropPurgeMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitPolicyConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreMode;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreZooKeeperConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class RoutingMetaStoreProxyBackendLocksTest {
  @Test
  public void lockRoutesByNamespaceAndRewritesNestedDbName() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicReference<LockRequest> capturedRequest = new AtomicReference<>();
    BackendInvocationSession session = newSession((proxy, method, args) -> {
      if ("lock".equals(method.getName())) {
        capturedRequest.set((LockRequest) args[0]);
        LockResponse response = new LockResponse();
        response.setLockid(7L);
        response.setState(LockState.ACQUIRED);
        return response;
      }
      throw new UnsupportedOperationException(method.getName());
    });
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), session));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", backend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    Object result = handler.invoke(null, method, new Object[] {lockRequest("catalog2__sales", "events")});

    Assert.assertTrue(result instanceof LockResponse);
    Assert.assertEquals("sales", capturedRequest.get().getComponent().get(0).getDbname());
    Assert.assertEquals("events", capturedRequest.get().getComponent().get(0).getTablename());
  }

  @Test
  public void defaultCatalogExclusiveNoTxnLockIsNotSubstitutedBySyntheticLock() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicReference<LockRequest> capturedRequest = new AtomicReference<>();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("lock".equals(method.getName())) {
                capturedRequest.set((LockRequest) args[0]);
                LockResponse response = new LockResponse();
                response.setLockid(11L);
                response.setState(LockState.ACQUIRED);
                return response;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    LockResponse lock = (LockResponse) handler.invoke(
        null,
        method,
        new Object[] {multiComponentLockRequest(
            81L,
            noTxnLockComponent(LockType.EXCLUSIVE, "finance", "ledger"))});

    Assert.assertEquals(11L, lock.getLockid());
    Assert.assertTrue(lock.getLockid() < Long.MAX_VALUE / 2);
    Assert.assertNotNull(capturedRequest.get());
    Assert.assertEquals(1, capturedRequest.get().getComponentSize());
    Assert.assertEquals(LockType.EXCLUSIVE, capturedRequest.get().getComponent().get(0).getType());
    Assert.assertEquals("finance", capturedRequest.get().getComponent().get(0).getDbname());
  }

  @Test
  public void backendLockTransportFailuresAreSurfacedAsMetaExceptions() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    BackendAdapter failingAdapter = new BackendAdapter() {
      @Override
      public Object invoke(
          CatalogBackend backend,
          Method backendMethod,
          Object[] args,
          ImpersonationContext impersonation
      ) throws Throwable {
        throw new TApplicationException(TApplicationException.INTERNAL_ERROR, "backend lock failed");
      }

      @Override
      public Object invokeRequest(
          CatalogBackend backend,
          String methodName,
          Object request,
          ImpersonationContext impersonation
      ) throws Throwable {
        throw new UnsupportedOperationException(methodName);
      }

      @Override
      public MetastoreCompatibility.BackendProfile backendProfile() {
        return MetastoreCompatibility.BackendProfile.MODERN_REQUESTS;
      }

      @Override
      public MetastoreRuntimeProfile runtimeProfile() {
        return MetastoreRuntimeProfile.APACHE_3_1_3;
      }

      @Override
      public String backendVersion() {
        return null;
      }
    };
    CatalogBackend backend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        failingAdapter,
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", null);
    backends.put("catalog2", backend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(null, method, new Object[] {lockRequest("catalog2__sales", "events")}));

    Assert.assertTrue(error.getMessage().contains("catalog2"));
    Assert.assertTrue(error.getMessage().contains("lock"));
    Assert.assertTrue(error.getMessage().contains("TApplicationException"));
    Assert.assertTrue(error.getMessage().contains("backend lock failed"));
  }

  /**
   * A transactional write component belongs to an ACID table, whose write ids can only be allocated
   * by the default catalog's TxnHandler. The shim must not answer for it: the request goes to the
   * backend and fails there instead of pretending the lock was taken.
   */
  @Test
  public void transactionalInsertLockForNonDefaultCatalogIsNotSubstitutedBySyntheticLock() throws Throwable {
    AtomicReference<LockRequest> capturedRequest = new AtomicReference<>();
    RoutingMetaStoreProxy handler = lockCapturingHandler("catalog2", capturedRequest);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    LockResponse lock = (LockResponse) handler.invoke(
        null,
        method,
        new Object[] {transactionalWriteLockRequest(
            "catalog2__sales", "events", 71L, LockType.SHARED_WRITE, DataOperationType.INSERT)});

    Assert.assertEquals(13L, lock.getLockid());
    Assert.assertTrue(lock.getLockid() < Long.MAX_VALUE / 2);
    Assert.assertNotNull(capturedRequest.get());
    Assert.assertEquals("sales", capturedRequest.get().getComponent().get(0).getDbname());
  }

  @Test
  public void defaultCatalogInsertLockIsNotSubstitutedBySyntheticLock() throws Throwable {
    AtomicReference<LockRequest> capturedRequest = new AtomicReference<>();
    RoutingMetaStoreProxy handler = lockCapturingHandler("catalog1", capturedRequest);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    LockResponse lock = (LockResponse) handler.invoke(
        null,
        method,
        new Object[] {syntheticWriteLockRequest(
            "finance", "ledger", 72L, LockType.SHARED_WRITE, DataOperationType.INSERT)});

    Assert.assertEquals(13L, lock.getLockid());
    Assert.assertTrue(lock.getLockid() < Long.MAX_VALUE / 2);
    Assert.assertNotNull(capturedRequest.get());
    Assert.assertEquals("finance", capturedRequest.get().getComponent().get(0).getDbname());
  }

  /** A lock that names only Hive's pseudo source has no namespace, so it follows the default pin. */
  @Test
  public void lockWithOnlyHiveDummySourceComponentGoesToDefaultBackend() throws Throwable {
    AtomicReference<LockRequest> capturedRequest = new AtomicReference<>();
    RoutingMetaStoreProxy handler = lockCapturingHandler("catalog1", capturedRequest);
    Method method = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    LockResponse lock = (LockResponse) handler.invoke(
        null,
        method,
        new Object[] {multiComponentLockRequest(73L, hiveDummySourceLockComponent())});

    Assert.assertEquals(13L, lock.getLockid());
    Assert.assertNotNull(capturedRequest.get());
    Assert.assertEquals("_dummy_database", capturedRequest.get().getComponent().get(0).getDbname());
  }

  private static RoutingMetaStoreProxy lockCapturingHandler(
      String capturingCatalog,
      AtomicReference<LockRequest> capturedRequest
  ) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String catalogName : List.of("catalog1", "catalog2")) {
      BackendInvocationSession session = catalogName.equals(capturingCatalog)
          ? newSession((proxy, method, args) -> {
            if ("lock".equals(method.getName())) {
              capturedRequest.set((LockRequest) args[0]);
              LockResponse response = new LockResponse();
              response.setLockid(13L);
              response.setState(LockState.ACQUIRED);
              return response;
            }
            throw new UnsupportedOperationException(method.getName());
          })
          : newSession();
      backends.put(
          catalogName,
          newBackend(
              config,
              config.catalogs().get(catalogName),
              new ApacheBackendAdapter(),
              newBackendRuntime(config, config.catalogs().get(catalogName), session)));
    }
    CatalogRouter router = new CatalogRouter(config, backends);
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
  }

}
