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

public class RoutingMetaStoreProxySyntheticReadLocksTest {
  @Test
  public void syntheticReadLockLifecycleStaysInsideProxyForNonDefaultCatalog() throws Throwable {
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

    AtomicInteger defaultBackendCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              defaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 41L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(41L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method unlockMethod = ThriftHiveMetastore.Iface.class.getMethod("unlock", UnlockRequest.class);
    handler.invoke(null, unlockMethod, new Object[] {new UnlockRequest(lock.getLockid())});

    NoSuchLockException error = Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
    Assert.assertTrue(error.getMessage().contains("Synthetic read lock"));
    Assert.assertEquals(0, defaultBackendCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
  }

  @Test
  public void syntheticReadLockHeartbeatForwardsTxnHeartbeatToDefaultBackend() throws Throwable {
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

    AtomicReference<HeartbeatRequest> capturedHeartbeat = new AtomicReference<>();
    AtomicInteger defaultBackendCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("heartbeat".equals(method.getName())) {
                defaultBackendCalls.incrementAndGet();
                capturedHeartbeat.set((HeartbeatRequest) args[0]);
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 52L)});

    Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
    HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
    heartbeatRequest.setTxnid(52L);
    heartbeatRequest.setLockid(lock.getLockid());
    handler.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

    Assert.assertEquals(1, defaultBackendCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertNotNull(capturedHeartbeat.get());
    Assert.assertTrue(capturedHeartbeat.get().isSetTxnid());
    Assert.assertEquals(52L, capturedHeartbeat.get().getTxnid());
    Assert.assertFalse(capturedHeartbeat.get().isSetLockid());
  }

  @Test
  public void syntheticNoTxnDbLockForNonDefaultCatalogUsesShim() throws Throwable {
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

    AtomicReference<HeartbeatRequest> capturedHeartbeat = new AtomicReference<>();
    AtomicInteger defaultHeartbeatCalls = new AtomicInteger();
    AtomicInteger defaultAbortCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("heartbeat".equals(method.getName())) {
                defaultHeartbeatCalls.incrementAndGet();
                capturedHeartbeat.set((HeartbeatRequest) args[0]);
                return null;
              }
              if ("abort_txn".equals(method.getName())) {
                defaultAbortCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticNoTxnDbLockRequest("catalog2__sales", 52L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(52L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
    HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
    heartbeatRequest.setTxnid(52L);
    heartbeatRequest.setLockid(lock.getLockid());
    handler.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

    Method abortMethod = ThriftHiveMetastore.Iface.class.getMethod("abort_txn", AbortTxnRequest.class);
    handler.invoke(null, abortMethod, new Object[] {new AbortTxnRequest(52L)});

    Assert.assertEquals(1, defaultHeartbeatCalls.get());
    Assert.assertEquals(1, defaultAbortCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertNotNull(capturedHeartbeat.get());
    Assert.assertEquals(52L, capturedHeartbeat.get().getTxnid());
    Assert.assertFalse(capturedHeartbeat.get().isSetLockid());
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void syntheticNoTxnExclusivePartitionLockForNonDefaultCatalogUsesShim() throws Throwable {
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

    AtomicInteger defaultAbortCalls = new AtomicInteger();
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("abort_txn".equals(method.getName())) {
                defaultAbortCalls.incrementAndGet();
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticNoTxnExclusivePartitionLockRequest(
            "catalog2__sales",
            "events",
            "p=2026-03-31",
            61L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(61L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method abortMethod = ThriftHiveMetastore.Iface.class.getMethod("abort_txn", AbortTxnRequest.class);
    handler.invoke(null, abortMethod, new Object[] {new AbortTxnRequest(61L)});

    Assert.assertEquals(1, defaultAbortCalls.get());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void lockRequestSpanningTwoCatalogsRoutesToTheDefaultCatalog() throws Throwable {
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
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
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
                return new LockResponse(64L, LockState.ACQUIRED);
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockRequest request = multiComponentLockRequest(
        71L,
        noTxnLockComponent(LockType.SHARED_READ, "catalog2__sales", "events"),
        noTxnLockComponent(LockType.EXCLUSIVE, "finance", "ledger"));

    LockResponse lock = (LockResponse) handler.invoke(null, lockMethod, new Object[] {request});

    // The default catalog owns the TxnHandler, so it is the one that keeps its real lock; the
    // component of the other catalog is dropped rather than rewritten onto this backend.
    Assert.assertEquals(64L, lock.getLockid());
    Assert.assertEquals(1, capturedRequest.get().getComponentSize());
    Assert.assertEquals("finance", capturedRequest.get().getComponent().get(0).getDbname());
    Assert.assertEquals("ledger", capturedRequest.get().getComponent().get(0).getTablename());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    // The client's request object must survive untouched: it is a thrift processor argument.
    Assert.assertEquals(2, request.getComponentSize());
    Assert.assertEquals("catalog2__sales", request.getComponent().get(0).getDbname());
    Assert.assertTrue(observability.metrics().render()
        .contains("hms_proxy_lock_request_split_total{catalog=\"catalog1\"} 1"));
  }

  @Test
  public void lockRequestSpanningTwoDatabasesOfOneCatalogKeepsEveryComponent() throws Throwable {
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
                return new LockResponse(65L, LockState.ACQUIRED);
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

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockRequest request = multiComponentLockRequest(
        72L,
        noTxnLockComponent(LockType.SHARED_READ, "finance", "ledger"),
        noTxnLockComponent(LockType.EXCLUSIVE, "sales", "events"));

    LockResponse lock = (LockResponse) handler.invoke(null, lockMethod, new Object[] {request});

    // Both databases live in the same catalog, so both components reach the one backend that can
    // lock them - each rewritten to its own database rather than all of them to a single one.
    Assert.assertEquals(65L, lock.getLockid());
    Assert.assertEquals(2, capturedRequest.get().getComponentSize());
    Assert.assertEquals("finance", capturedRequest.get().getComponent().get(0).getDbname());
    Assert.assertEquals("sales", capturedRequest.get().getComponent().get(1).getDbname());
  }

  @Test
  public void lockRequestWithSeveralComponentsInOneNamespaceStillUsesShim() throws Throwable {
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

    AtomicInteger backendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              backendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              backendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {multiComponentLockRequest(
            73L,
            noTxnLockComponent(LockType.SHARED_READ, "catalog2__sales", "events"),
            noTxnLockComponent(LockType.SHARED_READ, "catalog2__sales", "orders"))});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void lockRequestSpanningTwoCatalogsKeepsTheDummyPlaceholderWithTheRoutedComponents() throws Throwable {
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
    AtomicInteger nonDefaultBackendCalls = new AtomicInteger();
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
                return new LockResponse(66L, LockState.ACQUIRED);
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              nonDefaultBackendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockRequest request = multiComponentLockRequest(
        74L,
        dummyPlaceholderLockComponent(),
        noTxnLockComponent(LockType.SHARED_READ, "catalog2__sales", "events"),
        noTxnLockComponent(LockType.EXCLUSIVE, "finance", "ledger"));

    LockResponse lock = (LockResponse) handler.invoke(null, lockMethod, new Object[] {request});

    // The placeholder belongs to no catalog, so it never picks one and never counts as a component
    // that had to be dropped: it travels with whichever backend the real components selected.
    Assert.assertEquals(66L, lock.getLockid());
    Assert.assertEquals(2, capturedRequest.get().getComponentSize());
    Assert.assertEquals("_dummy_database", capturedRequest.get().getComponent().get(0).getDbname());
    Assert.assertEquals("finance", capturedRequest.get().getComponent().get(1).getDbname());
    Assert.assertEquals(0, nonDefaultBackendCalls.get());
    Assert.assertTrue(observability.metrics().render()
        .contains("hms_proxy_lock_request_split_total{catalog=\"catalog1\"} 1"));
  }

  @Test
  public void selectLockWithDummyPlaceholderStillUsesSyntheticShim() throws Throwable {
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

    AtomicInteger backendCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              backendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              backendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {multiComponentLockRequest(
            75L,
            dummyPlaceholderLockComponent(),
            selectLockComponent("catalog2__sales", "events"))});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void syntheticReadLocksAreReleasedWhenTxnCompletes() throws Throwable {
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

    AtomicInteger commitCalls = new AtomicInteger();
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("commit_txn".equals(method.getName())) {
                commitCalls.incrementAndGet();
                return null;
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

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 77L)});

    Method commitMethod = ThriftHiveMetastore.Iface.class.getMethod("commit_txn", CommitTxnRequest.class);
    handler.invoke(null, commitMethod, new Object[] {new CommitTxnRequest(77L)});

    Assert.assertEquals(1, commitCalls.get());
    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void syntheticReadLockMetricsAreRecordedForInMemoryShim() throws Throwable {
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

    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              if ("heartbeat".equals(method.getName())) {
                return null;
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
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 91L)});

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(91L);
    handler.invoke(null, checkLockMethod, new Object[] {checkRequest});

    Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
    HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
    heartbeatRequest.setTxnid(91L);
    heartbeatRequest.setLockid(lock.getLockid());
    handler.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

    Method unlockMethod = ThriftHiveMetastore.Iface.class.getMethod("unlock", UnlockRequest.class);
    handler.invoke(null, unlockMethod, new Object[] {new UnlockRequest(lock.getLockid())});

    String rendered = observability.metrics().render();

    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"acquire\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"acquired\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"check_lock\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"hit\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"touched\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"txn_forwarded\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"unlock\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"released\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_store_info{store_mode=\"in_memory\"} 1.0"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_locks_active{store_mode=\"in_memory\"} 0.0"));
  }

  @Test
  public void syntheticReadLocksCanFailOverAcrossProxyInstancesViaZooKeeperStore() throws Throwable {
    try (TestingServer zooKeeper = startTestingServerOrSkip()) {
      ProxyConfig config = ProxyConfig.builder()
          .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
          .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
          .catalogDbSeparator("__")
          .defaultCatalog("catalog1")
          .catalogs(Map.of(
              "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
              "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
          .backend(new BackendConfig(Map.of()))
          .compatibility(new CompatibilityConfig(false))
          .federation(new FederationConfig(false, ViewTextRewriteMode.DISABLED, false))
          .transactionalDdlGuard(new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()))
          .management(new ManagementConfig(false, "127.0.0.1", 10083))
          .syntheticReadLockStore(syntheticReadLockStoreConfig(zooKeeper.getConnectString()))
          .build();

      AtomicInteger firstProxyBackendCalls = new AtomicInteger();
      CatalogBackend defaultBackendA = newBackend(
          config,
          config.catalogs().get("catalog1"),
          new ApacheBackendAdapter(),
          newBackendRuntime(
              config,
              config.catalogs().get("catalog1"),
              newSession((proxy, method, args) -> {
                firstProxyBackendCalls.incrementAndGet();
                throw new UnsupportedOperationException(method.getName());
              })));
      CatalogBackend nonDefaultBackendA = newBackend(
          config,
          config.catalogs().get("catalog2"),
          new ApacheBackendAdapter(),
          newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));

      AtomicReference<HeartbeatRequest> capturedHeartbeat = new AtomicReference<>();
      AtomicInteger secondProxyCommitCalls = new AtomicInteger();
      CatalogBackend defaultBackendB = newBackend(
          config,
          config.catalogs().get("catalog1"),
          new ApacheBackendAdapter(),
          newBackendRuntime(
              config,
              config.catalogs().get("catalog1"),
              newSession((proxy, method, args) -> {
                if ("heartbeat".equals(method.getName())) {
                  capturedHeartbeat.set((HeartbeatRequest) args[0]);
                  return null;
                }
                if ("commit_txn".equals(method.getName())) {
                  secondProxyCommitCalls.incrementAndGet();
                  return null;
                }
                throw new UnsupportedOperationException(method.getName());
              })));
      CatalogBackend nonDefaultBackendB = newBackend(
          config,
          config.catalogs().get("catalog2"),
          new ApacheBackendAdapter(),
          newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));

      LinkedHashMap<String, CatalogBackend> backendsA = new LinkedHashMap<>();
      backendsA.put("catalog1", defaultBackendA);
      backendsA.put("catalog2", nonDefaultBackendA);
      LinkedHashMap<String, CatalogBackend> backendsB = new LinkedHashMap<>();
      backendsB.put("catalog1", defaultBackendB);
      backendsB.put("catalog2", nonDefaultBackendB);
      ProxyObservability observabilityA = new ProxyObservability(config);
      ProxyObservability observabilityB = new ProxyObservability(config);

      try (CatalogRouter routerA = new CatalogRouter(config, backendsA);
           CatalogRouter routerB = new CatalogRouter(config, backendsB);
           RoutingMetaStoreProxy secondProxy =
               new RoutingMetaStoreProxy(config, routerB, new FederationLayer(config, routerB), null, observabilityB)) {
        Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
        LockResponse lock;
        RoutingMetaStoreProxy firstProxy = new RoutingMetaStoreProxy(config, routerA, new FederationLayer(config, routerA), null, observabilityA);
        try {
          lock = (LockResponse) firstProxy.invoke(
              null,
              lockMethod,
              new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 88L)});
        } finally {
          firstProxy.close();
        }

        Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
        CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
        checkRequest.setTxnid(88L);
        LockResponse checked = (LockResponse) secondProxy.invoke(null, checkLockMethod, new Object[] {checkRequest});

        Assert.assertEquals(LockState.ACQUIRED, checked.getState());

        Method heartbeatMethod = ThriftHiveMetastore.Iface.class.getMethod("heartbeat", HeartbeatRequest.class);
        HeartbeatRequest heartbeatRequest = new HeartbeatRequest();
        heartbeatRequest.setTxnid(88L);
        heartbeatRequest.setLockid(lock.getLockid());
        secondProxy.invoke(null, heartbeatMethod, new Object[] {heartbeatRequest});

        Assert.assertNotNull(capturedHeartbeat.get());
        Assert.assertEquals(88L, capturedHeartbeat.get().getTxnid());
        Assert.assertFalse(capturedHeartbeat.get().isSetLockid());

        Method commitMethod = ThriftHiveMetastore.Iface.class.getMethod("commit_txn", CommitTxnRequest.class);
        secondProxy.invoke(null, commitMethod, new Object[] {new CommitTxnRequest(88L)});

        Assert.assertEquals(1, secondProxyCommitCalls.get());
        Assert.assertEquals(0, firstProxyBackendCalls.get());
        Assert.assertThrows(
            NoSuchLockException.class,
            () -> secondProxy.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));

        String rendered = observabilityB.metrics().render();
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_handoffs_total{operation=\"check_lock\",catalog=\"catalog2\",store_mode=\"zookeeper\"} 1"));
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_handoffs_total{operation=\"heartbeat\",catalog=\"catalog2\",store_mode=\"zookeeper\"} 1"));
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_handoffs_total{operation=\"release_txn\",catalog=\"all\",store_mode=\"zookeeper\"} 1"));
        Assert.assertTrue(rendered.contains(
            "hms_proxy_synthetic_read_lock_store_info{store_mode=\"zookeeper\"} 1.0"));
      }
    }
  }

  @Test
  public void throttledSyntheticReadLockDoesNotLeakLockState() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .rateLimit(new RateLimitConfig(
            RateLimitPolicyConfig.disabled(),
            RateLimitPolicyConfig.disabled(),
            Map.of(),
            Map.of(),
            Map.of("catalog2", new RateLimitPolicyConfig(1, 1)),
            Map.of()))
        .build();

    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession()));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession()));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);
    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 41L)});
    Assert.assertEquals(LockState.ACQUIRED, lock.getState());

    Assert.assertThrows(
        RateLimitExceededException.class,
        () -> handler.invoke(
            null,
            lockMethod,
            new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 42L)}));

    String rendered = observability.metrics().render();
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_lock_events_total{operation=\"acquire\",catalog=\"catalog2\",store_mode=\"in_memory\",result=\"acquired\"} 1"));
    Assert.assertTrue(rendered.contains(
        "hms_proxy_synthetic_read_locks_active{store_mode=\"in_memory\"} 1.0"));
  }

  @Test
  public void syntheticSharedWriteInsertLockLifecycleStaysInsideProxyForNonDefaultCatalog() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_WRITE, List.of(), backendCalls);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticWriteLockRequest(
            "catalog2__sales", "events", 61L, LockType.SHARED_WRITE, DataOperationType.INSERT)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);

    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    CheckLockRequest checkRequest = new CheckLockRequest(lock.getLockid());
    checkRequest.setTxnid(61L);
    LockResponse checked = (LockResponse) handler.invoke(null, checkLockMethod, new Object[] {checkRequest});
    Assert.assertEquals(LockState.ACQUIRED, checked.getState());

    Method unlockMethod = ThriftHiveMetastore.Iface.class.getMethod("unlock", UnlockRequest.class);
    handler.invoke(null, unlockMethod, new Object[] {new UnlockRequest(lock.getLockid())});

    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
    Assert.assertEquals(0, backendCalls.get());
  }

  /**
   * Hive takes an EXCLUSIVE lock for an INSERT into a non-ACID table under the default
   * hive.txn.strict.locking.mode=true, and always for INSERT OVERWRITE.
   */
  @Test
  public void syntheticExclusiveInsertLockForNonDefaultCatalogUsesShim() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_WRITE, List.of(), backendCalls);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticWriteLockRequest(
            "catalog2__sales", "events", 62L, LockType.EXCLUSIVE, DataOperationType.INSERT)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void syntheticNonTransactionalUpdateLockForNonDefaultCatalogUsesShim() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_WRITE, List.of(), backendCalls);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticWriteLockRequest(
            "catalog2__sales", "events", 63L, LockType.SHARED_WRITE, DataOperationType.UPDATE)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void syntheticNonTransactionalDeleteLockForNonDefaultCatalogUsesShim() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_WRITE, List.of(), backendCalls);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticWriteLockRequest(
            "catalog2__sales", "events", 64L, LockType.SHARED_WRITE, DataOperationType.DELETE)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void syntheticWriteLocksAreReleasedWhenTxnCommits() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_WRITE, List.of(), backendCalls);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticWriteLockRequest(
            "catalog2__sales", "events", 65L, LockType.SHARED_WRITE, DataOperationType.INSERT)});

    Method commitMethod = ThriftHiveMetastore.Iface.class.getMethod("commit_txn", CommitTxnRequest.class);
    handler.invoke(null, commitMethod, new Object[] {new CommitTxnRequest(65L)});

    Assert.assertEquals(1, backendCalls.get());
    Method checkLockMethod = ThriftHiveMetastore.Iface.class.getMethod("check_lock", CheckLockRequest.class);
    Assert.assertThrows(
        NoSuchLockException.class,
        () -> handler.invoke(null, checkLockMethod, new Object[] {new CheckLockRequest(lock.getLockid())}));
  }

  @Test
  public void syntheticWriteLockIsRejectedForReadOnlyNonDefaultCatalog() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_ONLY, List.of(), backendCalls);
    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(
            null,
            lockMethod,
            new Object[] {syntheticWriteLockRequest(
                "catalog2__sales", "events", 66L, LockType.SHARED_WRITE, DataOperationType.INSERT)}));

    Assert.assertTrue(error.getMessage().contains("READ_ONLY"));
    Assert.assertEquals(0, backendCalls.get());
  }

  @Test
  public void syntheticWriteLockIsRejectedForDatabaseOutsideWriteWhitelist() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler =
        writeLockShimHandler(CatalogAccessMode.READ_WRITE_DB_WHITELIST, List.of("finance"), backendCalls);
    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(
            null,
            lockMethod,
            new Object[] {syntheticWriteLockRequest(
                "catalog2__sales", "events", 67L, LockType.SHARED_WRITE, DataOperationType.INSERT)}));

    Assert.assertTrue(error.getMessage().contains("sales"));
    Assert.assertEquals(0, backendCalls.get());
  }

  /** The access-mode check is scoped to write components: reads of a READ_ONLY catalog still work. */
  @Test
  public void syntheticReadLockStillUsesShimForReadOnlyNonDefaultCatalog() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_ONLY, List.of(), backendCalls);
    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {syntheticReadLockRequest("catalog2__sales", "events", 68L)});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  /**
   * A component of another catalog is dropped from the request that reaches the backend, but it is
   * still the proxy's job to refuse a write into a READ_ONLY catalog: nothing downstream will ever
   * see that component, so dropping it must not drop the access-mode check with it.
   */
  @Test
  public void writeComponentOfAReadOnlyCatalogIsRefusedEvenWhenItIsDroppedFromTheRequest()
      throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_ONLY, List.of(), backendCalls);
    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);

    MetaException error = Assert.assertThrows(
        MetaException.class,
        () -> handler.invoke(
            null,
            lockMethod,
            new Object[] {multiComponentLockRequest(
                75L,
                // The default catalog decides where the request is routed, so the READ_ONLY
                // component below is the one that gets dropped.
                noTxnLockComponent(LockType.EXCLUSIVE, "finance", "ledger"),
                writeLockComponent(
                    LockType.EXCLUSIVE, DataOperationType.INSERT, "catalog2__sales", "events"))}));

    Assert.assertTrue(error.getMessage(), error.getMessage().contains("READ_ONLY"));
    Assert.assertEquals(0, backendCalls.get());
  }

  /**
   * With no component of the default catalog there is no real lock to keep, so the request routes by
   * the first catalog it names and the synthetic shim answers for it.
   */
  @Test
  public void lockRequestSpanningTwoNonDefaultCatalogsRoutesByTheFirstOne() throws Throwable {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2", catalogConfig("catalog2", "c2", null, null, Map.of("hive.metastore.uris", "thrift://two")),
            "catalog3", catalogConfig("catalog3", "c3", null, null, Map.of("hive.metastore.uris", "thrift://three"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    AtomicInteger backendCalls = new AtomicInteger();
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    for (String catalog : List.of("catalog1", "catalog2", "catalog3")) {
      backends.put(catalog, newBackend(
          config,
          config.catalogs().get(catalog),
          new ApacheBackendAdapter(),
          newBackendRuntime(
              config,
              config.catalogs().get(catalog),
              newSession((proxy, method, args) -> {
                backendCalls.incrementAndGet();
                throw new UnsupportedOperationException(method.getName());
              }))));
    }
    CatalogRouter router = new CatalogRouter(config, backends);
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {multiComponentLockRequest(
            76L,
            selectLockComponent("catalog2__sales", "events"),
            selectLockComponent("catalog3__finance", "ledger"))});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  /**
   * {@code INSERT ... VALUES} locks Hive's pseudo source together with the target table, so the
   * request names two databases that live in different catalogs. The pseudo source exists in no
   * metastore, so it must not decide the namespace or the shim eligibility of the request.
   */
  @Test
  public void insertValuesLockWithHiveDummySourceComponentUsesShim() throws Throwable {
    AtomicInteger backendCalls = new AtomicInteger();
    RoutingMetaStoreProxy handler = writeLockShimHandler(CatalogAccessMode.READ_WRITE, List.of(), backendCalls);

    Method lockMethod = ThriftHiveMetastore.Iface.class.getMethod("lock", LockRequest.class);
    LockResponse lock = (LockResponse) handler.invoke(
        null,
        lockMethod,
        new Object[] {multiComponentLockRequest(
            69L,
            hiveDummySourceLockComponent(),
            writeLockComponent(LockType.EXCLUSIVE, DataOperationType.INSERT, "catalog2__sales", "events"))});

    Assert.assertEquals(LockState.ACQUIRED, lock.getState());
    Assert.assertTrue(lock.getLockid() >= Long.MAX_VALUE / 2);
    Assert.assertEquals(0, backendCalls.get());
  }

  /**
   * Two catalogs where the non-default one carries the given access mode; the backends only count
   * calls, so any lock that escapes the shim shows up as a backend call.
   */
  private static RoutingMetaStoreProxy writeLockShimHandler(
      CatalogAccessMode accessMode,
      List<String> writeDbWhitelist,
      AtomicInteger backendCalls
  ) throws Exception {
    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1", catalogConfig("catalog1", "c1", null, null, Map.of("hive.metastore.uris", "thrift://one")),
            "catalog2",
            new CatalogConfig(
                "catalog2",
                "c2",
                "file:///c2",
                false,
                accessMode,
                writeDbWhitelist,
                null,
                null,
                Map.of("hive.metastore.uris", "thrift://two"))))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog1"),
            newSession((proxy, method, args) -> {
              backendCalls.incrementAndGet();
              if ("commit_txn".equals(method.getName())) {
                return null;
              }
              throw new UnsupportedOperationException(method.getName());
            })));
    CatalogBackend nonDefaultBackend = newBackend(
        config,
        config.catalogs().get("catalog2"),
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            config.catalogs().get("catalog2"),
            newSession((proxy, method, args) -> {
              backendCalls.incrementAndGet();
              throw new UnsupportedOperationException(method.getName());
            })));
    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", defaultBackend);
    backends.put("catalog2", nonDefaultBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    return new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null);
  }

}
