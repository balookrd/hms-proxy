package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.BackendInvocationSession;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.junit.Assert;
import org.junit.Test;

import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class RoutingMetaStoreProxyRefreshPrivilegesTest {

  @Test
  public void routesToRemoteCatalogAndInternalizesDatabaseName() throws Throwable {
    AtomicInteger defaultCalls = new AtomicInteger();
    AtomicInteger hdpCalls = new AtomicInteger();
    AtomicReference<HiveObjectRef> capturedObj = new AtomicReference<>();
    AtomicReference<GrantRevokePrivilegeRequest> capturedReq = new AtomicReference<>();

    ProxyConfig config = multiCatalogConfig(CatalogAccessMode.READ_WRITE, CatalogAccessMode.READ_WRITE);
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          defaultCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));
    CatalogBackend hdpBackend = newBackend(
        config,
        config.catalogs().get("hdp"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          hdpCalls.incrementAndGet();
          capturedObj.set((HiveObjectRef) args[0]);
          capturedReq.set((GrantRevokePrivilegeRequest) args[2]);
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));

    CatalogRouter router = new CatalogRouter(
        config,
        new LinkedHashMap<>(Map.of("catalog1", defaultBackend, "hdp", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null);

    HiveObjectRef objRef = new HiveObjectRef();
    objRef.setObjectType(HiveObjectType.DATABASE);
    objRef.setDbName("hdp__1cdpre");

    GrantRevokePrivilegeRequest request = new GrantRevokePrivilegeRequest();
    PrivilegeBag privileges = new PrivilegeBag();
    HiveObjectPrivilege priv = new HiveObjectPrivilege();
    HiveObjectRef privRef = new HiveObjectRef();
    privRef.setObjectType(HiveObjectType.DATABASE);
    privRef.setDbName("hdp__1cdpre");
    priv.setHiveObject(privRef);
    priv.setGrantInfo(new PrivilegeGrantInfo("SELECT", 0, "admin", null, true));
    privileges.addToPrivileges(priv);
    request.setPrivileges(privileges);

    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "refresh_privileges",
        HiveObjectRef.class,
        String.class,
        GrantRevokePrivilegeRequest.class);

    Object result = handler.invoke(null, method, new Object[] {objRef, "RangerHiveAuthorizer", request});

    Assert.assertTrue(result instanceof GrantRevokePrivilegeResponse);
    Assert.assertTrue(((GrantRevokePrivilegeResponse) result).isSuccess());
    Assert.assertEquals(0, defaultCalls.get());
    Assert.assertEquals(1, hdpCalls.get());
    Assert.assertEquals("1cdpre", capturedObj.get().getDbName());
    Assert.assertEquals("1cdpre", capturedReq.get().getPrivileges().getPrivileges().get(0).getHiveObject().getDbName());
  }

  @Test
  public void routesToDefaultCatalogWhenDatabaseNameOmitted() throws Throwable {
    AtomicInteger defaultCalls = new AtomicInteger();
    AtomicInteger hdpCalls = new AtomicInteger();

    ProxyConfig config = multiCatalogConfig(CatalogAccessMode.READ_WRITE, CatalogAccessMode.READ_WRITE);
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          defaultCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));
    CatalogBackend hdpBackend = newBackend(
        config,
        config.catalogs().get("hdp"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          hdpCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));

    CatalogRouter router = new CatalogRouter(
        config,
        new LinkedHashMap<>(Map.of("catalog1", defaultBackend, "hdp", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null);

    HiveObjectRef objRef = new HiveObjectRef();
    objRef.setObjectType(HiveObjectType.GLOBAL);

    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "refresh_privileges",
        HiveObjectRef.class,
        String.class,
        GrantRevokePrivilegeRequest.class);

    Object result = handler.invoke(
        null, method, new Object[] {objRef, "RangerHiveAuthorizer", new GrantRevokePrivilegeRequest()});

    Assert.assertTrue(result instanceof GrantRevokePrivilegeResponse);
    Assert.assertTrue(((GrantRevokePrivilegeResponse) result).isSuccess());
    Assert.assertEquals(1, defaultCalls.get());
    Assert.assertEquals(0, hdpCalls.get());
  }

  @Test
  public void returnsSuccessWithoutBackendCallWhenTargetCatalogIsReadOnly() throws Throwable {
    AtomicInteger defaultCalls = new AtomicInteger();
    AtomicInteger hdpCalls = new AtomicInteger();

    ProxyConfig config = multiCatalogConfig(CatalogAccessMode.READ_WRITE, CatalogAccessMode.READ_ONLY);
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          defaultCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));
    CatalogBackend hdpBackend = newBackend(
        config,
        config.catalogs().get("hdp"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          hdpCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));

    CatalogRouter router = new CatalogRouter(
        config,
        new LinkedHashMap<>(Map.of("catalog1", defaultBackend, "hdp", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null);

    HiveObjectRef objRef = new HiveObjectRef();
    objRef.setObjectType(HiveObjectType.DATABASE);
    objRef.setDbName("hdp__1cdpre");

    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "refresh_privileges",
        HiveObjectRef.class,
        String.class,
        GrantRevokePrivilegeRequest.class);

    Object result = handler.invoke(
        null, method, new Object[] {objRef, "RangerHiveAuthorizer", new GrantRevokePrivilegeRequest()});

    Assert.assertTrue(result instanceof GrantRevokePrivilegeResponse);
    Assert.assertTrue(((GrantRevokePrivilegeResponse) result).isSuccess());
    Assert.assertEquals(0, defaultCalls.get());
    Assert.assertEquals(0, hdpCalls.get());
  }

  @Test
  public void returnsSuccessFallbackWhenBackendDoesNotSupportMethod() throws Throwable {
    ProxyConfig config = multiCatalogConfig(CatalogAccessMode.READ_WRITE, CatalogAccessMode.READ_WRITE);
    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          throw new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: 'refresh_privileges'");
        })));
    CatalogBackend hdpBackend = newBackend(
        config,
        config.catalogs().get("hdp"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          throw new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: 'refresh_privileges'");
        })));

    CatalogRouter router = new CatalogRouter(
        config,
        new LinkedHashMap<>(Map.of("catalog1", defaultBackend, "hdp", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null);

    HiveObjectRef objRef = new HiveObjectRef();
    objRef.setObjectType(HiveObjectType.DATABASE);
    objRef.setDbName("hdp__1cdpre");

    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "refresh_privileges",
        HiveObjectRef.class,
        String.class,
        GrantRevokePrivilegeRequest.class);

    Object result = handler.invoke(
        null, method, new Object[] {objRef, "RangerHiveAuthorizer", new GrantRevokePrivilegeRequest()});

    Assert.assertTrue(result instanceof GrantRevokePrivilegeResponse);
    Assert.assertTrue(((GrantRevokePrivilegeResponse) result).isSuccess());
  }

  @Test
  public void returnsSuccessWithoutBackendCallWhenGloballyEnabledViaConfig() throws Throwable {
    AtomicInteger defaultCalls = new AtomicInteger();
    AtomicInteger hdpCalls = new AtomicInteger();

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            new CatalogConfig(
                "catalog1", "c1", "file:///c1", false, CatalogAccessMode.READ_WRITE, List.of(), null, null, Map.of("hive.metastore.uris", "thrift://default:9083")),
            "hdp",
            new CatalogConfig(
                "hdp", "hdp", "file:///hdp", false, CatalogAccessMode.READ_WRITE, List.of(), null, null, Map.of("hive.metastore.uris", "thrift://hdp:9083"))))
        .compatibility(new CompatibilityConfig(false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .latencyRouting(new io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig(
            null, null, null, null, null, null, null, true))
        .build();

    CatalogBackend defaultBackend = newBackend(
        config,
        config.catalogs().get("catalog1"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          defaultCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));
    CatalogBackend hdpBackend = newBackend(
        config,
        config.catalogs().get("hdp"),
        new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("hdp"), newSession((proxy, method, args) -> {
          hdpCalls.incrementAndGet();
          GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
          response.setSuccess(true);
          return response;
        })));

    CatalogRouter router = new CatalogRouter(
        config,
        new LinkedHashMap<>(Map.of("catalog1", defaultBackend, "hdp", hdpBackend)));
    RoutingMetaStoreProxy handler = new RoutingMetaStoreProxy(
        config, router, new FederationLayer(config, router), null);

    HiveObjectRef objRef = new HiveObjectRef();
    objRef.setObjectType(HiveObjectType.DATABASE);
    objRef.setDbName("hdp__1cdpre");

    Method method = ThriftHiveMetastore.Iface.class.getMethod(
        "refresh_privileges",
        HiveObjectRef.class,
        String.class,
        GrantRevokePrivilegeRequest.class);

    Object result = handler.invoke(
        null, method, new Object[] {objRef, "RangerHiveAuthorizer", new GrantRevokePrivilegeRequest()});

    Assert.assertTrue(result instanceof GrantRevokePrivilegeResponse);
    Assert.assertTrue(((GrantRevokePrivilegeResponse) result).isSuccess());
    Assert.assertEquals(0, defaultCalls.get());
    Assert.assertEquals(0, hdpCalls.get());
  }

  @Test
  public void parsesLatencyRoutingPropertiesCorrectly() {
    java.util.Properties props = new java.util.Properties();
    props.setProperty("routing.refresh-privileges.synthetic-success", "true");
    props.setProperty("routing.database-list-cache.ttl-seconds", "30");
    props.setProperty("routing.database-metadata-cache.ttl-seconds", "45");

    io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig parsed =
        io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfigParser.parse(
            new io.github.mmalykhin.hmsproxy.config.PropertyReader(props), 2);

    Assert.assertTrue(parsed.refreshPrivilegesSyntheticSuccess());
    Assert.assertEquals(30000L, parsed.databaseListCache().ttlMs());
    Assert.assertEquals(45000L, parsed.databaseMetadataCache().ttlMs());

    props.clear();
    props.setProperty("routing.refresh-privileges.mode", "SYNTHETIC_SUCCESS");
    parsed = io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfigParser.parse(
        new io.github.mmalykhin.hmsproxy.config.PropertyReader(props), 2);
    Assert.assertTrue(parsed.refreshPrivilegesSyntheticSuccess());
  }

  private static ProxyConfig multiCatalogConfig(
      CatalogAccessMode defaultAccessMode,
      CatalogAccessMode hdpAccessMode
  ) {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of(
            "catalog1",
            new CatalogConfig(
                "catalog1", "c1", "file:///c1", false, defaultAccessMode, List.of(), null, null, Map.of("hive.metastore.uris", "thrift://default:9083")),
            "hdp",
            new CatalogConfig(
                "hdp", "hdp", "file:///hdp", false, hdpAccessMode, List.of(), null, null, Map.of("hive.metastore.uris", "thrift://hdp:9083"))))
        .compatibility(new CompatibilityConfig(false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }
}
