package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableDropPurgeMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ExternalTableLocationRewriteMode;
import io.github.mmalykhin.hmsproxy.config.catalog.ViewTextRewriteMode;
import io.github.mmalykhin.hmsproxy.config.compatibility.CompatibilityConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardConfig;
import io.github.mmalykhin.hmsproxy.config.ddlguard.TransactionalDdlGuardMode;
import io.github.mmalykhin.hmsproxy.config.federation.FederationConfig;
import io.github.mmalykhin.hmsproxy.config.management.ManagementConfig;
import io.github.mmalykhin.hmsproxy.config.ratelimit.RateLimitConfig;
import io.github.mmalykhin.hmsproxy.config.routing.AdaptiveTimeoutConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendConfig;
import io.github.mmalykhin.hmsproxy.config.routing.BackendStatePollingConfig;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.routing.HedgedReadConfig;
import io.github.mmalykhin.hmsproxy.config.routing.IcebergPointerGuardConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.RangerConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.FrontendProfile;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.security.ranger.MetadataAuthorizer;
import io.github.mmalykhin.hmsproxy.security.ranger.RangerMetadataAuthorizer;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Assert;
import org.junit.Test;
import static io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxyTestSupport.*;

public class RoutingMetaStoreProxyRangerTest {

  @Test
  public void testSharedCacheFilteredByRanger() throws Throwable {
    CatalogRangerConfig cat1Ranger = new CatalogRangerConfig(
        true, null, "c1_hive_svc", "hive", "hms-proxy", null, 30000L, 5000, 5000, null, null, null, false);
    RangerConfig ranger = new RangerConfig(
        true, cat1Ranger, Map.of("catalog1", cat1Ranger));

    CatalogConfig catalog1Config = new CatalogConfig(
        "catalog1", "c1", null, true, CatalogAccessMode.READ_WRITE, List.of(),
        CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), MetastoreRuntimeProfile.APACHE_3_1_3, null,
        Map.of("hive.metastore.uris", "thrift://one"), 5000L, 10, 60000L, 10, 10, 60000L, cat1Ranger);

    ProxyConfig config = new ProxyConfig(
        new ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new SecurityConfig(SecurityMode.NONE, null, null, null, null, true, Map.of()),
        ".",
        "catalog1",
        Map.of("catalog1", catalog1Config),
        new BackendConfig(Map.of()),
        new CompatibilityConfig(FrontendProfile.APACHE_3_1_3, null, null, false),
        new FederationConfig(false, ViewTextRewriteMode.DISABLED, false),
        new TransactionalDdlGuardConfig(TransactionalDdlGuardMode.DISABLED, List.of()),
        new ManagementConfig(false, "127.0.0.1", 10083),
        null,
        SyntheticReadLockStoreConfig.inMemory(),
        RateLimitConfig.disabled(),
        new LatencyRoutingConfig(
            new BackendStatePollingConfig(false, 10_000, 5_000L),
            new AdaptiveTimeoutConfig(false, 2_000L, 1_000L, 10_000L, 4.0d, 0.2d),
            new CircuitBreakerConfig(false, 1, 200L),
            new HedgedReadConfig(false, 1, 30_000L),
            DegradedRoutingPolicy.STRICT,
            new DatabaseListCacheConfig(60_000L, 100, true),
            new DatabaseMetadataCacheConfig(60_000L, 100, true)),
        IcebergPointerGuardConfig.defaults(),
        List.of(),
        ranger
    );

    AtomicInteger getAllDatabasesCalls = new AtomicInteger();
    AtomicInteger getDatabaseCalls = new AtomicInteger();
    AtomicInteger getAllTablesCalls = new AtomicInteger();

    CatalogBackend backend1 = newBackend(
        config,
        catalog1Config,
        new ApacheBackendAdapter(),
        newBackendRuntime(
            config,
            catalog1Config,
            newSession((proxy, method, args) -> {
              if ("get_all_databases".equals(method.getName())) {
                getAllDatabasesCalls.incrementAndGet();
                return List.of("sales", "finance", "hr");
              }
              if ("get_database".equals(method.getName())) {
                getDatabaseCalls.incrementAndGet();
                String dbName = (String) args[0];
                Database db = new Database();
                db.setName(dbName);
                db.setDescription(dbName + " description");
                return db;
              }
              if ("get_all_tables".equals(method.getName())) {
                getAllTablesCalls.incrementAndGet();
                return List.of("orders", "customers", "secret_reports");
              }
              throw new UnsupportedOperationException(method.getName());
            })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    ProxyObservability observability = new ProxyObservability(config);
    CatalogRouter router = new CatalogRouter(config, backends);

    ServicePolicies servicePolicies = buildServicePolicies("c1_hive_svc");
    MetadataAuthorizer customAuthorizer = new RangerMetadataAuthorizer(ranger, config.catalogs()) {
      @Override
      protected RangerBasePlugin createPlugin(String catalogName, CatalogRangerConfig config) {
        RangerBasePlugin plugin = super.createPlugin(catalogName, config);
        plugin.setPolicies(servicePolicies);
        return plugin;
      }
    };

    FederationLayer federationLayer = new FederationLayer(config, router);
    DatabaseListCache listCache = new DatabaseListCache(config.latencyRouting().databaseListCache());
    DatabaseMetadataCache metaCache = new DatabaseMetadataCache(config.latencyRouting().databaseMetadataCache(), listCache);
    RequestRateLimiter rateLimiter = new RequestRateLimiter(config, observability.metrics());
    AdmissionGate admissionGate = new AdmissionGate(new BackendRoutingController(config, router, observability), rateLimiter);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(
        new io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer(config, null),
        admissionGate,
        observability,
        new FanoutExecutor(new BackendRoutingController(config, router, observability), router, admissionGate));

    RoutingHandler routingHandler = new RoutingHandler(
        config,
        router,
        federationLayer,
        new io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer(config, null),
        observability,
        dispatcher,
        new ImpersonationResolver(config),
        listCache,
        metaCache,
        null,
        customAuthorizer);

    Method getAllDbsMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    Method getDbMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);
    Method getAllTablesMethod = ThriftHiveMetastore.Iface.class.getMethod("get_all_tables", String.class);

    org.apache.hadoop.security.UserGroupInformation aliceUgi =
        org.apache.hadoop.security.UserGroupInformation.createRemoteUser("alice");
    org.apache.hadoop.security.UserGroupInformation bobUgi =
        org.apache.hadoop.security.UserGroupInformation.createRemoteUser("bob");

    // 1. Alice queries get_all_databases
    @SuppressWarnings("unchecked")
    List<String> aliceDbs = (List<String>) invokeAs(aliceUgi, routingHandler, getAllDbsMethod);
    Assert.assertEquals(1, getAllDatabasesCalls.get());
    Assert.assertEquals(List.of("sales"), aliceDbs);

    // 2. Bob queries get_all_databases -> shared cache hit! getAllDatabasesCalls remains 1
    @SuppressWarnings("unchecked")
    List<String> bobDbs = (List<String>) invokeAs(bobUgi, routingHandler, getAllDbsMethod);
    Assert.assertEquals(1, getAllDatabasesCalls.get());
    Assert.assertEquals(List.of("finance"), bobDbs);

    // 3. Alice queries get_database("sales") -> 1 backend call
    Database aliceSales = (Database) invokeAs(aliceUgi, routingHandler, getDbMethod, "sales");
    Assert.assertEquals("sales", aliceSales.getName());
    Assert.assertEquals(1, getDatabaseCalls.get());

    // 4. Bob queries get_database("sales") -> rejected by Ranger with NoSuchObjectException, no extra backend call
    try {
      invokeAs(bobUgi, routingHandler, getDbMethod, "sales");
      Assert.fail("Bob should not have access to 'sales'");
    } catch (NoSuchObjectException expected) {
      Assert.assertTrue(expected.getMessage().contains("sales"));
    }
    Assert.assertEquals(1, getDatabaseCalls.get());

    // 5. Bob queries get_database("finance") -> loads finance, 2nd backend call
    Database bobFinance = (Database) invokeAs(bobUgi, routingHandler, getDbMethod, "finance");
    Assert.assertEquals("finance", bobFinance.getName());
    Assert.assertEquals(2, getDatabaseCalls.get());

    // 6. Alice queries get_all_tables("sales") -> filters to only allowed tables
    @SuppressWarnings("unchecked")
    List<String> aliceTables = (List<String>) invokeAs(aliceUgi, routingHandler, getAllTablesMethod, "sales");
    Assert.assertEquals(1, getAllTablesCalls.get());
    // Alice policy allows orders and customers, but not secret_reports
    Assert.assertEquals(List.of("orders", "customers"), aliceTables);

    customAuthorizer.close();
  }

  private static Object invokeAs(
      org.apache.hadoop.security.UserGroupInformation ugi,
      RoutingHandler routingHandler,
      Method method,
      Object... args
  ) throws Throwable {
    try {
      return ugi.doAs((java.security.PrivilegedExceptionAction<Object>) () -> {
        try {
          return routingHandler.invoke(null, method, args);
        } catch (Throwable t) {
          if (t instanceof Exception e) {
            throw e;
          }
          throw new RuntimeException(t);
        }
      });
    } catch (java.lang.reflect.UndeclaredThrowableException ute) {
      throw ute.getUndeclaredThrowable();
    }
  }

  private ServicePolicies buildServicePolicies(String serviceName) {
    ServicePolicies sp = new ServicePolicies();
    sp.setServiceName(serviceName);
    sp.setServiceId(1L);

    RangerServiceDef sd = new RangerServiceDef();
    sd.setName("hive");
    sd.setId(1L);

    RangerServiceDef.RangerResourceDef dbRes = new RangerServiceDef.RangerResourceDef();
    dbRes.setItemId(1L);
    dbRes.setName("database");
    dbRes.setLevel(10);

    RangerServiceDef.RangerResourceDef tblRes = new RangerServiceDef.RangerResourceDef();
    tblRes.setItemId(2L);
    tblRes.setName("table");
    tblRes.setLevel(20);
    tblRes.setParent("database");

    sd.setResources(List.of(dbRes, tblRes));

    RangerServiceDef.RangerAccessTypeDef selectAcc = new RangerServiceDef.RangerAccessTypeDef(1L, "select", "select", null, null);
    RangerServiceDef.RangerAccessTypeDef readAcc = new RangerServiceDef.RangerAccessTypeDef(2L, "read", "read", null, null);
    RangerServiceDef.RangerAccessTypeDef useAcc = new RangerServiceDef.RangerAccessTypeDef(3L, "use", "use", null, null);

    sd.setAccessTypes(List.of(selectAcc, readAcc, useAcc));
    sp.setServiceDef(sd);

    // Policy 1: alice -> database: sales, table: orders, customers
    RangerPolicy p1 = new RangerPolicy();
    p1.setId(1L);
    p1.setService(serviceName);
    p1.setName("sales_policy");
    p1.setResources(Map.of(
        "database", new RangerPolicy.RangerPolicyResource("sales"),
        "table", new RangerPolicy.RangerPolicyResource(List.of("orders", "customers"), false, false)
    ));
    RangerPolicy.RangerPolicyItem item1 = new RangerPolicy.RangerPolicyItem();
    item1.setUsers(List.of("alice"));
    item1.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    p1.setPolicyItems(List.of(item1));

    // Policy 2: bob -> database: finance, table: *
    RangerPolicy p2 = new RangerPolicy();
    p2.setId(2L);
    p2.setService(serviceName);
    p2.setName("finance_policy");
    p2.setResources(Map.of(
        "database", new RangerPolicy.RangerPolicyResource("finance"),
        "table", new RangerPolicy.RangerPolicyResource("*")
    ));
    RangerPolicy.RangerPolicyItem item2 = new RangerPolicy.RangerPolicyItem();
    item2.setUsers(List.of("bob"));
    item2.setAccesses(List.of(new RangerPolicy.RangerPolicyItemAccess("select", true)));
    p2.setPolicyItems(List.of(item2));

    sp.setPolicies(List.of(p1, p2));
    return sp;
  }
}
