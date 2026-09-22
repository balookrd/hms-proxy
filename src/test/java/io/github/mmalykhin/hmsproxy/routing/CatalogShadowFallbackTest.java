package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.BackendAdapter;
import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogStartupMode;
import io.github.mmalykhin.hmsproxy.config.routing.CircuitBreakerConfig;
import io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Test;

public class CatalogShadowFallbackTest {

  @FunctionalInterface
  interface MockCallHandler {
    Object handle(CatalogBackend backend, Method method, Object[] args) throws Throwable;
  }

  @Test
  public void readOnlyCallFallsBackToShadowCatalogOnOutage() throws Throwable {
    AtomicInteger primaryCalls = new AtomicInteger();
    AtomicInteger secondaryCalls = new AtomicInteger();

    CatalogConfig primaryConfig = catalogConfig("primary", "secondary", true);
    CatalogConfig secondaryConfig = catalogConfig("secondary", null, false);
    ProxyConfig proxyConfig = proxyConfig(primaryConfig, secondaryConfig);

    ProxyObservability observability = new ProxyObservability(proxyConfig);

    CatalogBackend primaryBackend = createBackend(proxyConfig, primaryConfig, (backend, method, args) -> {
      primaryCalls.incrementAndGet();
      throw new TTransportException("Connection refused (primary DC WAN link down)");
    });

    CatalogBackend secondaryBackend = createBackend(proxyConfig, secondaryConfig, (backend, method, args) -> {
      secondaryCalls.incrementAndGet();
      Database db = new Database();
      db.setName("analytics_db");
      db.setDescription("Served from shadow replica");
      return db;
    });

    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("primary", primaryBackend);
    backends.put("secondary", secondaryBackend);

    CatalogRouter router = CatalogRouter.createForTest(proxyConfig, backends);

    RequestRateLimiter rateLimiter = new RequestRateLimiter(proxyConfig, observability.metrics());
    AdmissionGate admissionGate = new AdmissionGate(
        new BackendRoutingController(proxyConfig, router, observability), rateLimiter);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(
        new CompatibilityLayer(proxyConfig, null),
        admissionGate,
        observability,
        new FanoutExecutor(new BackendRoutingController(proxyConfig, router, observability), router, admissionGate),
        router
    );

    Method getDatabaseMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    Object result = dispatcher.invokeDirect(
        primaryBackend,
        getDatabaseMethod,
        new Object[] {"analytics_db"},
        null,
        100L,
        false,
        false
    );

    Assert.assertNotNull(result);
    Assert.assertTrue("Result must be Database", result instanceof Database);
    Database db = (Database) result;
    Assert.assertEquals("analytics_db", db.getName());
    Assert.assertEquals("Served from shadow replica", db.getDescription());

    Assert.assertEquals("Primary must have been attempted once", 1, primaryCalls.get());
    Assert.assertEquals("Secondary shadow must have handled the fallback", 1, secondaryCalls.get());

    String metrics = observability.metrics().render();
    Assert.assertTrue("Metric must record fallback: " + metrics,
        metrics.contains("hms_proxy_catalog_fallback_total{primary_catalog=\"primary\",fallback_catalog=\"secondary\",method=\"get_database\"} 1"));
  }

  @Test
  public void mutatingCallDoesNotFallBackToShadowCatalogOnOutage() throws Throwable {
    AtomicInteger primaryCalls = new AtomicInteger();
    AtomicInteger secondaryCalls = new AtomicInteger();

    CatalogConfig primaryConfig = catalogConfig("primary", "secondary", true);
    CatalogConfig secondaryConfig = catalogConfig("secondary", null, false);
    ProxyConfig proxyConfig = proxyConfig(primaryConfig, secondaryConfig);

    ProxyObservability observability = new ProxyObservability(proxyConfig);

    CatalogBackend primaryBackend = createBackend(proxyConfig, primaryConfig, (backend, method, args) -> {
      primaryCalls.incrementAndGet();
      throw new TTransportException("Connection refused");
    });

    CatalogBackend secondaryBackend = createBackend(proxyConfig, secondaryConfig, (backend, method, args) -> {
      secondaryCalls.incrementAndGet();
      return null;
    });

    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("primary", primaryBackend);
    backends.put("secondary", secondaryBackend);

    CatalogRouter router = CatalogRouter.createForTest(proxyConfig, backends);

    RequestRateLimiter rateLimiter = new RequestRateLimiter(proxyConfig, observability.metrics());
    AdmissionGate admissionGate = new AdmissionGate(
        new BackendRoutingController(proxyConfig, router, observability), rateLimiter);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(
        new CompatibilityLayer(proxyConfig, null),
        admissionGate,
        observability,
        new FanoutExecutor(new BackendRoutingController(proxyConfig, router, observability), router, admissionGate),
        router
    );

    Method dropDatabaseMethod = ThriftHiveMetastore.Iface.class.getMethod(
        "drop_database", String.class, boolean.class, boolean.class);

    try {
      dispatcher.invokeDirect(
          primaryBackend,
          dropDatabaseMethod,
          new Object[] {"analytics_db", false, false},
          null,
          101L,
          false,
          false
      );
      Assert.fail("Mutating operation must not fall back to shadow and must throw exception");
    } catch (MetaException expected) {
      // Expected: drop_database failure is normalized to MetaException
    }

    Assert.assertEquals(1, primaryCalls.get());
    Assert.assertEquals("Mutating method must NEVER be routed to shadow catalog", 0, secondaryCalls.get());

    String metrics = observability.metrics().render();
    Assert.assertFalse("Metric must not contain shadow fallback for drop_database",
        metrics.contains("method=\"drop_database\""));
  }

  @Test
  public void readOnlyCallDoesNotFallBackWhenFallbackOnOutageIsDisabled() throws Throwable {
    AtomicInteger primaryCalls = new AtomicInteger();
    AtomicInteger secondaryCalls = new AtomicInteger();

    // fallbackOnOutage = false
    CatalogConfig primaryConfig = catalogConfig("primary", "secondary", false);
    CatalogConfig secondaryConfig = catalogConfig("secondary", null, false);
    ProxyConfig proxyConfig = proxyConfig(primaryConfig, secondaryConfig);

    ProxyObservability observability = new ProxyObservability(proxyConfig);

    CatalogBackend primaryBackend = createBackend(proxyConfig, primaryConfig, (backend, method, args) -> {
      primaryCalls.incrementAndGet();
      throw new TTransportException("Connection refused");
    });

    CatalogBackend secondaryBackend = createBackend(proxyConfig, secondaryConfig, (backend, method, args) -> {
      secondaryCalls.incrementAndGet();
      return new Database("analytics_db", "", "", Collections.emptyMap());
    });

    Map<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("primary", primaryBackend);
    backends.put("secondary", secondaryBackend);

    CatalogRouter router = CatalogRouter.createForTest(proxyConfig, backends);

    RequestRateLimiter rateLimiter = new RequestRateLimiter(proxyConfig, observability.metrics());
    AdmissionGate admissionGate = new AdmissionGate(
        new BackendRoutingController(proxyConfig, router, observability), rateLimiter);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(
        new CompatibilityLayer(proxyConfig, null),
        admissionGate,
        observability,
        new FanoutExecutor(new BackendRoutingController(proxyConfig, router, observability), router, admissionGate),
        router
    );

    Method getDatabaseMethod = ThriftHiveMetastore.Iface.class.getMethod("get_database", String.class);

    try {
      dispatcher.invokeDirect(
          primaryBackend,
          getDatabaseMethod,
          new Object[] {"analytics_db"},
          null,
          102L,
          false,
          false
      );
      Assert.fail("Expected exception when fallback-on-outage is disabled");
    } catch (MetaException expected) {
      // Expected
    }

    Assert.assertEquals(1, primaryCalls.get());
    Assert.assertEquals(0, secondaryCalls.get());
  }

  private static CatalogConfig catalogConfig(String name, String fallbackCatalog, boolean fallbackOnOutage) {
    return new CatalogConfig(
        name,
        name + " desc",
        "thrift://" + name + ":9083",
        false,
        CatalogAccessMode.READ_WRITE,
        Collections.emptyList(),
        CatalogExposureMode.ALLOW_ALL,
        Collections.emptyList(),
        Collections.emptyMap(),
        MetastoreRuntimeProfile.APACHE_3_1_3,
        null,
        Collections.emptyMap(),
        0L,
        128,
        0L,
        1,
        4,
        0L,
        null,
        CatalogStartupMode.STRICT,
        true,
        0,
        10000L,
        fallbackCatalog,
        fallbackOnOutage
    );
  }

  private static ProxyConfig proxyConfig(CatalogConfig primary, CatalogConfig secondary) {
    Map<String, CatalogConfig> catalogs = new LinkedHashMap<>();
    catalogs.put(primary.name(), primary);
    catalogs.put(secondary.name(), secondary);

    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog(primary.name())
        .catalogs(catalogs)
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static CatalogBackend createBackend(
      ProxyConfig proxyConfig,
      CatalogConfig config,
      MockCallHandler handler
  ) {
    BackendAdapter adapter = new BackendAdapter() {
      @Override
      public Object invoke(
          CatalogBackend backend,
          Method method,
          Object[] args,
          ImpersonationContext impersonation
      ) throws Throwable {
        return handler.handle(backend, method, args);
      }

      @Override
      public Object invokeRequest(
          CatalogBackend backend,
          String methodName,
          Object request,
          ImpersonationContext impersonation
      ) throws Throwable {
        Method method = ThriftHiveMetastore.Iface.class.getMethod(methodName, request.getClass());
        return handler.handle(backend, method, new Object[] {request});
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
        return "3.1.3";
      }
    };

    Catalog catalog = new Catalog();
    catalog.setName(config.name());

    return CatalogBackend.createForTest(
        proxyConfig,
        config,
        new HiveConf(),
        adapter,
        null,
        catalog,
        null
    );
  }
}
