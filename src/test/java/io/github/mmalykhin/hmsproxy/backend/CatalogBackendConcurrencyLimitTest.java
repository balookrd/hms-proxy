package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogStartupMode;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CatalogBackendConcurrencyLimitTest {

  private ExecutorService executor;

  @Before
  public void setUp() {
    executor = Executors.newFixedThreadPool(4);
  }

  @After
  public void tearDown() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  private Future<Object> submitCall(CatalogBackend backend, String methodName) {
    return executor.submit(() -> {
      try {
        return backend.invokeRawByName(methodName, new Class<?>[0], new Object[0], null);
      } catch (Throwable t) {
        if (t instanceof Exception) {
          throw (Exception) t;
        }
        throw new RuntimeException(t);
      }
    });
  }

  @Test
  public void concurrencyLimitBlocksAndTimesOutWhenExceeded() throws Throwable {
    CountDownLatch holdLatch = new CountDownLatch(1);
    CountDownLatch firstCallEntered = new CountDownLatch(1);

    CatalogBackend backend = createBackend(1, 100, (proxy, method, args) -> {
      if ("getStatus".equals(method.getName())) {
        firstCallEntered.countDown();
        holdLatch.await(5, TimeUnit.SECONDS);
        return com.facebook.fb303.fb_status.ALIVE;
      }
      return null;
    });

    Future<Object> firstCall = submitCall(backend, "getStatus");

    Assert.assertTrue("First call must enter backend invocation", firstCallEntered.await(2, TimeUnit.SECONDS));

    // Second call should attempt to acquire permit, wait ~100ms, and fail with MetaException
    Future<Object> secondCall = submitCall(backend, "getStatus");

    try {
      secondCall.get(2, TimeUnit.SECONDS);
      Assert.fail("Second call must fail due to concurrency limit timeout");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Assert.assertTrue("Cause must be MetaException, got: " + cause, cause instanceof MetaException);
      Assert.assertTrue(
          "Message must contain concurrency limit details, got: " + cause.getMessage(),
          cause.getMessage().contains("exceeded max concurrent calls limit of 1")
      );
    }

    // Release first call
    holdLatch.countDown();
    Object firstResult = firstCall.get(2, TimeUnit.SECONDS);
    Assert.assertEquals(com.facebook.fb303.fb_status.ALIVE, firstResult);
  }

  @Test
  public void queuedCallSucceedsWhenPermitBecomesAvailableWithinTimeout() throws Throwable {
    CountDownLatch holdLatch = new CountDownLatch(1);
    CountDownLatch firstCallEntered = new CountDownLatch(1);

    CatalogBackend backend = createBackend(1, 1000, (proxy, method, args) -> {
      if ("getStatus".equals(method.getName())) {
        if (firstCallEntered.getCount() > 0) {
          firstCallEntered.countDown();
          holdLatch.await(2, TimeUnit.SECONDS);
          return com.facebook.fb303.fb_status.ALIVE;
        }
        return com.facebook.fb303.fb_status.STOPPED;
      }
      return null;
    });

    Future<Object> firstCall = submitCall(backend, "getStatus");

    Assert.assertTrue(firstCallEntered.await(2, TimeUnit.SECONDS));

    Future<Object> secondCall = submitCall(backend, "getStatus");

    // Give second call a moment to queue up in semaphore
    Thread.sleep(50);
    // Release first call permit
    holdLatch.countDown();

    Assert.assertEquals(com.facebook.fb303.fb_status.ALIVE, firstCall.get(2, TimeUnit.SECONDS));
    Assert.assertEquals(com.facebook.fb303.fb_status.STOPPED, secondCall.get(2, TimeUnit.SECONDS));
  }

  @Test
  public void permitIsReleasedEvenWhenBackendCallThrowsException() throws Throwable {
    AtomicBoolean throwException = new AtomicBoolean(true);

    CatalogBackend backend = createBackend(1, 200, (proxy, method, args) -> {
      if ("getStatus".equals(method.getName())) {
        if (throwException.get()) {
          throw new RuntimeException("backend simulated failure");
        }
        return com.facebook.fb303.fb_status.ALIVE;
      }
      return null;
    });

    try {
      backend.invokeRawByName("getStatus", new Class<?>[0], new Object[0], null);
      Assert.fail("Expected exception on first call");
    } catch (Throwable expected) {
      // Expected
    }

    // Since permit was released in finally block, next call should immediately succeed
    throwException.set(false);
    Object result = backend.invokeRawByName("getStatus", new Class<?>[0], new Object[0], null);
    Assert.assertEquals(com.facebook.fb303.fb_status.ALIVE, result);
  }

  private CatalogBackend createBackend(
      int maxConcurrentCalls,
      long concurrencyTimeoutMs,
      java.lang.reflect.InvocationHandler thriftHandler
  ) throws Exception {
    CatalogConfig config = new CatalogConfig(
        "remote_wan_catalog",
        "remote wan catalog",
        "thrift://remote-wan-hms:9083",
        false,
        io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE,
        Collections.emptyList(),
        io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL,
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
        maxConcurrentCalls,
        concurrencyTimeoutMs,
        null,
        false
    );

    ProxyConfig proxyConfig = ProxyConfig.builder()
        .server(new io.github.mmalykhin.hmsproxy.config.server.ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new io.github.mmalykhin.hmsproxy.config.security.SecurityConfig(
            io.github.mmalykhin.hmsproxy.config.security.SecurityMode.NONE, null, null, null, null, false, java.util.Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog(config.name())
        .catalogs(java.util.Map.of(config.name(), config))
        .syntheticReadLockStore(io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig.inMemory())
        .build();

    ThriftHiveMetastore.Iface thriftClient = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        thriftHandler
    );

    Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class
    );
    ctor.setAccessible(true);
    BackendInvocationSession session = ctor.newInstance(null, thriftClient, null);

    BackendRuntime.SessionFactory factory = new BackendRuntime.SessionFactory() {
      @Override
      public boolean requiresIsolatedClassLoader(MetastoreRuntimeProfile runtimeProfile) {
        return false;
      }

      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile
      ) {
        return session;
      }

      @Override
      public BackendInvocationSession open(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile,
          ClassLoader isolatedClassLoader
      ) {
        return session;
      }

      @Override
      public BackendInvocationSession openImpersonating(
          ProxyConfig proxyConfig,
          CatalogConfig catalogConfig,
          HiveConf hiveConf,
          boolean backendKerberosEnabled,
          MetastoreRuntimeProfile runtimeProfile,
          String userName,
          List<String> groupNames
      ) {
        return session;
      }
    };

    HiveConf hiveConf = new HiveConf();
    BackendRuntime runtime = BackendRuntime.open(
        proxyConfig,
        config,
        hiveConf,
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory
    );

    BackendAdapter adapter = new BackendAdapter() {
      @Override
      public Object invoke(
          CatalogBackend backend,
          Method method,
          Object[] args,
          ImpersonationContext impersonation
      ) throws Throwable {
        return backend.invokeRaw(method, args, impersonation);
      }

      @Override
      public Object invokeRequest(
          CatalogBackend backend,
          String methodName,
          Object request,
          ImpersonationContext impersonation
      ) throws Throwable {
        return backend.invokeRawByName(methodName, new Class<?>[] {request.getClass()}, new Object[] {request}, impersonation);
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

    return new CatalogBackend(
        proxyConfig,
        config,
        hiveConf,
        adapter,
        runtime,
        catalog,
        null
    );
  }
}
