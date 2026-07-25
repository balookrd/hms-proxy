package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assume;
import org.junit.Assert;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;
import io.github.mmalykhin.hmsproxy.config.security.SecurityMode;
import io.github.mmalykhin.hmsproxy.config.server.ServerConfig;
import io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig;

public class BackendRuntimeTest {
  @Test
  public void openOpensSessionWithConfiguredProfile() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();

    BackendRuntime.open(
        config(),
        catalogConfig(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, "/tmp/hdp.jar"),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        factory);

    Assert.assertEquals(List.of(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78), factory.openProfiles);
  }

  @Test
  public void reconnectOpensNewSessionWithSameProfile() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();

    BackendRuntime runtime = BackendRuntime.open(
        config(),
        catalogConfig(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, "/tmp/hdp.jar"),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        factory);
    HortonworksBackendAdapter adapter =
        new HortonworksBackendAdapter(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78);

    runtime.reconnectShared(adapter);

    Assert.assertEquals(
        List.of(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78),
        factory.openProfiles);
  }

  @Test
  public void reconnectApacheOpensNewApacheSession() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();

    BackendRuntime runtime = BackendRuntime.open(
        config(),
        catalogConfig(MetastoreRuntimeProfile.APACHE_3_1_3, null),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory);
    ApacheBackendAdapter adapter = new ApacheBackendAdapter();

    runtime.reconnectShared(adapter);

    Assert.assertEquals(
        List.of(MetastoreRuntimeProfile.APACHE_3_1_3, MetastoreRuntimeProfile.APACHE_3_1_3),
        factory.openProfiles);
  }

  @Test
  public void hortonworksRuntimeReusesIsolatedClassLoaderAcrossReconnects() throws Exception {
    Path hdpJar = Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar")
        .toAbsolutePath();
    Assume.assumeTrue(Files.isReadable(hdpJar));
    RecordingSessionFactory factory = new RecordingSessionFactory(true);
    CatalogConfig catalogConfig = catalogConfig(
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        hdpJar.toString());

    BackendRuntime runtime = BackendRuntime.open(
        config(),
        catalogConfig,
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        factory);
    try {
      HortonworksBackendAdapter adapter =
          new HortonworksBackendAdapter(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78);

      runtime.reconnectShared(adapter);
    } finally {
      runtime.close();
    }

    Assert.assertEquals(2, factory.isolatedClassLoaders.size());
    Assert.assertNotNull(factory.isolatedClassLoaders.get(0));
    Assert.assertSame(factory.isolatedClassLoaders.get(0), factory.isolatedClassLoaders.get(1));
  }

  @Test
  public void invokeAfterCloseFailsWithoutOpeningNewSession() throws Throwable {
    RecordingSessionFactory factory = new RecordingSessionFactory();
    BackendRuntime runtime = BackendRuntime.open(
        config(),
        catalogConfig(MetastoreRuntimeProfile.APACHE_3_1_3, null),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory);

    runtime.close();

    try {
      runtime.invokeSharedByName("getStatus", new Class<?>[0], new Object[0]);
      Assert.fail("expected invocation on a closed runtime to fail");
    } catch (MetaException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
    }
    Assert.assertEquals(List.of(MetastoreRuntimeProfile.APACHE_3_1_3), factory.openProfiles);
  }

  @Test
  public void sessionOpenersAfterCloseFailWithoutOpeningNewSession() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();
    BackendRuntime runtime = BackendRuntime.open(
        config(),
        catalogConfig(MetastoreRuntimeProfile.APACHE_3_1_3, null),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory);

    runtime.close();

    try {
      runtime.reconnectShared(new ApacheBackendAdapter());
      Assert.fail("expected reconnect on a closed runtime to fail");
    } catch (MetaException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
    }
    try {
      runtime.openImpersonationSession(MetastoreRuntimeProfile.APACHE_3_1_3, "alice", List.of("g1"));
      Assert.fail("expected impersonation session open on a closed runtime to fail");
    } catch (MetaException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
    }
    try {
      runtime.openEphemeralSession(new HiveConf(), MetastoreRuntimeProfile.APACHE_3_1_3);
      Assert.fail("expected ephemeral session open on a closed runtime to fail");
    } catch (MetaException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
    }
    Assert.assertEquals(List.of(MetastoreRuntimeProfile.APACHE_3_1_3), factory.openProfiles);
  }

  @Test
  public void closeTimeoutClosesInFlightSessionInsteadOfPoolingIt() throws Throwable {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    CloseRecorder recorder = new CloseRecorder();
    ControlledSessionFactory factory = new ControlledSessionFactory(
        () -> isolatedSession(recorder, blockingThriftClient(entered, finish)));
    BackendRuntime runtime = BackendRuntime.open(
        config(),
        pooledCatalogConfig(1, 200L, null),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.APACHE_3_1_3,
        factory);

    AtomicReference<Throwable> callerFailure = new AtomicReference<>();
    Thread caller = new Thread(() -> {
      try {
        runtime.invokeSharedByName("getStatus", new Class<?>[0], new Object[0]);
      } catch (Throwable t) {
        callerFailure.set(t);
      }
    }, "in-flight-caller");
    caller.start();
    Assert.assertTrue("caller did not reach the backend call", entered.await(10, TimeUnit.SECONDS));

    runtime.close();

    finish.countDown();
    caller.join(TimeUnit.SECONDS.toMillis(10));
    Assert.assertNull(callerFailure.get());
    Assert.assertEquals("in-flight session must be closed, not pooled, after runtime close", 1, recorder.closes());
    try {
      runtime.invokeSharedByName("getStatus", new Class<?>[0], new Object[0]);
      Assert.fail("expected invocation on a closed runtime to fail");
    } catch (MetaException expected) {
      Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("closed"));
    }
    Assert.assertEquals(1, factory.opens.get());
  }

  @Test
  public void closeTimeoutDefersClassLoaderCloseUntilInFlightSessionReturns() throws Throwable {
    Path hdpJar = Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar")
        .toAbsolutePath();
    Assume.assumeTrue(Files.isReadable(hdpJar));
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch finish = new CountDownLatch(1);
    CloseRecorder recorder = new CloseRecorder();
    ControlledSessionFactory factory = new ControlledSessionFactory(
        () -> isolatedSession(recorder, blockingThriftClient(entered, finish)),
        true);
    BackendRuntime runtime = BackendRuntime.open(
        config(),
        pooledCatalogConfig(1, 200L, hdpJar.toString()),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        factory);
    MetastoreApiClassLoader classLoader = (MetastoreApiClassLoader) factory.isolatedClassLoaders.get(0);
    Assert.assertNotNull(classLoader);

    AtomicReference<Throwable> callerFailure = new AtomicReference<>();
    Thread caller = new Thread(() -> {
      try {
        runtime.invokeSharedByName("getStatus", new Class<?>[0], new Object[0]);
      } catch (Throwable t) {
        callerFailure.set(t);
      }
    }, "in-flight-caller");
    caller.start();
    Assert.assertTrue("caller did not reach the backend call", entered.await(10, TimeUnit.SECONDS));

    runtime.close();

    Assert.assertNotNull(
        "isolated classloader must stay usable while a request is still in flight",
        classLoader.findResource("org/apache/hadoop/hive/metastore/api/Table.class"));

    finish.countDown();
    caller.join(TimeUnit.SECONDS.toMillis(10));
    Assert.assertNull(callerFailure.get());
    Assert.assertEquals(1, recorder.closes());
    Assert.assertNull(
        "isolated classloader must be closed once the last in-flight session returns",
        classLoader.findResource("org/apache/hadoop/hive/metastore/api/Database.class"));
  }

  private static ProxyConfig config() {
    return ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("catalog1")
        .catalogs(Map.of("catalog1", catalogConfig(null, null)))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();
  }

  private static CatalogConfig catalogConfig(
      MetastoreRuntimeProfile runtimeProfile,
      String backendJar
  ) {
    return new CatalogConfig(
        "catalog1",
        "c1",
        "file:///c1",
        false,
        CatalogAccessMode.READ_WRITE,
        java.util.List.of(),
        runtimeProfile,
        backendJar,
        Map.of("hive.metastore.uris", "thrift://one"));
  }

  private static CatalogConfig pooledCatalogConfig(
      int sharedSessionPoolSize,
      long latencyBudgetMs,
      String backendJar
  ) {
    return new CatalogConfig(
        "catalog1",
        "c1",
        "file:///c1",
        false,
        CatalogAccessMode.READ_WRITE,
        List.of(),
        CatalogExposureMode.ALLOW_ALL,
        List.of(),
        Map.of(),
        backendJar == null
            ? MetastoreRuntimeProfile.APACHE_3_1_3
            : MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        backendJar,
        Map.of("hive.metastore.uris", "thrift://one"),
        latencyBudgetMs,
        128,
        0L,
        sharedSessionPoolSize,
        4,
        0L);
  }

  private static ThriftHiveMetastore.Iface blockingThriftClient(CountDownLatch entered, CountDownLatch finish) {
    return (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if (!"getStatus".equals(method.getName())) {
            throw new UnsupportedOperationException(method.getName());
          }
          entered.countDown();
          if (!finish.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("blocking backend call was never released");
          }
          return null;
        });
  }

  /** Builds a session whose close() is observable through the raw isolated client stand-in. */
  private static BackendInvocationSession isolatedSession(
      Object rawClient,
      ThriftHiveMetastore.Iface delegate
  ) throws Exception {
    IsolatedInvocationBridge bridge = new IsolatedInvocationBridge(
        BackendRuntimeTest.class.getClassLoader(), delegate, ThriftHiveMetastore.Iface.class);
    Constructor<IsolatedMetastoreClient> isolatedCtor = IsolatedMetastoreClient.class.getDeclaredConstructor(
        Object.class, IsolatedInvocationBridge.class);
    isolatedCtor.setAccessible(true);
    IsolatedMetastoreClient isolatedClient = isolatedCtor.newInstance(rawClient, bridge);
    Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    ctor.setAccessible(true);
    return ctor.newInstance(null, null, isolatedClient);
  }

  public static final class CloseRecorder {
    private final AtomicInteger closes = new AtomicInteger();

    public void close() {
      closes.incrementAndGet();
    }

    int closes() {
      return closes.get();
    }
  }

  @FunctionalInterface
  private interface SessionSupplier {
    BackendInvocationSession get() throws Exception;
  }

  private static final class ControlledSessionFactory implements BackendRuntime.SessionFactory {
    private final SessionSupplier supplier;
    private final boolean requiresIsolatedClassLoader;
    private final AtomicInteger opens = new AtomicInteger();
    private final List<ClassLoader> isolatedClassLoaders = new ArrayList<>();

    private ControlledSessionFactory(SessionSupplier supplier) {
      this(supplier, false);
    }

    private ControlledSessionFactory(SessionSupplier supplier, boolean requiresIsolatedClassLoader) {
      this.supplier = supplier;
      this.requiresIsolatedClassLoader = requiresIsolatedClassLoader;
    }

    @Override
    public boolean requiresIsolatedClassLoader(MetastoreRuntimeProfile runtimeProfile) {
      return requiresIsolatedClassLoader && runtimeProfile != null && runtimeProfile.isHortonworks();
    }

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, null);
    }

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        ClassLoader isolatedClassLoader
    ) throws MetaException {
      isolatedClassLoaders.add(isolatedClassLoader);
      if (opens.incrementAndGet() > 1) {
        throw new MetaException("unexpected extra backend session open");
      }
      try {
        return supplier.get();
      } catch (Exception e) {
        MetaException me = new MetaException("controlled session factory failed");
        me.initCause(e);
        throw me;
      }
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
    ) throws MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, null);
    }
  }

  private static BackendInvocationSession newSession() throws Exception {
    ThriftHiveMetastore.Iface thriftClient = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          throw new UnsupportedOperationException(method.getName());
        });
    Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    ctor.setAccessible(true);
    return ctor.newInstance(null, thriftClient, null);
  }

  private static final class RecordingSessionFactory implements BackendRuntime.SessionFactory {
    private final List<MetastoreRuntimeProfile> openProfiles = new ArrayList<>();
    private final List<ClassLoader> isolatedClassLoaders = new ArrayList<>();
    private final boolean requiresIsolatedClassLoader;

    private RecordingSessionFactory() {
      this(false);
    }

    private RecordingSessionFactory(boolean requiresIsolatedClassLoader) {
      this.requiresIsolatedClassLoader = requiresIsolatedClassLoader;
    }

    @Override
    public boolean requiresIsolatedClassLoader(MetastoreRuntimeProfile runtimeProfile) {
      return requiresIsolatedClassLoader && runtimeProfile != null && runtimeProfile.isHortonworks();
    }

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws org.apache.hadoop.hive.metastore.api.MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, null);
    }

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        ClassLoader isolatedClassLoader
    ) throws org.apache.hadoop.hive.metastore.api.MetaException {
      openProfiles.add(runtimeProfile);
      isolatedClassLoaders.add(isolatedClassLoader);
      try {
        return newSession();
      } catch (Exception e) {
        org.apache.hadoop.hive.metastore.api.MetaException metaException =
            new org.apache.hadoop.hive.metastore.api.MetaException("test session factory failed");
        metaException.initCause(e);
        throw metaException;
      }
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
    ) throws org.apache.hadoop.hive.metastore.api.MetaException {
      return openImpersonating(
          proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, userName, groupNames, null);
    }

    @Override
    public BackendInvocationSession openImpersonating(
        ProxyConfig proxyConfig,
        CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        String userName,
        List<String> groupNames,
        ClassLoader isolatedClassLoader
    ) throws org.apache.hadoop.hive.metastore.api.MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile, isolatedClassLoader);
    }
  }
}
