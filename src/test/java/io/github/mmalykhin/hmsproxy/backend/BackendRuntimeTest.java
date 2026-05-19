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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assume;
import org.junit.Assert;
import org.junit.Test;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
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
