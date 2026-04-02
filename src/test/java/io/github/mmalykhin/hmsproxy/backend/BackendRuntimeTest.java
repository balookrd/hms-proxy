package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

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

  private static ProxyConfig config() {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", catalogConfig(null, null)));
  }

  private static ProxyConfig.CatalogConfig catalogConfig(
      MetastoreRuntimeProfile runtimeProfile,
      String backendJar
  ) {
    return new ProxyConfig.CatalogConfig(
        "catalog1",
        "c1",
        "file:///c1",
        false,
        ProxyConfig.CatalogAccessMode.READ_WRITE,
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

    @Override
    public BackendInvocationSession open(
        ProxyConfig proxyConfig,
        ProxyConfig.CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile
    ) throws org.apache.hadoop.hive.metastore.api.MetaException {
      openProfiles.add(runtimeProfile);
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
        ProxyConfig.CatalogConfig catalogConfig,
        HiveConf hiveConf,
        boolean backendKerberosEnabled,
        MetastoreRuntimeProfile runtimeProfile,
        String userName,
        List<String> groupNames
    ) throws org.apache.hadoop.hive.metastore.api.MetaException {
      return open(proxyConfig, catalogConfig, hiveConf, backendKerberosEnabled, runtimeProfile);
    }
  }
}
