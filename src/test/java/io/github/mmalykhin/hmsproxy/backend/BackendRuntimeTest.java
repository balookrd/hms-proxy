package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

public class BackendRuntimeTest {
  @Test
  public void reconnectKeepsApacheSessionWhenDetectedVersionIsApache() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();
    BackendInvocationSession bootstrap = newSession("3.1.3");
    factory.enqueue("3.1.3");
    BackendRuntime runtime = BackendRuntime.open(
        config(), catalogConfig(null, null), new HiveConf(), false,
        MetastoreRuntimeProfile.APACHE_3_1_3, bootstrap, factory);
    ApacheBackendAdapter adapter = new ApacheBackendAdapter("3.1.3");

    String version = runtime.reconnectShared(adapter);

    Assert.assertEquals("3.1.3", version);
    Assert.assertEquals(List.of(MetastoreRuntimeProfile.APACHE_3_1_3), factory.openProfiles);
  }

  @Test
  public void reconnectSwitchesToHortonworksSessionWhenDetectedVersionIsLegacy() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();
    BackendInvocationSession bootstrap = newSession("3.1.3");
    factory.enqueue("3.1.0.3.1.0.0-78");
    factory.enqueue("3.1.0.3.1.0.0-78");
    BackendRuntime runtime = BackendRuntime.open(
        config(), catalogConfig(null, null), new HiveConf(), false,
        MetastoreRuntimeProfile.APACHE_3_1_3, bootstrap, factory);
    ApacheBackendAdapter adapter = new ApacheBackendAdapter("3.1.3");

    String version = runtime.reconnectShared(adapter);

    Assert.assertEquals("3.1.0.3.1.0.0-78", version);
    Assert.assertEquals(
        List.of(MetastoreRuntimeProfile.APACHE_3_1_3, MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78),
        factory.openProfiles);
    Assert.assertEquals(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, adapter.runtimeProfile());
  }

  @Test
  public void openUsesRuntimeOverrideImmediatelyForHortonworksCatalog() throws Exception {
    RecordingSessionFactory factory = new RecordingSessionFactory();
    BackendInvocationSession bootstrap = newSession("3.1.3");
    factory.enqueue("3.1.0.3.1.0.0-78");

    BackendRuntime.open(
        config(),
        catalogConfig(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78, "/tmp/hdp.jar"),
        new HiveConf(),
        false,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
        bootstrap,
        factory);

    Assert.assertEquals(List.of(MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78), factory.openProfiles);
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
        runtimeProfile,
        backendJar,
        Map.of("hive.metastore.uris", "thrift://one"));
  }

  private static BackendInvocationSession newSession(String version) throws Exception {
    ThriftHiveMetastore.Iface thriftClient = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          if ("getVersion".equals(method.getName())) {
            return version;
          }
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
    private final Deque<String> versions = new ArrayDeque<>();
    private final List<MetastoreRuntimeProfile> openProfiles = new ArrayList<>();

    void enqueue(String version) {
      versions.addLast(version);
    }

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
        return newSession(versions.isEmpty() ? "3.1.3" : versions.removeFirst());
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
