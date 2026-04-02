package io.github.mmalykhin.hmsproxy.frontend;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class FrontendProcessorFactoryTest {
  private static final Path HDP_78_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();
  private static final Path HDP_6150_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar").toAbsolutePath();

  @Test
  public void apacheFrontendUsesNativeProcessor() throws Exception {
    TProcessor processor = FrontendProcessorFactory.create(apacheConfig(), noopHandler());

    Assert.assertEquals(
        "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Processor",
        processor.getClass().getName());
  }

  @Test
  public void hortonworksFrontendUsesIsolatedProcessor() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));

    TProcessor processor = FrontendProcessorFactory.create(
        hortonworksConfig(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_0_78, HDP_78_JAR),
        noopHandler());

    Assert.assertEquals(
        "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Processor",
        processor.getClass().getName());
    Assert.assertNotSame(
        ThriftHiveMetastore.Processor.class.getClassLoader(),
        processor.getClass().getClassLoader());
  }

  @Test
  public void newerHortonworksFrontendUsesIsolatedProcessor() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    TProcessor processor = FrontendProcessorFactory.create(
        hortonworksConfig(ProxyConfig.FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR),
        noopHandler());

    Assert.assertEquals(
        "org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Processor",
        processor.getClass().getName());
    Assert.assertNotSame(
        ThriftHiveMetastore.Processor.class.getClassLoader(),
        processor.getClass().getClassLoader());
  }

  private static ProxyConfig apacheConfig() {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", new ProxyConfig.CatalogConfig(
            "catalog1", "c1", "file:///c1", false, ProxyConfig.CatalogAccessMode.READ_WRITE, java.util.List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            ProxyConfig.FrontendProfile.APACHE_3_1_3,
            null,
            null,
            false));
  }

  private static ProxyConfig hortonworksConfig(ProxyConfig.FrontendProfile frontendProfile, Path jar) {
    return new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, Map.of()),
        "__",
        "catalog1",
        Map.of("catalog1", new ProxyConfig.CatalogConfig(
            "catalog1", "c1", "file:///c1", false, ProxyConfig.CatalogAccessMode.READ_WRITE, java.util.List.of(),
            null, null, Map.of("hive.metastore.uris", "thrift://one"))),
        new ProxyConfig.CompatibilityConfig(
            frontendProfile,
            jar.toString(),
            null,
            false));
  }

  private static ThriftHiveMetastore.Iface noopHandler() {
    return (ThriftHiveMetastore.Iface) java.lang.reflect.Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (proxy, method, args) -> {
          throw new UnsupportedOperationException(method.getName());
        });
  }
}
