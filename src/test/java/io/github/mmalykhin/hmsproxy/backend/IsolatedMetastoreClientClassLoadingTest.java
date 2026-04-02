package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.nio.file.Files;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assume;
import org.junit.Assert;
import org.junit.Test;

public class IsolatedMetastoreClientClassLoadingTest {
  @Test
  public void hortonworksRuntimeForcesSequentialUriSelection() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hive.metastore.uri.selection", "RANDOM");

    IsolatedMetastoreClient.applyHortonworksCompatibilityWorkarounds(
        conf,
        Configuration.class,
        MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78);

    Assert.assertEquals("SEQUENTIAL", conf.get("hive.metastore.uri.selection"));
  }

  @Test
  public void apacheRuntimeLeavesUriSelectionUntouched() throws Exception {
    Configuration conf = new Configuration(false);
    conf.set("hive.metastore.uri.selection", "RANDOM");

    IsolatedMetastoreClient.applyHortonworksCompatibilityWorkarounds(
        conf,
        Configuration.class,
        MetastoreRuntimeProfile.APACHE_3_1_3);

    Assert.assertEquals("RANDOM", conf.get("hive.metastore.uri.selection"));
  }

  @Test
  public void isolatedLoaderUsesChildHadoopClasses() throws Exception {
    java.nio.file.Path jarFile = java.nio.file.Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar")
        .toAbsolutePath();
    Assume.assumeTrue(Files.isReadable(jarFile));

    ClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(jarFile),
        IsolatedMetastoreClientClassLoadingTest.class.getClassLoader());

    Assert.assertNotSame(Configuration.class, Class.forName(
        "org.apache.hadoop.conf.Configuration",
        true,
        classLoader));
    Assert.assertNotSame(
        org.apache.hadoop.security.UserGroupInformation.class,
        Class.forName("org.apache.hadoop.security.UserGroupInformation", true, classLoader));
    Assert.assertNotSame(
        org.apache.hadoop.util.ReflectionUtils.class,
        Class.forName("org.apache.hadoop.util.ReflectionUtils", true, classLoader));
  }

  @Test
  public void configurationResolvesFilterHookInIsolatedLoader() throws Exception {
    ProxyConfig config = new ProxyConfig(
        new ProxyConfig.ServerConfig("test", "127.0.0.1", 9083, 1, 4),
        new ProxyConfig.SecurityConfig(ProxyConfig.SecurityMode.NONE, null, null, null, null, false, java.util.Map.of()),
        ".",
        "catalog1",
        java.util.Map.of("catalog1", new ProxyConfig.CatalogConfig(
            "catalog1",
            "c1",
            "file:///c1",
            false,
            ProxyConfig.CatalogAccessMode.READ_WRITE,
            java.util.List.of(),
            MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78,
            "hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar",
            java.util.Map.of("hive.metastore.uris", "thrift://one"))));

    java.nio.file.Path jarFile = java.nio.file.Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar")
        .toAbsolutePath();
    Assume.assumeTrue(Files.isReadable(jarFile));

    java.nio.file.Path jar = MetastoreRuntimeJarResolver.resolveBackendJar(
        config, config.catalogs().get("catalog1"), MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78);
    ClassLoader classLoader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(jar),
        IsolatedMetastoreClientClassLoadingTest.class.getClassLoader());
    Class<?> implClass = Class.forName(
        "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl",
        true,
        classLoader);
    Class<?> interfaceClass = Class.forName(
        "org.apache.hadoop.hive.metastore.MetaStoreFilterHook",
        true,
        classLoader);
    Configuration conf = new Configuration(false);
    conf.setClassLoader(classLoader);

    Class<?> resolved = conf.getClass(
        "hive.metastore.filter.hook",
        implClass.asSubclass(Object.class),
        interfaceClass.asSubclass(Object.class));

    Assert.assertEquals(implClass, resolved);
  }
}
