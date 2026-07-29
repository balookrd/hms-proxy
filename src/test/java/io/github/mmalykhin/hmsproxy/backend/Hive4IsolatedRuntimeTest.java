package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TApplicationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * The class-loading shape of the Hive 4 backend runtime, pinned by a real classloader rather
 * than a mocked invocation handler: the smoke stand caught {@code HiveMetaStoreClient} failing
 * to even load, because the Hive 4 client is generated against libthrift 0.16 while the fat jar
 * carries 0.9.3 - a companion libthrift jar loaded child-first is what fixes it, and the value
 * converter has to keep working across that loader boundary.
 */
public class Hive4IsolatedRuntimeTest {
  private static final Path HIVE_4_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-common-4.1.0.jar").toAbsolutePath();

  @Test
  public void hive4ClientClassLoadsInIsolatedRuntime() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    ClassLoader loader = MetastoreApiClassLoader.forBackendRuntime(
        HIVE_4_JAR, MetastoreRuntimeProfile.APACHE_4_1_0, getClass().getClassLoader());

    // The exact load that failed on the stand: HiveMetaStoreClient references
    // org.apache.thrift.transport.layered.TFramedTransport, which 0.9.3 does not have.
    Class<?> client = Class.forName("org.apache.hadoop.hive.metastore.HiveMetaStoreClient", true, loader);
    Assert.assertSame(loader, client.getClassLoader());

    Class<?> layered = Class.forName("org.apache.thrift.transport.layered.TFramedTransport", true, loader);
    Assert.assertSame("companion libthrift must be child-first", loader, layered.getClassLoader());

    Class<?> childTBase = Class.forName("org.apache.thrift.TBase", true, loader);
    Assert.assertNotSame("the isolated runtime must not share the parent's TBase",
        org.apache.thrift.TBase.class, childTBase);

    // The linkage the stand actually tripped over next: ThriftHiveMetastore$Client extends
    // fb303's FacebookService$Client, and a parent-first fb303 is linked against the parent's
    // TProtocol - resolving the constructor forces that check.
    Class<?> thriftClient =
        Class.forName("org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore$Client", true, loader);
    Class<?> childProtocol = Class.forName("org.apache.thrift.protocol.TProtocol", true, loader);
    Assert.assertSame(loader, childProtocol.getClassLoader());
    Assert.assertNotNull(thriftClient.getConstructor(childProtocol, childProtocol));

    // Referenced by the Hive 4 client at call time (getTable and friends) but living in
    // hive-storage-api - the stand first died on this one only when a real RPC ran.
    Class<?> tableName = Class.forName("org.apache.hadoop.hive.common.TableName", true, loader);
    Assert.assertSame("hive-storage-api companion must be child-first", loader, tableName.getClassLoader());
  }

  @Test
  public void nonHive4RuntimeKeepsParentThrift() throws Exception {
    Path apacheJar = Path.of("hive-metastore", "hive-standalone-metastore-3.1.3.jar").toAbsolutePath();
    Assume.assumeTrue(Files.isReadable(apacheJar));
    ClassLoader loader = MetastoreApiClassLoader.forBackendRuntime(
        apacheJar, MetastoreRuntimeProfile.APACHE_3_1_3, getClass().getClassLoader());
    Assert.assertSame("3.1 runtimes must keep sharing the parent's thrift",
        org.apache.thrift.TBase.class, Class.forName("org.apache.thrift.TBase", true, loader));
  }

  @Test
  public void convertsStructsAcrossTheThriftLoaderBoundary() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    ClassLoader loader = MetastoreApiClassLoader.forBackendRuntime(
        HIVE_4_JAR, MetastoreRuntimeProfile.APACHE_4_1_0, getClass().getClassLoader());

    // Child -> parent: a struct built inside the isolated runtime (child TBase) must land as
    // the parent's same-named class with its fields intact.
    Class<?> childTableClass = Class.forName("org.apache.hadoop.hive.metastore.api.Table", true, loader);
    Object childTable = childTableClass.getConstructor().newInstance();
    childTableClass.getMethod("setTableName", String.class).invoke(childTable, "events");
    childTableClass.getMethod("setDbName", String.class).invoke(childTable, "sales");

    Object converted = ThriftValueConverter.convertDynamicValue(childTable, getClass().getClassLoader());
    Assert.assertTrue(converted instanceof Table);
    Assert.assertEquals("events", ((Table) converted).getTableName());
    Assert.assertEquals("sales", ((Table) converted).getDbName());

    // Parent -> child: the inbound direction the invocation bridge takes for every argument.
    Table parentTable = new Table();
    parentTable.setTableName("clicks");
    Object childConverted = ThriftValueConverter.convertValue(parentTable, childTableClass, loader);
    Assert.assertSame(childTableClass, childConverted.getClass());
    Method getTableName = childTableClass.getMethod("getTableName");
    Assert.assertEquals("clicks", getTableName.invoke(childConverted));
  }

  @Test
  public void mapsForeignThriftInfrastructureExceptionsOntoTheParent() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    ClassLoader loader = MetastoreApiClassLoader.forBackendRuntime(
        HIVE_4_JAR, MetastoreRuntimeProfile.APACHE_4_1_0, getClass().getClassLoader());

    Class<?> childAppException = Class.forName("org.apache.thrift.TApplicationException", true, loader);
    Assume.assumeTrue("companion libthrift not child-first?", childAppException.getClassLoader() == loader);
    Throwable child = (Throwable) childAppException
        .getConstructor(int.class, String.class)
        .newInstance(TApplicationException.UNKNOWN_METHOD, "no such method");

    Throwable converted = ThriftValueConverter.convertThrowable(child, getClass().getClassLoader());
    Assert.assertTrue("classifier-visible parent type expected, got " + converted.getClass(),
        converted instanceof TApplicationException);
    Assert.assertEquals(TApplicationException.UNKNOWN_METHOD,
        ((TApplicationException) converted).getType());
    Assert.assertEquals("no such method", converted.getMessage());
  }
}
