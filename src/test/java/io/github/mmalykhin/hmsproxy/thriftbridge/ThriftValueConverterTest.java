package io.github.mmalykhin.hmsproxy.thriftbridge;

import io.github.mmalykhin.hmsproxy.backend.MetastoreApiClassLoader;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class ThriftValueConverterTest {
  private static final Path HIVE_4_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-common-4.1.0.jar").toAbsolutePath();
  private static final Path HDP_78_JAR =
      Path.of("hive-metastore", "hive-standalone-metastore-3.1.0.3.1.0.0-78.jar").toAbsolutePath();

  @Test
  public void mapsForeignTransportExceptionSubclassesToParentTTransportException() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    try (MetastoreApiClassLoader loader = MetastoreApiClassLoader.forBackendRuntime(
        HIVE_4_JAR, MetastoreRuntimeProfile.APACHE_4_1_0, getClass().getClassLoader())) {
      Class<?> eofClass = Class.forName("org.apache.thrift.transport.TEOFException", true, loader);
      Throwable foreignEof = (Throwable) eofClass.getConstructor(String.class).newInstance("unexpected eof");

      Throwable convertedEof = ThriftValueConverter.convertThrowable(foreignEof, getClass().getClassLoader());
      Assert.assertTrue(
          "subclass of child TTransportException must map onto parent TTransportException, got "
              + convertedEof.getClass(),
          convertedEof instanceof TTransportException);
      Assert.assertTrue(ThriftFailureClassifier.isTransportFailure(convertedEof));
      Assert.assertEquals("unexpected eof", convertedEof.getMessage());

      Class<?> saslFrameClass =
          Class.forName("org.apache.thrift.transport.sasl.TInvalidSaslFrameException", true, loader);
      Throwable foreignSasl = (Throwable) saslFrameClass.getConstructor(String.class).newInstance("bad frame");

      Throwable convertedSasl = ThriftValueConverter.convertThrowable(foreignSasl, getClass().getClassLoader());
      Assert.assertTrue(
          "deep subclass of child TTransportException must map onto parent TTransportException, got "
              + convertedSasl.getClass(),
          convertedSasl instanceof TTransportException);
      Assert.assertTrue(ThriftFailureClassifier.isTransportFailure(convertedSasl));
      Assert.assertEquals("bad frame", convertedSasl.getMessage());

      Class<?> protoClass = Class.forName("org.apache.thrift.protocol.TProtocolException", true, loader);
      Throwable foreignProto = (Throwable) protoClass.getConstructor(int.class, String.class)
          .newInstance(TProtocolException.INVALID_DATA, "bad data");
      Throwable convertedProto = ThriftValueConverter.convertThrowable(foreignProto, getClass().getClassLoader());
      Assert.assertTrue(convertedProto instanceof TProtocolException);
      Assert.assertEquals(TProtocolException.INVALID_DATA, ((TProtocolException) convertedProto).getType());
    }
  }

  @Test
  public void convertThrowableGuardsAgainstSelfReferentialCause() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    try (MetastoreApiClassLoader loader = MetastoreApiClassLoader.forBackendRuntime(
        HIVE_4_JAR, MetastoreRuntimeProfile.APACHE_4_1_0, getClass().getClassLoader())) {
      @SuppressWarnings("serial")
      TException selfCausing = new TException("loop") {
        @Override
        public synchronized Throwable getCause() {
          return this;
        }
      };

      Throwable converted = ThriftValueConverter.convertThrowable(selfCausing, loader);
      Assert.assertNotNull(converted);
      Assert.assertEquals("loop", converted.getMessage());
      Assert.assertNotSame(converted, converted.getCause());
    }
  }

  @Test
  public void releasesThreadLocalSerializerAfterOversizedStruct() throws Exception {
    Assume.assumeTrue(Files.isReadable(HDP_78_JAR));
    try (MetastoreApiClassLoader loader = new MetastoreApiClassLoader(
        MetastoreApiClassLoader.buildIsolatedRuntimeUrls(HDP_78_JAR),
        getClass().getClassLoader())) {
      Class<?> childTableClass = Class.forName("org.apache.hadoop.hive.metastore.api.Table", true, loader);
      Method putToParams = Table.class.getMethod("putToParameters", String.class, String.class);

      TSerializer initial = ThriftValueConverter.currentThreadSerializerForTest();

      Table smallTable = new Table();
      smallTable.setDbName("sales");
      smallTable.setTableName("small");
      ThriftValueConverter.convertTBase(smallTable, childTableClass);
      TSerializer afterSmall = ThriftValueConverter.currentThreadSerializerForTest();
      Assert.assertSame("small conversions should reuse the thread-local serializer", initial, afterSmall);

      Table largeTable = new Table();
      largeTable.setDbName("sales");
      largeTable.setTableName("large");
      String oversized = "x".repeat(ThriftValueConverter.MAX_RETAINED_SERIALIZER_BYTES + 4096);
      putToParams.invoke(largeTable, "comment", oversized);
      ThriftValueConverter.convertTBase(largeTable, childTableClass);

      TSerializer afterLarge = ThriftValueConverter.currentThreadSerializerForTest();
      Assert.assertNotSame(
          "oversized serialization should drop the expanded ByteArrayOutputStream from ThreadLocal",
          initial,
          afterLarge);
    }
  }
}
