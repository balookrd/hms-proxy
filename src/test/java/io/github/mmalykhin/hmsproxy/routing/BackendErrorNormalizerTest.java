package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

public class BackendErrorNormalizerTest {

  // What HiveMetaStoreClient.open() throws when the backend host is down (measured on the stand).
  private static final String CONNECT_FAILURE = "Could not connect to meta store using any of the URIs"
      + " provided. Most recent failure: org.apache.thrift.transport.TTransportException:"
      + " java.net.UnknownHostException: hms-apache";

  @Test
  public void connectFailureNamesTheCatalogOnTheDeclaredMethodPath() throws Exception {
    Method createTable = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    MetaException original = new MetaException(CONNECT_FAILURE);

    Throwable normalized = BackendErrorNormalizer.normalize(createTable, "apache", original);

    assertWrapsConnectFailure(normalized, original, "create_table");
  }

  @Test
  public void connectFailureNamesTheCatalogOnTheInfrastructurePath() {
    MetaException original = new MetaException(CONNECT_FAILURE);

    Throwable normalized = BackendErrorNormalizer.normalizeInfrastructure("apache", "get_table_req", original);

    assertWrapsConnectFailure(normalized, original, "get_table_req");
  }

  @Test
  public void otherMetaExceptionsPassThroughUnchanged() throws Exception {
    Method createTable = ThriftHiveMetastore.Iface.class.getMethod("create_table", Table.class);
    MetaException business = new MetaException("Table already exists: default.t");

    Assert.assertSame(business, BackendErrorNormalizer.normalize(createTable, "apache", business));
    Assert.assertSame(business, BackendErrorNormalizer.normalizeInfrastructure("apache", "create_table", business));
  }

  private static void assertWrapsConnectFailure(Throwable normalized, MetaException original, String method) {
    Assert.assertTrue(String.valueOf(normalized), normalized instanceof MetaException);
    Assert.assertEquals(
        "Backend catalog 'apache' failed in method '" + method + "' with MetaException: " + CONNECT_FAILURE,
        normalized.getMessage());
    Assert.assertSame(original, normalized.getCause());
    // Outage handling keeps recognizing the wrapped failure.
    Assert.assertTrue(ThriftFailureClassifier.isConnectFailure(normalized));
    Assert.assertTrue(BackendCallDispatcher.isOutageFailure(normalized));
  }
}
