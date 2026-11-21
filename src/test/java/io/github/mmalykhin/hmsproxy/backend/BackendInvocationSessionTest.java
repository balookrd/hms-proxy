package io.github.mmalykhin.hmsproxy.backend;

import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.junit.Assert;
import org.junit.Test;

public class BackendInvocationSessionTest {
  @Test
  public void failedThriftClientExtractionClosesBackendClient() {
    CloseRecordingClient client = new CloseRecordingClient();

    try {
      BackendInvocationSession.extractThriftClientOrClose(client);
      Assert.fail("expected reflective thrift client extraction to fail for a foreign client type");
    } catch (Exception expected) {
      // HiveMetaStoreClient.client cannot be read from an unrelated instance
    }

    Assert.assertEquals("client must be closed when thrift client extraction fails", 1, client.closes);
  }

  @Test
  public void runAfterCloseChainsCallbacksPreservingAllActionsAndRunsImmediatelyWhenClosed() throws Exception {
    Constructor<BackendInvocationSession> ctor = BackendInvocationSession.class.getDeclaredConstructor(
        org.apache.hadoop.hive.metastore.HiveMetaStoreClient.class,
        ThriftHiveMetastore.Iface.class,
        IsolatedMetastoreClient.class);
    ctor.setAccessible(true);
    ThriftHiveMetastore.Iface proxy = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[] {ThriftHiveMetastore.Iface.class},
        (p, method, args) -> null);
    BackendInvocationSession session = ctor.newInstance(null, proxy, null);

    List<String> executed = new ArrayList<>();
    session.runAfterClose(() -> {
      executed.add("first");
      throw new RuntimeException("simulated first hook failure");
    });
    session.runAfterClose(() -> executed.add("second"));

    try {
      session.close();
      Assert.fail("expected runtime failure from first close hook to propagate");
    } catch (RuntimeException expected) {
      Assert.assertEquals("simulated first hook failure", expected.getMessage());
    }
    Assert.assertEquals("all chained hooks must run even if an earlier hook fails",
        List.of("first", "second"), executed);

    session.runAfterClose(() -> executed.add("after-close"));
    Assert.assertEquals("hook registered on an already closed session must execute immediately",
        List.of("first", "second", "after-close"), executed);
  }

  private static final class CloseRecordingClient implements AutoCloseable {
    private int closes;

    @Override
    public void close() {
      closes++;
    }
  }
}
