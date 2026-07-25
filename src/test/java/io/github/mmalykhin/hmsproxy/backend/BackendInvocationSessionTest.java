package io.github.mmalykhin.hmsproxy.backend;

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

  private static final class CloseRecordingClient implements AutoCloseable {
    private int closes;

    @Override
    public void close() {
      closes++;
    }
  }
}
