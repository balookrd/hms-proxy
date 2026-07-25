package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.thrift.transport.TSocket;
import org.junit.Assert;
import org.junit.Test;

public class FrontDoorServerSocketTest {

  @Test
  public void acceptedSocketGetsClientTimeoutAndKeepAlive() throws Exception {
    ClientSocketConfig socketConfig = new ClientSocketConfig(1234, true, 30, 10, 3);
    FrontDoorServerSocket serverSocket = newServerSocket(socketConfig);
    try {
      TSocket accepted = acceptOneConnection(serverSocket);
      try {
        Assert.assertEquals("accepted socket must not block in read forever",
            1234, accepted.getSocket().getSoTimeout());
        Assert.assertTrue("accepted socket must have TCP keepalive enabled",
            accepted.getSocket().getKeepAlive());
      } finally {
        accepted.close();
      }
    } finally {
      serverSocket.close();
    }
  }

  /**
   * libthrift's own TSocket(Socket) enables SO_KEEPALIVE, so disabling it in the proxy config has
   * to actively turn it back off.
   */
  @Test
  public void disabledSettingsOverrideTheLibthriftDefaults() throws Exception {
    ClientSocketConfig socketConfig = new ClientSocketConfig(0, false, 30, 10, 3);
    FrontDoorServerSocket serverSocket = newServerSocket(socketConfig);
    try {
      TSocket accepted = acceptOneConnection(serverSocket);
      try {
        Assert.assertEquals(0, accepted.getSocket().getSoTimeout());
        Assert.assertFalse(accepted.getSocket().getKeepAlive());
      } finally {
        accepted.close();
      }
    } finally {
      serverSocket.close();
    }
  }

  private static FrontDoorServerSocket newServerSocket(ClientSocketConfig socketConfig)
      throws Exception {
    return new FrontDoorServerSocket(
        new InetSocketAddress("127.0.0.1", 0), socketConfig, "front-door-socket-test");
  }

  private static TSocket acceptOneConnection(FrontDoorServerSocket serverSocket) throws Exception {
    int port = serverSocket.getServerSocket().getLocalPort();
    serverSocket.listen();
    try (Socket client = new Socket()) {
      client.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
      return (TSocket) serverSocket.accept();
    }
  }
}
