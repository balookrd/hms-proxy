package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
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

  @Test
  public void drainEndsTheReadOfAnOpenConnectionAndClosingUntracksIt() throws Exception {
    FrontDoorServerSocket serverSocket = newServerSocket(new ClientSocketConfig(5_000, true, 30, 10, 3));
    try {
      serverSocket.listen();
      int port = serverSocket.getServerSocket().getLocalPort();
      try (Socket client = new Socket()) {
        client.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
        TSocket accepted = (TSocket) serverSocket.accept();
        Assert.assertEquals(1, serverSocket.trackedClientCount());
        // Opened before the drain, as the worker's transport already holds it.
        InputStream in = accepted.getSocket().getInputStream();

        serverSocket.drainClients();
        // The client is still connected: only the drain can produce EOF before the 5s timeout.
        Assert.assertEquals(-1, in.read());

        accepted.close();
        Assert.assertEquals("a closed connection must not stay tracked",
            0, serverSocket.trackedClientCount());
      }
    } finally {
      serverSocket.close();
    }
  }

  @Test
  public void connectionAcceptedAfterDrainIsDrainedOnAccept() throws Exception {
    FrontDoorServerSocket serverSocket = newServerSocket(new ClientSocketConfig(5_000, true, 30, 10, 3));
    try {
      serverSocket.listen();
      int port = serverSocket.getServerSocket().getLocalPort();
      serverSocket.drainClients();
      try (Socket client = new Socket()) {
        client.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
        TSocket accepted = (TSocket) serverSocket.accept();
        try {
          Assert.assertTrue(accepted.getSocket().isInputShutdown());
        } finally {
          accepted.close();
        }
      }
    } finally {
      serverSocket.close();
    }
  }

  @Test
  public void saslHandshakeTimeoutAndMaxFrameGuardEnforcesAndRestores() throws Exception {
    ClientSocketConfig socketConfig = new ClientSocketConfig(5_000, true, 30, 10, 3);
    FrontDoorServerSocket serverSocket = newServerSocket(socketConfig);
    try {
      serverSocket.listen();
      int port = serverSocket.getServerSocket().getLocalPort();
      try (Socket client = new Socket()) {
        client.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
        FrontDoorServerSocket.TrackedSocket accepted =
            (FrontDoorServerSocket.TrackedSocket) serverSocket.accept();
        try {
          Assert.assertTrue(accepted.beginSaslHandshake(1_234, 16));
          Assert.assertFalse("re-entering handshake must be idempotent",
              accepted.beginSaslHandshake(999, 8));
          Assert.assertEquals(1_234, accepted.getSocket().getSoTimeout());

          OutputStream out = client.getOutputStream();
          // Frame 1: length = 0 (status=START, len=0), read in small 2-byte and 3-byte chunks.
          out.write(new byte[] {0x01, 0, 0, 0, 0});
          // Frame 2: length = 4 (status=OK, len=4, payload="abcd").
          out.write(new byte[] {0x02, 0, 0, 0, 4, 'a', 'b', 'c', 'd'});
          out.flush();

          byte[] buf = new byte[16];
          Assert.assertEquals(2, accepted.read(buf, 0, 2));
          Assert.assertEquals(3, accepted.read(buf, 2, 3));
          Assert.assertEquals(9, accepted.read(buf, 5, 9));

          // Post-handshake: restores clientTimeoutMs and disables SASL frame inspection.
          accepted.finishSaslHandshake();
          Assert.assertEquals(5_000, accepted.getSocket().getSoTimeout());
          out.write(new byte[] {0x01, 0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
          out.flush();
          Assert.assertEquals(5, accepted.read(buf, 0, 5));
        } finally {
          accepted.close();
        }
      }
    } finally {
      serverSocket.close();
    }
  }

  @Test
  public void saslHandshakeRejectsOversizedAndNegativeFrameLengthsBeforeAllocation() throws Exception {
    ClientSocketConfig socketConfig = new ClientSocketConfig(5_000, true, 30, 10, 3);
    FrontDoorServerSocket serverSocket = newServerSocket(socketConfig);
    try {
      serverSocket.listen();
      int port = serverSocket.getServerSocket().getLocalPort();
      for (byte[] header : new byte[][] {
          new byte[] {0x01, 0, 0, 0, 17},
          new byte[] {0x01, (byte) 0x80, 0, 0, 1}
      }) {
        try (Socket client = new Socket()) {
          client.connect(new InetSocketAddress("127.0.0.1", port), 2_000);
          FrontDoorServerSocket.TrackedSocket accepted =
              (FrontDoorServerSocket.TrackedSocket) serverSocket.accept();
          try {
            Assert.assertTrue(accepted.beginSaslHandshake(1_000, 16));
            client.getOutputStream().write(header);
            client.getOutputStream().flush();
            byte[] buf = new byte[5];
            int total = 0;
            try {
              while (total < 5) {
                total += accepted.read(buf, total, 5 - total);
              }
              Assert.fail("Expected TTransportException for invalid SASL handshake header");
            } catch (TTransportException expected) {
              Assert.assertTrue(expected.getMessage(),
                  expected.getMessage().contains("exceeds configured limit"));
            }
          } finally {
            accepted.finishSaslHandshake();
            accepted.close();
          }
        }
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
