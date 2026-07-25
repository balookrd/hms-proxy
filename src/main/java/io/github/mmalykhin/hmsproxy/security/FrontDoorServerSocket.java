package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import jdk.net.ExtendedSocketOptions;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Listening socket that bounds the lifetime of accepted front-door connections.
 *
 * <p>libthrift 0.9.3's {@code TServerSocket(InetSocketAddress)} constructor delegates with
 * {@code clientTimeout = 0}, so every accepted socket gets {@code setTimeout(0)} and a client that
 * dies without FIN/RST pins a {@code TThreadPoolServer} worker in read forever. The client timeout
 * is passed through the transport args instead.
 *
 * <p>{@code TSocket(Socket)} does enable SO_KEEPALIVE on every accepted socket, but leaves the
 * timers at the OS defaults, where a dead peer is only detected after roughly two hours. Keepalive
 * is therefore re-applied here so the configuration is authoritative in both directions, and the
 * timers are tuned per socket. Tuning uses optional JDK socket options, so a platform without them
 * degrades to plain SO_KEEPALIVE with the OS {@code tcp_keepalive_*} defaults.
 */
final class FrontDoorServerSocket extends TServerSocket {
  private static final Logger LOG = LoggerFactory.getLogger(FrontDoorServerSocket.class);

  private final ClientSocketConfig socketConfig;
  private final String listenerName;
  private volatile boolean keepAliveTuningUnsupported;

  FrontDoorServerSocket(
      InetSocketAddress bindAddr,
      ClientSocketConfig socketConfig,
      String listenerName
  ) throws TTransportException {
    super(new ServerSocketTransportArgs()
        .bindAddr(bindAddr)
        .clientTimeout(socketConfig.clientTimeoutMs()));
    this.socketConfig = socketConfig;
    this.listenerName = listenerName;
  }

  @Override
  protected TSocket acceptImpl() throws TTransportException {
    TSocket transport = super.acceptImpl();
    applyKeepAlive(transport.getSocket());
    return transport;
  }

  private void applyKeepAlive(Socket socket) {
    try {
      // Explicit in both directions: TSocket turned keepalive on already, so disabling it in the
      // proxy config has to actively turn it back off.
      socket.setKeepAlive(socketConfig.tcpKeepAlive());
    } catch (SocketException e) {
      LOG.debug("Listener '{}' could not apply TCP keepalive on an accepted socket",
          listenerName, e);
      return;
    }
    if (!socketConfig.tcpKeepAlive() || keepAliveTuningUnsupported) {
      return;
    }
    try {
      socket.setOption(ExtendedSocketOptions.TCP_KEEPIDLE, socketConfig.keepAliveIdleSeconds());
      socket.setOption(ExtendedSocketOptions.TCP_KEEPINTERVAL, socketConfig.keepAliveIntervalSeconds());
      socket.setOption(ExtendedSocketOptions.TCP_KEEPCOUNT, socketConfig.keepAliveCount());
    } catch (UnsupportedOperationException | IOException e) {
      // Logged once per listener: without the extension the socket still has SO_KEEPALIVE, it
      // just falls back to the (usually much longer) OS-wide keepalive timers.
      keepAliveTuningUnsupported = true;
      LOG.info("Listener '{}' runs on a platform without per-socket TCP keepalive tuning; "
              + "dead-peer detection falls back to the OS tcp_keepalive_* defaults ({})",
          listenerName, e.toString());
    }
  }
}
