package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 *
 * <p>Accepted sockets are also tracked so a stopping listener can {@link #drainClients() drain}
 * them: {@code TThreadPoolServer.serve()} only returns once every worker has finished, and a worker
 * parked on an idle keep-alive connection otherwise waits for the client.
 */
final class FrontDoorServerSocket extends TServerSocket {
  private static final Logger LOG = LoggerFactory.getLogger(FrontDoorServerSocket.class);

  private final ClientSocketConfig socketConfig;
  private final String listenerName;
  private volatile boolean keepAliveTuningUnsupported;
  private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
  private volatile boolean draining;

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
    // super.acceptImpl() already applied the client timeout to the socket itself; it is rewrapped
    // only to learn when the worker closes it. TSocket(Socket) turns keepalive on, so the configured
    // keepalive is applied after the rewrap.
    Socket socket = super.acceptImpl().getSocket();
    TSocket transport = new TrackedSocket(socket);
    applyKeepAlive(socket);
    clients.add(socket);
    // Registered before the check: a drain racing this accept either sees the socket in the set
    // or has already raised the flag, so no connection escapes it.
    if (draining) {
      shutdownInput(socket);
    }
    return transport;
  }

  /**
   * Half-closes the read side of every accepted connection. A worker waiting for the next request
   * reads EOF and exits; a worker in the middle of a call still writes its reply, then exits on its
   * next read. Connections accepted after this call are drained on accept. Idempotent.
   */
  void drainClients() {
    draining = true;
    for (Socket socket : clients) {
      shutdownInput(socket);
    }
  }

  int trackedClientCount() {
    return clients.size();
  }

  private void shutdownInput(Socket socket) {
    try {
      if (!socket.isClosed() && !socket.isInputShutdown()) {
        socket.shutdownInput();
      }
    } catch (IOException e) {
      // The worker owns the socket and may be closing it concurrently.
      LOG.debug("Listener '{}' could not drain a client connection", listenerName, e);
    }
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

  /** Untracks the socket when the worker closes its transport. */
  private final class TrackedSocket extends TSocket {
    private final Socket socket;

    TrackedSocket(Socket socket) throws TTransportException {
      super(socket);
      this.socket = socket;
    }

    @Override
    public void close() {
      try {
        super.close();
      } finally {
        clients.remove(socket);
      }
    }
  }
}
