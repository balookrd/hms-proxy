package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.frontend.FrontendProcessorFactory;
import io.github.mmalykhin.hmsproxy.config.server.ClientSocketConfig;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.security.SecurityConfig;

public final class MetastoreThriftServer {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreThriftServer.class);
  private static final long STOP_POLL_MILLIS = 25L;
  private static final long STOP_TIMEOUT_MILLIS = 10_000L;

  private final String listenerName;
  private final TServerSocket serverSocket;
  private final TServer server;

  private final Object lifecycle = new Object();
  private boolean stopRequested;
  private volatile boolean insideServe;

  public MetastoreThriftServer(
      ProxyConfig config,
      ThriftHiveMetastore.Iface handler,
      FrontDoorSecurity frontDoorSecurity
  ) throws Exception {
    this.listenerName = config.server().name();
    TProcessor processor = FrontendProcessorFactory.create(config, handler);
    ClientSocketConfig clientSocket = config.server().clientSocket();
    try {
      this.serverSocket = new FrontDoorServerSocket(
          new InetSocketAddress(config.server().bindHost(), config.server().port()),
          clientSocket,
          listenerName);
    } catch (TTransportException e) {
      String reason = e.getCause() instanceof BindException
          ? e.getCause().getMessage()
          : e.getMessage();
      LOG.error("Failed to bind metastore Thrift listener on {}:{} - {}",
          config.server().bindHost(), config.server().port(), reason);
      throw e;
    }
    logClientSocketSettings(clientSocket);

    TTransportFactory transportFactory = new TTransportFactory();
    if (frontDoorSecurity != null) {
      transportFactory = unwrappingSaslFactory(frontDoorSecurity.createTransportFactory());
      processor = frontDoorSecurity.wrapProcessor(processor);
      LOG.info("Kerberos/SASL enabled with principal {}", config.security().serverPrincipal());
      LOG.info("Front door delegation-token DIGEST auth is enabled");
    }

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
        .processor(processor)
        .transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(config.server().minWorkerThreads())
        .maxWorkerThreads(config.server().maxWorkerThreads());
    this.server = new TThreadPoolServer(args);
  }

  private void logClientSocketSettings(ClientSocketConfig clientSocket) {
    if (!clientSocket.clientTimeoutEnabled()) {
      LOG.warn("Listener '{}' accepts client sockets without a read timeout "
              + "(server.client-socket-timeout-ms=0); a client dying without FIN/RST can pin a "
              + "worker thread until TCP keepalive gives up", listenerName);
    }
    LOG.info("Listener '{}' front-door socket settings: clientTimeoutMs={}, tcpKeepAlive={}{}",
        listenerName,
        clientSocket.clientTimeoutMs(),
        clientSocket.tcpKeepAlive(),
        clientSocket.tcpKeepAlive()
            ? " (idle=" + clientSocket.keepAliveIdleSeconds() + "s, interval="
                + clientSocket.keepAliveIntervalSeconds() + "s, count=" + clientSocket.keepAliveCount()
                + ", dead-peer detection <= " + clientSocket.keepAliveDetectionSeconds() + "s)"
            : "");
  }

  public static String frontDoorClientPrincipal(SecurityConfig security) {
    // Hive's thrift bridge uses this principal to validate inbound Kerberos/SASL clients for the
    // proxy listener itself. Backend credentials are configured separately via client-principal.
    return security.serverPrincipal();
  }

  /**
   * Wraps a SASL transport factory to unwrap RuntimeExceptions caused by TSaslTransportException
   * back into TTransportException. TThreadPoolServer catches TTransportException silently, but
   * logs RuntimeException at ERROR. The underlying cause (client connected without sending SASL
   * data, e.g. a TCP-level health probe) is benign and not worth alarming on.
   */
  private static TTransportFactory unwrappingSaslFactory(TTransportFactory delegate) {
    return new TTransportFactory() {
      @Override
      public TTransport getTransport(TTransport base) {
        try {
          return delegate.getTransport(base);
        } catch (RuntimeException e) {
          if (e.getCause() instanceof TTransportException) {
            // TTransportFactory.getTransport() does not declare TTransportException, but
            // TThreadPoolServer catches it by type at runtime. Sneaky-throw the underlying
            // TTransportException so TThreadPoolServer handles it silently instead of
            // logging a spurious ERROR for benign connections (e.g. TCP health probes)
            // that send no SASL data.
            MetastoreThriftServer.<RuntimeException>sneakyThrow(e.getCause());
          }
          throw e;
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }

  /**
   * Blocks in the Thrift accept loop until {@link #stop()} is called. Returns immediately when a
   * stop was already requested: libthrift's {@code serve()} clears its own {@code stopped_} flag
   * on entry, so a stop that lost the race would otherwise be discarded and the listener would
   * keep accepting.
   */
  public void serve() {
    synchronized (lifecycle) {
      if (stopRequested) {
        LOG.info("Listener '{}' was stopped before it started serving", listenerName);
        return;
      }
      insideServe = true;
    }
    try {
      server.serve();
    } finally {
      insideServe = false;
    }
  }

  /**
   * Stops the listener and releases its port. Idempotent and safe to call concurrently with
   * {@link #serve()}, including before {@code serve()} has started.
   *
   * <p>Ownership note: the shared {@link FrontDoorSecurity} is deliberately not closed here.
   * Every listener of the proxy shares one instance, so stopping a single listener must not
   * stop the delegation-token secret manager threads for the others. The component that opened
   * it closes it.
   */
  public void stop() {
    synchronized (lifecycle) {
      if (stopRequested) {
        return;
      }
      stopRequested = true;
      if (!insideServe) {
        // serve() can no longer enter the Thrift loop, so release the port directly instead of
        // going through TServer.stop() (which would be a no-op on a server that never started).
        serverSocket.close();
        return;
      }
    }
    stopServingLoop();
  }

  private void stopServingLoop() {
    // TThreadPoolServer.serve() clears stopped_ exactly once, after listen() and just before the
    // accept loop. A stop() landing in that window is erased, and the accept loop then spins on
    // an already-closed socket logging a warning per iteration. Re-requesting the stop until the
    // serve loop has actually left bounds that window to a single poll interval.
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(STOP_TIMEOUT_MILLIS);
    while (true) {
      server.stop();
      if (!insideServe) {
        return;
      }
      if (System.nanoTime() - deadline >= 0L) {
        LOG.warn("Listener '{}' did not leave the Thrift accept loop within {}ms of stop()",
            listenerName, STOP_TIMEOUT_MILLIS);
        return;
      }
      try {
        Thread.sleep(STOP_POLL_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while stopping listener '{}'", listenerName);
        return;
      }
    }
  }
}
