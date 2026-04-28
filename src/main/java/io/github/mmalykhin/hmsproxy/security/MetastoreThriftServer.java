package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.frontend.FrontendProcessorFactory;
import java.net.BindException;
import java.net.InetSocketAddress;
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

  private final ProxyConfig config;
  private final FrontDoorSecurity frontDoorSecurity;
  private final TServer server;

  public MetastoreThriftServer(
      ProxyConfig config,
      ThriftHiveMetastore.Iface handler,
      FrontDoorSecurity frontDoorSecurity
  ) throws Exception {
    this.config = config;
    this.frontDoorSecurity = frontDoorSecurity;
    TProcessor processor = FrontendProcessorFactory.create(config, handler);
    TServerSocket serverSocket;
    try {
      serverSocket = new TServerSocket(
          new InetSocketAddress(config.server().bindHost(), config.server().port()));
    } catch (TTransportException e) {
      String reason = e.getCause() instanceof BindException
          ? e.getCause().getMessage()
          : e.getMessage();
      LOG.error("Failed to bind metastore Thrift listener on {}:{} - {}",
          config.server().bindHost(), config.server().port(), reason);
      throw e;
    }

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

  public void serve() {
    server.serve();
  }

  public void stop() {
    if (server.isServing()) {
      server.stop();
    }
    if (frontDoorSecurity != null) {
      frontDoorSecurity.close();
    }
  }
}
