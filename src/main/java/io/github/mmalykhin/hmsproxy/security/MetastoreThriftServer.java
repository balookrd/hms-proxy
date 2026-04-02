package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.frontend.FrontendProcessorFactory;
import java.net.InetSocketAddress;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    TServerSocket serverSocket = new TServerSocket(
        new InetSocketAddress(config.server().bindHost(), config.server().port()));

    TTransportFactory transportFactory = new TTransportFactory();
    if (frontDoorSecurity != null) {
      transportFactory = frontDoorSecurity.createTransportFactory();
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

  public static String frontDoorClientPrincipal(ProxyConfig.SecurityConfig security) {
    // Hive's thrift bridge uses this principal to validate inbound Kerberos/SASL clients for the
    // proxy listener itself. Backend credentials are configured separately via client-principal.
    return security.serverPrincipal();
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
