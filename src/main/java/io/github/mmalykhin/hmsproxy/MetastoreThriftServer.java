package io.github.mmalykhin.hmsproxy;

import java.net.InetSocketAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MetastoreThriftServer {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreThriftServer.class);

  private final ProxyConfig config;
  private final TServer server;

  MetastoreThriftServer(ProxyConfig config, ThriftHiveMetastore.Iface handler) throws Exception {
    this.config = config;
    TProcessor processor = new ThriftHiveMetastore.Processor<>(handler);
    TServerSocket serverSocket = new TServerSocket(
        new InetSocketAddress(config.server().bindHost(), config.server().port()));

    TTransportFactory transportFactory = new TTransportFactory();
    if (config.security().kerberosEnabled()) {
      Configuration securityConf = new Configuration(false);
      securityConf.set("hadoop.security.authentication", config.security().mode().hadoopAuthValue());
      UserGroupInformation.setConfiguration(securityConf);
      HadoopThriftAuthBridge bridge = HadoopThriftAuthBridge.getBridge();
      HadoopThriftAuthBridge.Server saslServer = bridge.createServer(
          config.security().keytab(),
          config.security().serverPrincipal(),
          frontDoorClientPrincipal(config.security()));
      transportFactory = saslServer.createTransportFactory(bridge.getHadoopSaslProperties(securityConf));
      processor = saslServer.wrapProcessor(processor);
      LOG.info("Kerberos/SASL enabled with principal {}", config.security().serverPrincipal());
    }

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
        .processor(processor)
        .transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(config.server().minWorkerThreads())
        .maxWorkerThreads(config.server().maxWorkerThreads());
    this.server = new TThreadPoolServer(args);
  }

  static String frontDoorClientPrincipal(ProxyConfig.SecurityConfig security) {
    // Hive's thrift bridge uses this principal to validate inbound Kerberos/SASL clients for the
    // proxy listener itself. Backend credentials are configured separately via client-principal.
    return security.serverPrincipal();
  }

  void serve() {
    server.serve();
  }

  void stop() {
    if (server.isServing()) {
      server.stop();
    }
  }
}
