package io.github.mmalykhin.hmsproxy;

import java.nio.file.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HmsProxyApplication {
  private static final Logger LOG = LoggerFactory.getLogger(HmsProxyApplication.class);

  private HmsProxyApplication() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: java -jar hms-proxy.jar <config.properties>");
      System.exit(64);
    }

    HiveConf.setLoadMetastoreConfig(false);
    HiveConf.setLoadHiveServer2Config(false);

    ProxyConfig config = ProxyConfigLoader.load(Path.of(args[0]));
    try (CatalogRouter router = CatalogRouter.open(config)) {
      RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router);
      ThriftHiveMetastore.Iface proxy =
          RoutingMetaStoreHandler.newProxy(ThriftHiveMetastore.Iface.class, handler);
      MetastoreThriftServer server = new MetastoreThriftServer(config, proxy);
      installShutdownHook(server);
      LOG.info("Starting HMS proxy '{}' on {}:{}", config.server().name(),
          config.server().bindHost(), config.server().port());
      server.serve();
    }
  }

  private static void installShutdownHook(MetastoreThriftServer server) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("Shutdown requested, stopping HMS proxy");
      server.stop();
    }, "hms-proxy-shutdown"));
  }
}
