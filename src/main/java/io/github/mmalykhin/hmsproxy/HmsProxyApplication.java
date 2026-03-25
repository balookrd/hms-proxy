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
    LoggingBootstrap.initialize();

    if (args.length != 1) {
      System.err.println("Usage: java -jar hms-proxy.jar <config.properties>");
      System.exit(64);
    }

    HiveConf.setLoadMetastoreConfig(false);
    HiveConf.setLoadHiveServer2Config(false);

    try {
      ProxyConfig config = ProxyConfigLoader.load(Path.of(args[0]));
      try (CatalogRouter router = CatalogRouter.open(config)) {
        FrontDoorSecurity frontDoorSecurity = FrontDoorSecurity.open(config);
        RoutingMetaStoreHandler handler = new RoutingMetaStoreHandler(config, router, frontDoorSecurity);
        ThriftHiveMetastore.Iface proxy =
            RoutingMetaStoreHandler.newProxy(ThriftHiveMetastore.Iface.class, handler);
        MetastoreThriftServer server = new MetastoreThriftServer(config, proxy, frontDoorSecurity);
        installShutdownHook(server);
        LOG.info("Starting HMS proxy '{}' on {}:{}", config.server().name(),
            config.server().bindHost(), config.server().port());
        server.serve();
      }
    } catch (Exception e) {
      emitKerberosJvmHint(e);
      throw e;
    }
  }

  private static void installShutdownHook(MetastoreThriftServer server) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("Shutdown requested, stopping HMS proxy");
      server.stop();
    }, "hms-proxy-shutdown"));
  }

  private static void emitKerberosJvmHint(Throwable error) {
    if (!containsKerberosModuleAccessFailure(error)) {
      return;
    }

    System.err.println("Kerberos startup failed because Hadoop 2.x is running on a modular JDK.");
    System.err.println(
        "Start the proxy with JVM flags "
            + "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
            + "and --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED.");
    System.err.println(
        "Also make sure a readable krb5.conf is available, for example via "
            + "-Djava.security.krb5.conf=/etc/krb5.conf.");
  }

  private static boolean containsKerberosModuleAccessFailure(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof IllegalAccessException
          && current.getMessage() != null
          && current.getMessage().contains("sun.security.krb5")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
