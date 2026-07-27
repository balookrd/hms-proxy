package io.github.mmalykhin.hmsproxy.app;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.ProxyConfigLoader;
import io.github.mmalykhin.hmsproxy.config.listener.AdditionalFrontendConfig;
import io.github.mmalykhin.hmsproxy.frontend.HortonworksFrontendExtension;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.restcatalog.IcebergRestServices;
import io.github.mmalykhin.hmsproxy.restcatalog.RestCatalogServer;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import io.github.mmalykhin.hmsproxy.routing.RoutingMetaStoreProxy;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import io.github.mmalykhin.hmsproxy.security.MetastoreThriftServer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    // Opened before the first resource so the shutdown hook can wait for the ordered teardown
    // that unwinds on this thread, no matter where startup fails.
    CountDownLatch teardownComplete = new CountDownLatch(1);
    try {
      ProxyConfig config = ProxyConfigLoader.load(Path.of(args[0]));
      ProxyObservability observability = new ProxyObservability(config);
      FrontDoorSecurity frontDoorSecurity = FrontDoorSecurity.open(config);
      try (frontDoorSecurity;
           CatalogRouter router = CatalogRouter.open(config, observability.metrics());
           ManagementHttpServer managementServer = ManagementHttpServer.open(config, router, observability);
           RoutingMetaStoreProxy handler =
               new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router),
                   frontDoorSecurity, observability)) {
        ThriftHiveMetastore.Iface proxy =
            RoutingMetaStoreProxy.newProxy(
                ThriftHiveMetastore.Iface.class,
                handler,
                HortonworksFrontendExtension.class);
        try (AdditionalFrontendThriftServers extras =
            AdditionalFrontendThriftServers.open(config, proxy, frontDoorSecurity);
             IcebergRestServices restServices =
                 config.restCatalog().enabled() ? IcebergRestServices.open(config, proxy) : null;
             RestCatalogServer restServer = RestCatalogServer.open(config, restServices)) {
        MetastoreThriftServer server = new MetastoreThriftServer(config, proxy, frontDoorSecurity);
        installShutdownHook(server, teardownComplete, config.server().shutdownTimeoutSeconds());
        LOG.info("Starting HMS proxy '{}' on {}:{}", config.server().name(),
            config.server().bindHost(), config.server().port());
        for (AdditionalFrontendConfig extra : extras.running()) {
          LOG.info("Additional frontend '{}' on {}:{} advertising profile {}",
              extra.name(), extra.bindHost(), extra.port(), extra.frontendProfile());
        }
        LOG.info("Routing config: defaultCatalog='{}', catalogDbSeparator='{}', catalogs={}",
            config.defaultCatalog(), config.catalogDbSeparator(), config.catalogNames());
        LOG.info("Compatibility config: frontendProfile={}, frontendVersion={}, preserveBackendCatalogName={}",
            config.compatibility().frontendProfile(),
            config.compatibility().frontendProfile().metastoreVersion(),
            config.federation().preserveBackendCatalogName());
        LOG.info("Frontend runtime profile: {} ({})",
            config.compatibility().frontendProfile().runtimeProfile(),
            config.compatibility().frontendProfile().runtimeProfile().displayName());
        if (config.compatibility().backendStandaloneMetastoreJar() != null) {
          LOG.info("Backend standalone-metastore jar override: {}",
              config.compatibility().backendStandaloneMetastoreJar());
        }
        if (config.compatibility().frontendProfile().runtimeProfile().isHortonworks()) {
          LOG.warn("Hortonworks frontend profile is enabled through the standalone-metastore jar {}. "
                  + "Common Apache/HDP-overlapping thrift calls are bridged automatically, and selected "
                  + "HDP-only request-wrapper methods are adapted to Apache equivalents. Some HDP-only "
                  + "methods without a safe Apache mapping can still remain unsupported.",
              config.compatibility().frontendStandaloneMetastoreJar());
        }
        for (String catalogName : config.catalogNames()) {
          LOG.info("Catalog '{}' external DB example: {}",
              catalogName, router.externalDatabaseName(catalogName, "default"));
        }
        server.serve();
        }
      }
    } catch (Exception e) {
      emitKerberosJvmHint(e);
      throw e;
    } finally {
      // Releases the shutdown hook: everything above this point (additional frontends,
      // management listener, router backends, front-door security) has been closed in order.
      teardownComplete.countDown();
    }
  }

  private static void installShutdownHook(
      MetastoreThriftServer server,
      CountDownLatch teardownComplete,
      int shutdownTimeoutSeconds
  ) {
    Runtime.getRuntime().addShutdownHook(new Thread(
        new ProxyShutdownHook(server::stop, teardownComplete,
            TimeUnit.SECONDS.toMillis(shutdownTimeoutSeconds)),
        "hms-proxy-shutdown"));
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
