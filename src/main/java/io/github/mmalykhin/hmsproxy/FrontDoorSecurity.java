package io.github.mmalykhin.hmsproxy;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MemoryTokenStore;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FrontDoorSecurity implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FrontDoorSecurity.class);

  private final Configuration securityConf;
  private final HadoopThriftAuthBridge bridge;
  private final HadoopThriftAuthBridge.Server saslServer;
  private final MetastoreDelegationTokenManager delegationTokenManager;

  private FrontDoorSecurity(
      Configuration securityConf,
      HadoopThriftAuthBridge bridge,
      HadoopThriftAuthBridge.Server saslServer,
      MetastoreDelegationTokenManager delegationTokenManager
  ) {
    this.securityConf = securityConf;
    this.bridge = bridge;
    this.saslServer = saslServer;
    this.delegationTokenManager = delegationTokenManager;
  }

  static FrontDoorSecurity open(ProxyConfig config) throws Exception {
    if (!config.security().kerberosEnabled()) {
      return null;
    }

    HiveConf securityConf = new HiveConf();
    securityConf.set("hadoop.security.authentication", config.security().mode().hadoopAuthValue());
    UserGroupInformation.setConfiguration(securityConf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(securityConf);

    HadoopThriftAuthBridge bridge = HadoopThriftAuthBridge.getBridge();
    HadoopThriftAuthBridge.Server saslServer = bridge.createServer(
        config.security().keytab(),
        config.security().serverPrincipal(),
        MetastoreThriftServer.frontDoorClientPrincipal(config.security()));

    MetastoreDelegationTokenManager delegationTokenManager = new MetastoreDelegationTokenManager();
    delegationTokenManager.startDelegationTokenSecretManager(securityConf, null);
    saslServer.setSecretManager(delegationTokenManager.getSecretManager());

    String tokenStoreClass = securityConf.get(
        "hive.cluster.delegation.token.store.class",
        MemoryTokenStore.class.getName());
    LOG.info("Front door delegation-token manager started using token store {}", tokenStoreClass);
    if (MemoryTokenStore.class.getName().equals(tokenStoreClass)) {
      LOG.warn("Front door delegation-token manager is using in-memory token storage. "
              + "Proxy restarts will invalidate existing HiveServer2 delegation tokens. "
              + "Configure a persistent token store in hive-site.xml/metastore-site.xml "
              + "(for example ZooKeeperTokenStore or DBTokenStore) if HS2 sessions must survive proxy restarts.");
    }
    return new FrontDoorSecurity(securityConf, bridge, saslServer, delegationTokenManager);
  }

  TTransportFactory createTransportFactory() throws Exception {
    Map<String, String> saslProperties = bridge.getHadoopSaslProperties(securityConf);
    return saslServer.createTransportFactory(saslProperties);
  }

  TProcessor wrapProcessor(TProcessor processor) {
    return saslServer.wrapProcessor(processor);
  }

  String issueDelegationToken(String owner, String renewer) throws IOException, InterruptedException {
    return delegationTokenManager.getDelegationToken(owner, renewer, remoteAddress());
  }

  long renewDelegationToken(String token) throws IOException {
    return delegationTokenManager.renewDelegationToken(token);
  }

  void cancelDelegationToken(String token) throws IOException {
    delegationTokenManager.cancelDelegationToken(token);
  }

  String remoteUser() {
    return saslServer.getRemoteUser();
  }

  private String remoteAddress() {
    InetAddress address = saslServer.getRemoteAddress();
    if (address == null) {
      return null;
    }
    return address.getHostAddress();
  }

  @Override
  public void close() {
    if (delegationTokenManager.getSecretManager() != null) {
      delegationTokenManager.getSecretManager().stopThreads();
    }
  }
}
