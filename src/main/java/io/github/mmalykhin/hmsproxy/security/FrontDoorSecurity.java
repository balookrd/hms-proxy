package io.github.mmalykhin.hmsproxy.security;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.util.PrincipalUtil;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.MemoryTokenStore;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FrontDoorSecurity implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(FrontDoorSecurity.class);
  private static final String TOKEN_STORE_CLASS_KEY = "hive.cluster.delegation.token.store.class";
  private static final String FALLBACK_TOKEN_STORE_CLASS_KEY = "metastore.cluster.delegation.token.store.class";
  private static final String METASTORE_KERBEROS_PRINCIPAL_KEY = "hive.metastore.kerberos.principal";
  private static final String METASTORE_KERBEROS_KEYTAB_KEY = "hive.metastore.kerberos.keytab.file";
  private static final String TOKEN_STORE_CONNECT_STRING_KEY =
      "hive.cluster.delegation.token.store.zookeeper.connectString";
  private static final String FALLBACK_TOKEN_STORE_CONNECT_STRING_KEY =
      "metastore.cluster.delegation.token.store.zookeeper.connectString";
  private static final String TOKEN_STORE_ZK_QUORUM_KEY = "hive.zookeeper.quorum";
  private static final String TOKEN_STORE_ZNODE_KEY =
      "hive.cluster.delegation.token.store.zookeeper.znode";
  private static final String FALLBACK_TOKEN_STORE_ZNODE_KEY =
      "metastore.cluster.delegation.token.store.zookeeper.znode";

  private final Configuration securityConf;
  private final HadoopThriftAuthBridge bridge;
  private final HadoopThriftAuthBridge.Server saslServer;
  private final MetastoreDelegationTokenManager delegationTokenManager;
  private final LocalDelegationTokenStore localTokenStore;

  private FrontDoorSecurity(
      Configuration securityConf,
      HadoopThriftAuthBridge bridge,
      HadoopThriftAuthBridge.Server saslServer,
      MetastoreDelegationTokenManager delegationTokenManager,
      LocalDelegationTokenStore localTokenStore
  ) {
    this.securityConf = securityConf;
    this.bridge = bridge;
    this.saslServer = saslServer;
    this.delegationTokenManager = delegationTokenManager;
    this.localTokenStore = localTokenStore;
  }

  public static FrontDoorSecurity open(ProxyConfig config) throws Exception {
    if (!config.security().kerberosEnabled()) {
      return null;
    }

    HiveConf securityConf = new HiveConf();
    config.security().frontDoorConf().forEach(securityConf::set);
    securityConf.set("hadoop.security.authentication", config.security().mode().hadoopAuthValue());
    applyZooKeeperKerberosDefaults(config, securityConf);
    configureZooKeeperClientJaas(securityConf);
    emitConfigurationDiagnostics(config, securityConf);
    ProcessKerberosConfiguration.processWide().installFrontDoorConfiguration(securityConf);
    ensureKeytabLoginUser(config, securityConf, ProcessKerberosConfiguration.processWide());
    ProxyUsers.refreshSuperUserGroupsConfiguration(securityConf);

    HadoopThriftAuthBridge bridge = HadoopThriftAuthBridge.getBridge();
    String serverPrincipal = KerberosPrincipalUtil.resolveForLocalHost(config.security().serverPrincipal());
    HadoopThriftAuthBridge.Server saslServer = bridge.createServer(
        config.security().keytab(),
        serverPrincipal,
        KerberosPrincipalUtil.resolveForLocalHost(MetastoreThriftServer.frontDoorClientPrincipal(config.security())));
    // The bridge logs the server principal in itself, so later startup steps must not replace the
    // process login user underneath this SASL server.
    ProcessKerberosConfiguration.processWide().recordLoginUser(serverPrincipal);

    MetastoreDelegationTokenManager delegationTokenManager = new MetastoreDelegationTokenManager();
    delegationTokenManager.startDelegationTokenSecretManager(securityConf, null);
    saslServer.setSecretManager(delegationTokenManager.getSecretManager());
    LocalDelegationTokenStore localTokenStore =
        LocalDelegationTokenStore.fromSecretManager(delegationTokenManager.getSecretManager());

    String tokenStoreClass = tokenStoreClass(securityConf);
    LOG.info("Front door delegation-token manager started using token store {}", tokenStoreClass);
    String connectString = firstConfiguredValue(
        securityConf,
        TOKEN_STORE_CONNECT_STRING_KEY,
        FALLBACK_TOKEN_STORE_CONNECT_STRING_KEY,
        TOKEN_STORE_ZK_QUORUM_KEY);
    String znode = firstConfiguredValue(
        securityConf,
        TOKEN_STORE_ZNODE_KEY,
        FALLBACK_TOKEN_STORE_ZNODE_KEY);
    if (connectString != null || znode != null) {
      LOG.info("Front door delegation-token store details: connectString='{}', znode='{}'",
          connectString != null ? connectString : "<unset>",
          znode != null ? znode : "<unset>");
    }
    if (MemoryTokenStore.class.getName().equals(tokenStoreClass)) {
      LOG.warn("Front door delegation-token manager is using in-memory token storage. "
              + "Proxy restarts will invalidate existing HiveServer2 delegation tokens. "
              + "Configure a persistent token store in hive-site.xml or via "
              + "security.front-door-conf.* properties "
              + "(for example ZooKeeperTokenStore or DBTokenStore) if HS2 sessions must survive proxy restarts.");
      if (HiveConf.getHiveSiteLocation() == null && config.security().frontDoorConf().isEmpty()) {
        emitImportantWarning("HiveConf did not load hive-site.xml for the proxy process, and no "
            + "security.front-door-conf.* overrides were provided. "
            + "Front-door delegation tokens are therefore running on MemoryTokenStore. "
            + "Put hive-site.xml on the proxy classpath or set the delegation-token store "
            + "directly in hms-proxy.properties.");
      }
    }
    return new FrontDoorSecurity(securityConf, bridge, saslServer, delegationTokenManager, localTokenStore);
  }

  private static void emitConfigurationDiagnostics(ProxyConfig config, HiveConf securityConf) {
    URL hiveSite = HiveConf.getHiveSiteLocation();
    URL metastoreSite = HiveConf.getMetastoreSiteLocation();
    LOG.info("Front door HiveConf resources: hive-site={}, metastore-site={}",
        formatUrl(hiveSite), formatUrl(metastoreSite));
    if (!config.security().frontDoorConf().isEmpty()) {
      LOG.info("Applied {} front-door HiveConf override(s) from proxy config: {}",
          config.security().frontDoorConf().size(),
          config.security().frontDoorConf().keySet());
      LOG.debug("Effective front-door token store class after overrides: {}", tokenStoreClass(securityConf));
    }
  }

  static void applyZooKeeperKerberosDefaults(ProxyConfig config, Configuration conf) {
    if (!config.security().kerberosEnabled()) {
      return;
    }
    String tokenStoreClass = tokenStoreClass(conf);
    if (!tokenStoreClass.endsWith(".ZooKeeperTokenStore")) {
      return;
    }
    if (!config.security().frontDoorConf().containsKey(METASTORE_KERBEROS_PRINCIPAL_KEY)) {
      conf.set(METASTORE_KERBEROS_PRINCIPAL_KEY,
          KerberosPrincipalUtil.resolveForLocalHost(config.security().serverPrincipal()));
    }
    if (!config.security().frontDoorConf().containsKey(METASTORE_KERBEROS_KEYTAB_KEY)) {
      conf.set(METASTORE_KERBEROS_KEYTAB_KEY, config.security().keytab());
    }
  }

  static void ensureKeytabLoginUser(
      ProxyConfig config,
      Configuration conf,
      ProcessKerberosConfiguration kerberos
  ) throws IOException {
    if (!config.security().kerberosEnabled()) {
      return;
    }
    if (!tokenStoreClass(conf).endsWith(".ZooKeeperTokenStore")) {
      return;
    }
    String principal = KerberosPrincipalUtil.resolveForLocalHost(config.security().serverPrincipal());
    LOG.info("Refreshing Hadoop login user from keytab before starting ZooKeeperTokenStore using principal {}",
        principal);
    kerberos.ensureLoginUserFromKeytab(principal, config.security().keytab());
  }

  static void configureZooKeeperClientJaas(Configuration conf) throws IOException {
    if (!tokenStoreClass(conf).endsWith(".ZooKeeperTokenStore")) {
      return;
    }
    String principal = trimToNull(conf.get(METASTORE_KERBEROS_PRINCIPAL_KEY));
    String keytab = trimToNull(conf.get(METASTORE_KERBEROS_KEYTAB_KEY));
    if (principal == null || keytab == null) {
      return;
    }
    SecurityUtils.setZookeeperClientKerberosJaasConfig(principal, keytab);
    LOG.info("Configured ZooKeeper SASL client JAAS entry '{}' for delegation-token store principal {}",
        System.getProperty("zookeeper.sasl.clientconfig", "<unset>"),
        principal);
  }

  private static String tokenStoreClass(Configuration conf) {
    String value = firstConfiguredValue(conf, TOKEN_STORE_CLASS_KEY, FALLBACK_TOKEN_STORE_CLASS_KEY);
    return value != null ? value : MemoryTokenStore.class.getName();
  }

  private static String firstConfiguredValue(Configuration conf, String... keys) {
    for (String key : keys) {
      String value = trimToNull(conf.get(key));
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static String formatUrl(URL url) {
    return url == null ? "<not found>" : url.toExternalForm();
  }

  private static void emitImportantWarning(String message) {
    LOG.warn(message);
    System.err.println("hms-proxy WARN: " + message);
  }

  TTransportFactory createTransportFactory() throws Exception {
    Map<String, String> saslProperties = bridge.getHadoopSaslProperties(securityConf);
    return saslServer.createTransportFactory(saslProperties);
  }

  TProcessor wrapProcessor(TProcessor processor) {
    return wrapWithClientRequestContext(
        processor, saslServer::wrapProcessor, this::remoteAddress, this::remoteUser);
  }

  /**
   * Captures the client identity into {@link ClientRequestContext} from *inside* the SASL
   * processor. Hive's {@code TUGIAssumingProcessor} publishes the per-request remote address and
   * remote user into static ThreadLocals within its own {@code process()} call and never clears
   * them, so reading them before that call returns the identity left behind by whichever
   * connection previously used this pooled worker thread.
   */
  static TProcessor wrapWithClientRequestContext(
      TProcessor processor,
      UnaryOperator<TProcessor> saslWrapper,
      Supplier<String> remoteAddressSupplier,
      Supplier<String> remoteUserSupplier
  ) {
    TProcessor contextAware = (in, out) -> {
      String previousRemoteAddress = ClientRequestContext.setRemoteAddress(remoteAddressSupplier.get());
      String previousRemoteUser = ClientRequestContext.setRemoteUser(remoteUserSupplier.get());
      try {
        return processor.process(in, out);
      } finally {
        ClientRequestContext.restoreRemoteAddress(previousRemoteAddress);
        ClientRequestContext.restoreRemoteUser(previousRemoteUser);
      }
    };
    return saslWrapper.apply(contextAware);
  }

  public String issueDelegationToken(String owner, String renewer)
      throws IOException, InterruptedException, MetaException {
    try {
      return delegationTokenManager.getDelegationToken(owner, renewer, remoteAddress());
    } catch (AuthorizationException e) {
      MetaException metaException = new MetaException(
          delegationTokenAuthorizationMessage(owner, renewer, remoteUser(), remoteAddress()));
      metaException.initCause(e);
      throw metaException;
    }
  }

  public long renewDelegationToken(String token) throws IOException {
    return delegationTokenManager.renewDelegationToken(token);
  }

  public void cancelDelegationToken(String token) throws IOException {
    delegationTokenManager.cancelDelegationToken(token);
  }

  public boolean addToken(String tokenIdentifier, String token) throws MetaException {
    return localTokenStore.addToken(tokenIdentifier, token);
  }

  public boolean removeToken(String tokenIdentifier) throws MetaException {
    return localTokenStore.removeToken(tokenIdentifier);
  }

  public String getToken(String tokenIdentifier) throws MetaException {
    return localTokenStore.getToken(tokenIdentifier);
  }

  public java.util.List<String> getAllTokenIdentifiers() throws MetaException {
    return localTokenStore.getAllTokenIdentifiers();
  }

  public int addMasterKey(String key) throws MetaException {
    return localTokenStore.addMasterKey(key);
  }

  public void updateMasterKey(int keySeq, String key) throws MetaException {
    localTokenStore.updateMasterKey(keySeq, key);
  }

  public boolean removeMasterKey(int keySeq) {
    return localTokenStore.removeMasterKey(keySeq);
  }

  public java.util.List<String> getMasterKeys() throws MetaException {
    return localTokenStore.getMasterKeys();
  }

  String remoteUser() {
    return saslServer.getRemoteUser();
  }

  public static String delegationTokenAuthorizationMessage(
      String owner,
      String renewer,
      String authenticatedUser,
      String remoteAddress
  ) {
    String principal = trimToNull(authenticatedUser);
    String proxyUser = PrincipalUtil.shortUserName(principal);
    if (proxyUser == null) {
      proxyUser = "<service-user>";
    }

    StringBuilder message = new StringBuilder()
        .append("Front-door get_delegation_token for owner '")
        .append(owner)
        .append("' and renewer '")
        .append(renewer)
        .append("' was authenticated as '")
        .append(principal != null ? principal : "<unknown>")
        .append("'");
    if (remoteAddress != null) {
      message.append(" from ").append(remoteAddress);
    }
    message.append(" and was rejected by Hadoop proxy-user authorization. ")
        .append("The proxy serves delegation-token RPCs locally, so allow this service principal via ")
        .append("security.front-door-conf.hadoop.proxyuser.")
        .append(proxyUser)
        .append(".hosts and security.front-door-conf.hadoop.proxyuser.")
        .append(proxyUser)
        .append(".groups (or load the same core-site.xml into the proxy process).");
    return message.toString();
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
