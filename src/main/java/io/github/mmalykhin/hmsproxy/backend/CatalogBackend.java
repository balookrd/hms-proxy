package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.util.TimeoutValueParser;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;

public final class CatalogBackend implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogBackend.class);
  private static final String SOCKET_TIMEOUT_KEY = "hive.metastore.client.socket.timeout";
  private static final long SOCKET_TIMEOUT_MIN_HYSTERESIS_MS = 2_000L;
  private static final double SOCKET_TIMEOUT_HYSTERESIS_FRACTION = 0.25d;

  public enum AdaptiveTimeoutResult {
    APPLIED,
    SKIPPED_HYSTERESIS,
    SKIPPED_COOLDOWN,
    UNCHANGED
  }

  private final ProxyConfig proxyConfig;
  private final CatalogConfig config;
  private final HiveConf hiveConf;
  private final Map<String, ImpersonationClient> impersonationClients = new LinkedHashMap<>(16, 0.75f, true);
  private final BackendAdapter adapter;
  private final BackendRuntime runtime;
  private final Catalog catalog;
  private final PrometheusMetrics metrics;
  private final AtomicLong impersonationActiveSessions = new AtomicLong();
  private final AtomicLong impersonationIdleSessions = new AtomicLong();
  private final Object reconnectLock = new Object();
  private volatile long appliedClientTimeoutMs;
  private volatile long lastReconnectAtNanos;

  private CatalogBackend(
      ProxyConfig proxyConfig,
      CatalogConfig config,
      HiveConf hiveConf,
      BackendAdapter adapter,
      BackendRuntime runtime,
      Catalog catalog,
      PrometheusMetrics metrics
  ) {
    this.proxyConfig = proxyConfig;
    this.config = config;
    this.hiveConf = hiveConf;
    this.adapter = adapter;
    this.runtime = runtime;
    this.catalog = catalog;
    this.metrics = metrics;
    this.appliedClientTimeoutMs = TimeoutValueParser.parseDurationMs(hiveConf.get(SOCKET_TIMEOUT_KEY), 0L);
    publishImpersonationGauges();
  }

  public static CatalogBackend open(ProxyConfig proxyConfig, CatalogConfig catalogConfig)
      throws MetaException {
    return open(proxyConfig, catalogConfig, null);
  }

  public static CatalogBackend open(
      ProxyConfig proxyConfig,
      CatalogConfig catalogConfig,
      PrometheusMetrics metrics
  ) throws MetaException {
    HiveConf conf = new HiveConf();
    boolean backendKerberosEnabled = backendKerberosEnabled(catalogConfig);
    conf.set("hadoop.security.authentication", backendKerberosEnabled ? "kerberos" : "simple");
    for (Map.Entry<String, String> entry : catalogConfig.hiveConf().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    MetastoreRuntimeProfile runtimeProfile = catalogConfig.runtimeProfile() != null
        ? catalogConfig.runtimeProfile()
        : MetastoreRuntimeProfile.APACHE_3_1_3;
    BackendAdapter adapter = BackendAdapterFactory.create(runtimeProfile);
    BackendRuntime runtime = BackendRuntime.open(
        proxyConfig, catalogConfig, conf, backendKerberosEnabled, runtimeProfile, metrics);
    LOG.info("Backend catalog '{}' selected runtimeProfile={} compatibilityProfile={}",
        catalogConfig.name(), adapter.runtimeProfile(), adapter.backendProfile());
    Catalog catalog = new Catalog();
    catalog.setName(catalogConfig.name());
    catalog.setDescription(catalogConfig.description());
    catalog.setLocationUri(catalogConfig.locationUri());
    return new CatalogBackend(proxyConfig, catalogConfig, conf, adapter, runtime, catalog, metrics);
  }

  public String name() {
    return config.name();
  }

  public Catalog catalog() {
    return new Catalog(catalog);
  }

  public URI defaultFileSystemUri() {
    return FileSystem.getDefaultUri(hiveConf);
  }

  public HiveConf hiveConf() {
    return hiveConf;
  }

  public boolean impersonationEnabled() {
    return config.impersonationEnabled();
  }

  public String backendVersion() {
    return adapter.backendVersion();
  }

  public MetastoreCompatibility.BackendProfile backendProfile() {
    return adapter.backendProfile();
  }

  public MetastoreRuntimeProfile runtimeProfile() {
    return adapter.runtimeProfile();
  }

  public void checkConnectivity() throws Throwable {
    invokeRawByName("getStatus", new Class<?>[0], new Object[0], null);
  }

  public void probeConnectivity(long timeoutMs) throws Throwable {
    HiveConf probeConf = new HiveConf(hiveConf);
    probeConf.set(SOCKET_TIMEOUT_KEY, TimeoutValueParser.formatDurationMs(timeoutMs));
    try (BackendInvocationSession session = runtime.openEphemeralSession(probeConf, adapter.runtimeProfile())) {
      session.invokeByName("getStatus", new Class<?>[0], new Object[0]);
    }
  }

  public AdaptiveTimeoutResult ensureClientSocketTimeout(long timeoutMs, long cooldownMs)
      throws MetaException {
    if (timeoutMs <= 0) {
      return AdaptiveTimeoutResult.UNCHANGED;
    }
    if (!exceedsHysteresis(timeoutMs)) {
      return appliedClientTimeoutMs == timeoutMs
          ? AdaptiveTimeoutResult.UNCHANGED
          : AdaptiveTimeoutResult.SKIPPED_HYSTERESIS;
    }
    synchronized (reconnectLock) {
      if (!exceedsHysteresis(timeoutMs)) {
        return appliedClientTimeoutMs == timeoutMs
            ? AdaptiveTimeoutResult.UNCHANGED
            : AdaptiveTimeoutResult.SKIPPED_HYSTERESIS;
      }
      if (cooldownMs > 0 && lastReconnectAtNanos != 0L) {
        long elapsedMs = (System.nanoTime() - lastReconnectAtNanos) / 1_000_000L;
        if (elapsedMs < cooldownMs) {
          return AdaptiveTimeoutResult.SKIPPED_COOLDOWN;
        }
      }
      hiveConf.set(SOCKET_TIMEOUT_KEY, TimeoutValueParser.formatDurationMs(timeoutMs));
      runtime.reconnectShared(adapter);
      synchronized (this) {
        for (ImpersonationClient client : impersonationClients.values()) {
          client.evict();
        }
        impersonationClients.clear();
      }
      appliedClientTimeoutMs = timeoutMs;
      lastReconnectAtNanos = System.nanoTime();
    }
    LOG.info("Backend catalog '{}' applied adaptive socket timeout {}",
        config.name(), TimeoutValueParser.formatDurationMs(timeoutMs));
    return AdaptiveTimeoutResult.APPLIED;
  }

  public Object invoke(Method method, Object[] args, ImpersonationContext impersonation)
      throws Throwable {
    return adapter.invoke(this, method, args, impersonation);
  }

  public Object invokeRequest(
      String methodName,
      Object request,
      ImpersonationContext impersonation
  )
      throws Throwable {
    return adapter.invokeRequest(this, methodName, request, impersonation);
  }

  public Object invokeRaw(Method method, Object[] args, ImpersonationContext impersonation)
      throws Throwable {
    if (impersonation != null && config.impersonationEnabled()) {
      return invokeWithImpersonation(method, args, impersonation);
    }
    if (impersonation != null && LOG.isDebugEnabled()) {
      LOG.debug("Backend catalog '{}' has impersonation disabled, using shared client for user '{}'",
          config.name(), impersonation.userName());
    }
    return invokeSharedClient(method, args);
  }

  public Object invokeRawByName(
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args,
      ImpersonationContext impersonation
  ) throws Throwable {
    if (impersonation != null && config.impersonationEnabled()) {
      return impersonationClient(impersonation).invokeByName(methodName, parameterTypes, args);
    }
    if (impersonation != null && LOG.isDebugEnabled()) {
      LOG.debug("Backend catalog '{}' has impersonation disabled, using shared client for user '{}'",
          config.name(), impersonation.userName());
    }
    return runtime.invokeSharedByName(methodName, parameterTypes, args);
  }

  private Object invokeSharedClient(Method method, Object[] args) throws Throwable {
    return runtime.invokeShared(method, args);
  }

  private Object invokeWithImpersonation(
      Method method,
      Object[] args,
      ImpersonationContext impersonation
  ) throws Throwable {
    return impersonationClient(impersonation).invoke(method, args);
  }

  @Override
  public synchronized void close() {
    closeQuietly(runtime, "backend runtime");
    for (ImpersonationClient impersonationClient : impersonationClients.values()) {
      impersonationClient.closeQuietly();
    }
    impersonationClients.clear();
  }

  private static boolean backendKerberosEnabled(CatalogConfig catalogConfig) {
    return Boolean.parseBoolean(catalogConfig.hiveConf().getOrDefault("hive.metastore.sasl.enabled", "false"));
  }

  private boolean exceedsHysteresis(long timeoutMs) {
    long applied = appliedClientTimeoutMs;
    if (applied <= 0L) {
      return true;
    }
    long hysteresis = Math.max(
        SOCKET_TIMEOUT_MIN_HYSTERESIS_MS,
        Math.round(applied * SOCKET_TIMEOUT_HYSTERESIS_FRACTION));
    return Math.abs(timeoutMs - applied) >= hysteresis;
  }

  public static void closeQuietly(AutoCloseable closeable, String description) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception e) {
      LOG.warn("Ignoring failure while closing {}", description, e);
    }
  }

  private synchronized ImpersonationClient impersonationClient(
      ImpersonationContext impersonation
  ) throws MetaException {
    ImpersonationClient client = impersonationClients.get(impersonation.userName());
    if (client != null) {
      long ttlMs = config.impersonationClientIdleTtlMs();
      if (ttlMs > 0 && System.currentTimeMillis() - client.lastUsedMs > ttlMs) {
        LOG.info("Evicting idle impersonation client for user '{}' in catalog '{}'",
            impersonation.userName(), config.name());
        impersonationClients.remove(impersonation.userName());
        client.evict();
        client = null;
      } else {
        return client;
      }
    }

    client = new ImpersonationClient(impersonation.userName(), impersonation.groupNames());
    impersonationClients.put(impersonation.userName(), client);
    evictOldImpersonationClientsIfNeeded();
    return client;
  }

  private void evictOldImpersonationClientsIfNeeded() {
    while (impersonationClients.size() > config.maxImpersonationClients()) {
      String eldestUser = impersonationClients.keySet().iterator().next();
      ImpersonationClient evicted = impersonationClients.remove(eldestUser);
      if (evicted != null) {
        LOG.info("Evicting cached impersonation client for user '{}' in catalog '{}'",
            eldestUser, config.name());
        evicted.closeQuietly();
      }
    }
  }

  private final class ImpersonationClient implements AutoCloseable {
    private final String userName;
    private final List<String> groupNames;
    private BackendInvocationSession session;
    private volatile boolean evicted;
    volatile long lastUsedMs = System.currentTimeMillis();

    private ImpersonationClient(String userName, List<String> groupNames) throws MetaException {
      this.userName = userName;
      this.groupNames = List.copyOf(groupNames);
      open();
    }

    synchronized Object invoke(Method method, Object[] args) throws Throwable {
      lastUsedMs = System.currentTimeMillis();
      try {
        if ("set_ugi".equals(method.getName())) {
          return List.copyOf(groupNames);
        }
        return session.invoke(method, args);
      } catch (Throwable cause) {
        if (!(cause instanceof org.apache.thrift.TApplicationException)
            && !(cause instanceof org.apache.thrift.transport.TTransportException)) {
          throw cause;
        }
        if (evicted) {
          throw cause;
        }
        LOG.warn("Backend catalog '{}' transport failed for impersonated user '{}' in method {}, reconnecting once",
            config.name(), userName, method.getName(), cause);
        reconnect();
        try {
          if ("set_ugi".equals(method.getName())) {
            return List.copyOf(groupNames);
          }
          return session.invoke(method, args);
        } catch (Throwable retryError) {
          throw retryError;
        }
      }
    }

    synchronized Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
      lastUsedMs = System.currentTimeMillis();
      try {
        if ("set_ugi".equals(methodName)) {
          return List.copyOf(groupNames);
        }
        return session.invokeByName(methodName, parameterTypes, args);
      } catch (Throwable cause) {
        if (!(cause instanceof org.apache.thrift.TApplicationException)
            && !(cause instanceof org.apache.thrift.transport.TTransportException)) {
          throw cause;
        }
        if (evicted) {
          throw cause;
        }
        LOG.warn("Backend catalog '{}' transport failed for impersonated user '{}' in method {}, reconnecting once",
            config.name(), userName, methodName, cause);
        reconnect();
        if ("set_ugi".equals(methodName)) {
          return List.copyOf(groupNames);
        }
        return session.invokeByName(methodName, parameterTypes, args);
      }
    }

    @Override
    public synchronized void close() {
      CatalogBackend.closeQuietly(session, "impersonation backend metastore session for user '" + userName + "'");
    }

    private void closeQuietly() {
      try {
        close();
      } catch (RuntimeException e) {
        LOG.warn("Failed to close cached impersonation client for user '{}' in catalog '{}'",
            userName, config.name(), e);
      }
    }

    private void evict() {
      evicted = true;
      closeQuietly();
    }

    private void open() throws MetaException {
      session = runtime.openImpersonationSession(adapter.runtimeProfile(), userName, groupNames);
      LOG.debug("Opened cached impersonation client for user '{}' in catalog '{}'", userName, config.name());
    }

    private synchronized void reconnect() throws MetaException {
      close();
      open();
    }
  }
}
