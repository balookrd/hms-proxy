package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.security.ProcessKerberosConfiguration;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
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
  // Mirrors impersonationClients.size(); written under the catalog monitor so gauges can be
  // published without touching the non-thread-safe map outside it.
  private volatile int impersonationUserCount;
  private volatile long appliedClientTimeoutMs;
  private volatile long lastReconnectAtNanos;
  private volatile boolean closed;

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
    if (backendKerberosEnabled) {
      // Backend sessions open lazily and reconnect on the hot path, so the process-wide UGI
      // configuration is installed here at startup. It is a no-op when the front door already
      // installed its own, richer configuration.
      ProcessKerberosConfiguration.processWide().ensureConfigured("kerberos");
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
    if (closed) {
      throw closedException();
    }
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
      String previousTimeout = hiveConf.get(SOCKET_TIMEOUT_KEY);
      hiveConf.set(SOCKET_TIMEOUT_KEY, TimeoutValueParser.formatDurationMs(timeoutMs));
      try {
        runtime.reconnectShared(adapter);
      } catch (Throwable t) {
        // Keep hiveConf in sync with the sessions that are actually live, and arm the cooldown so a
        // failing reconnect cannot quiesce the pool again on every subsequent call.
        if (previousTimeout == null) {
          hiveConf.unset(SOCKET_TIMEOUT_KEY);
        } else {
          hiveConf.set(SOCKET_TIMEOUT_KEY, previousTimeout);
        }
        lastReconnectAtNanos = System.nanoTime();
        throw t;
      }
      // Detach the clients under the monitor, close their sockets outside of it.
      for (ImpersonationClient client : drainImpersonationClients()) {
        client.evict();
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
  public void close() {
    closed = true;
    // Impersonation sessions first: closing them may still need classes from the runtime classloader.
    // Draining detaches them under the monitor so the sockets close outside of it.
    for (ImpersonationClient impersonationClient : drainImpersonationClients()) {
      impersonationClient.closeQuietly();
    }
    closeQuietly(runtime, "backend runtime");
    publishImpersonationGauges();
  }

  private List<ImpersonationClient> drainImpersonationClients() {
    synchronized (this) {
      if (impersonationClients.isEmpty()) {
        return List.of();
      }
      List<ImpersonationClient> drained = new ArrayList<>(impersonationClients.values());
      impersonationClients.clear();
      impersonationUserCount = 0;
      return drained;
    }
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

  private MetaException closedException() {
    return new MetaException("Backend catalog '" + config.name() + "' is closed");
  }

  /**
   * Resolves (and, if needed, creates) the per-user impersonation client. The catalog monitor only
   * guards the bookkeeping map: constructing a client opens no connection, and evicted clients are
   * closed after the monitor is released, so no blocking network I/O ever runs under it.
   */
  private ImpersonationClient impersonationClient(ImpersonationContext impersonation) throws MetaException {
    ImpersonationClient idleEvicted = null;
    List<ImpersonationClient> overflowEvicted = List.of();
    ImpersonationClient client;
    synchronized (this) {
      if (closed) {
        throw closedException();
      }
      client = impersonationClients.get(impersonation.userName());
      if (client != null) {
        long ttlMs = config.impersonationClientIdleTtlMs();
        if (ttlMs > 0 && System.currentTimeMillis() - client.lastUsedMs > ttlMs) {
          impersonationClients.remove(impersonation.userName());
          idleEvicted = client;
          client = null;
        } else {
          return client;
        }
      }
      client = new ImpersonationClient(impersonation.userName(), impersonation.groupNames());
      impersonationClients.put(impersonation.userName(), client);
      overflowEvicted = drainOverflowImpersonationClientsLocked();
      impersonationUserCount = impersonationClients.size();
    }

    if (idleEvicted != null) {
      LOG.info("Evicting idle impersonation client for user '{}' in catalog '{}'",
          impersonation.userName(), config.name());
      idleEvicted.evict();
    }
    for (ImpersonationClient evicted : overflowEvicted) {
      LOG.info("Evicting cached impersonation client for user '{}' in catalog '{}'",
          evicted.userName, config.name());
      evicted.closeQuietly();
      if (metrics != null) {
        metrics.recordImpersonationSessionEviction(config.name(), "user_capacity");
      }
    }
    publishImpersonationGauges();
    return client;
  }

  private List<ImpersonationClient> drainOverflowImpersonationClientsLocked() {
    if (impersonationClients.size() <= config.maxImpersonationClients()) {
      return List.of();
    }
    List<ImpersonationClient> evicted = new ArrayList<>();
    while (impersonationClients.size() > config.maxImpersonationClients()) {
      String eldestUser = impersonationClients.keySet().iterator().next();
      ImpersonationClient removed = impersonationClients.remove(eldestUser);
      if (removed != null) {
        evicted.add(removed);
      }
    }
    return evicted;
  }

  private void publishImpersonationGauges() {
    if (metrics == null) {
      return;
    }
    metrics.setImpersonationPoolUsers(config.name(), impersonationUserCount);
    metrics.setImpersonationPoolSessions(
        config.name(),
        impersonationActiveSessions.get(),
        impersonationIdleSessions.get());
  }

  private final class ImpersonationClient {
    private static final long DEFAULT_BORROW_TIMEOUT_MS = 30_000L;

    private final String userName;
    private final List<String> groupNames;
    private final int maxSize;
    private final long sessionIdleTtlMs;
    private final Semaphore permits;
    private final Object lock = new Object();
    private final Deque<PooledSession> idle = new ArrayDeque<>();
    private int totalSessions;
    private volatile boolean evicted;
    volatile long lastUsedMs = System.currentTimeMillis();

    // Opens no session: the first borrow() creates one under a held permit, the same way the shared
    // pool does. Keeping connect/Kerberos/set_ugi out of the constructor keeps it off the catalog monitor.
    private ImpersonationClient(String userName, List<String> groupNames) {
      this.userName = userName;
      this.groupNames = List.copyOf(groupNames);
      this.maxSize = config.impersonationPoolMaxSize();
      this.sessionIdleTtlMs = config.impersonationSessionIdleTtlMs();
      this.permits = new Semaphore(maxSize, true);
    }

    Object invoke(Method method, Object[] args) throws Throwable {
      if ("set_ugi".equals(method.getName())) {
        lastUsedMs = System.currentTimeMillis();
        return List.copyOf(groupNames);
      }
      return invokeOnPool(
          method.getName(),
          session -> session.invoke(method, args));
    }

    Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
      if ("set_ugi".equals(methodName)) {
        lastUsedMs = System.currentTimeMillis();
        return List.copyOf(groupNames);
      }
      return invokeOnPool(
          methodName,
          session -> session.invokeByName(methodName, parameterTypes, args));
    }

    private Object invokeOnPool(String label, SessionCall call) throws Throwable {
      lastUsedMs = System.currentTimeMillis();
      BackendInvocationSession session = borrow();
      try {
        Object result = call.apply(session);
        release(session);
        session = null;
        return result;
      } catch (Throwable cause) {
        if (!ThriftFailureClassifier.isTransportFailure(cause) || evicted) {
          if (session != null) {
            // A protocol desync poisons the connection, but the call must not be replayed.
            if (ThriftFailureClassifier.isProtocolDesync(cause)) {
              discard(session, "protocol_desync");
            } else {
              release(session);
            }
            session = null;
          }
          throw cause;
        }
        LOG.warn("Backend catalog '{}' transport failed for impersonated user '{}' in method {}, retrying once",
            config.name(), userName, label, cause);
        discard(session, "transport_failure");
        session = null;
        BackendInvocationSession retry = borrow();
        try {
          Object result = call.apply(retry);
          release(retry);
          retry = null;
          return result;
        } catch (Throwable retryError) {
          if (retry != null) {
            if (ThriftFailureClassifier.isTransportFailure(retryError)) {
              discard(retry, "transport_failure");
            } else if (ThriftFailureClassifier.isProtocolDesync(retryError)) {
              discard(retry, "protocol_desync");
            } else {
              release(retry);
            }
          }
          throw retryError;
        }
      } finally {
        if (session != null) {
          release(session);
        }
      }
    }

    // An eviction (TTL, capacity, adaptive-timeout reconnect) can retire this client after a caller
    // already resolved it but before it borrows. Such a caller is served from a fresh single-use
    // session instead of being failed: the drained idle deque cannot hand back a closed session,
    // and release() closes whatever an evicted client borrowed.
    private BackendInvocationSession borrow() throws MetaException {
      long timeoutMs = config.latencyBudgetMs() > 0L
          ? config.latencyBudgetMs()
          : DEFAULT_BORROW_TIMEOUT_MS;
      boolean acquired;
      try {
        acquired = permits.tryAcquire(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        MetaException me = new MetaException(
            "Interrupted while waiting for impersonation session for user '" + userName
                + "' in catalog '" + config.name() + "'");
        me.initCause(e);
        throw me;
      }
      if (!acquired) {
        LOG.warn("Backend catalog '{}' impersonation pool exhausted for user '{}': no permit within {} ms (poolSize={})",
            config.name(), userName, timeoutMs, maxSize);
        if (metrics != null) {
          metrics.recordImpersonationSessionAcquireTimeout(config.name());
        }
        throw new MetaException(
            "Timed out waiting for impersonation session for user '" + userName
                + "' in catalog '" + config.name() + "' after " + timeoutMs + " ms");
      }
      List<BackendInvocationSession> expired = null;
      PooledSession reuse;
      synchronized (lock) {
        reuse = pollFreshLocked();
        if (reuse == null && sessionIdleTtlMs > 0) {
          expired = drainExpiredLocked();
        }
      }
      if (expired != null) {
        for (BackendInvocationSession s : expired) {
          CatalogBackend.closeQuietly(s, "idle impersonation session for user '" + userName + "'");
          impersonationIdleSessions.decrementAndGet();
          if (metrics != null) {
            metrics.recordImpersonationSessionEviction(config.name(), "idle");
          }
        }
        publishImpersonationGauges();
      }
      if (reuse != null) {
        impersonationIdleSessions.decrementAndGet();
        impersonationActiveSessions.incrementAndGet();
        publishImpersonationGauges();
        return reuse.session;
      }
      try {
        BackendInvocationSession fresh = openSession();
        synchronized (lock) {
          totalSessions++;
        }
        impersonationActiveSessions.incrementAndGet();
        publishImpersonationGauges();
        return fresh;
      } catch (Throwable t) {
        permits.release();
        if (t instanceof MetaException me) {
          throw me;
        }
        MetaException me = new MetaException(
            "Unable to open impersonation session for user '" + userName + "' in catalog '"
                + config.name() + "'");
        me.initCause(t);
        throw me;
      }
    }

    private void release(BackendInvocationSession session) {
      boolean drop = false;
      synchronized (lock) {
        if (evicted) {
          drop = true;
          totalSessions--;
        } else {
          idle.addFirst(new PooledSession(session, System.currentTimeMillis()));
        }
      }
      impersonationActiveSessions.decrementAndGet();
      if (drop) {
        CatalogBackend.closeQuietly(session, "impersonation session (evicted) for user '" + userName + "'");
      } else {
        impersonationIdleSessions.incrementAndGet();
      }
      permits.release();
      publishImpersonationGauges();
    }

    private void discard(BackendInvocationSession session, String reason) {
      synchronized (lock) {
        totalSessions--;
      }
      impersonationActiveSessions.decrementAndGet();
      CatalogBackend.closeQuietly(session, "impersonation session (" + reason + ") for user '" + userName + "'");
      if (metrics != null) {
        metrics.recordImpersonationSessionEviction(config.name(), reason);
      }
      permits.release();
      publishImpersonationGauges();
    }

    private PooledSession pollFreshLocked() {
      while (!idle.isEmpty()) {
        PooledSession candidate = idle.pollFirst();
        if (sessionIdleTtlMs > 0
            && System.currentTimeMillis() - candidate.releasedAtMs > sessionIdleTtlMs) {
          totalSessions--;
          CatalogBackend.closeQuietly(
              candidate.session, "expired idle impersonation session for user '" + userName + "'");
          impersonationIdleSessions.decrementAndGet();
          if (metrics != null) {
            metrics.recordImpersonationSessionEviction(config.name(), "idle");
          }
          continue;
        }
        return candidate;
      }
      return null;
    }

    private List<BackendInvocationSession> drainExpiredLocked() {
      List<BackendInvocationSession> expired = new ArrayList<>();
      long now = System.currentTimeMillis();
      Iterator<PooledSession> it = idle.iterator();
      while (it.hasNext()) {
        PooledSession s = it.next();
        if (now - s.releasedAtMs > sessionIdleTtlMs) {
          it.remove();
          totalSessions--;
          expired.add(s.session);
        }
      }
      return expired;
    }

    private void evict() {
      List<PooledSession> drained;
      synchronized (lock) {
        evicted = true;
        drained = new ArrayList<>(idle);
        idle.clear();
        totalSessions -= drained.size();
      }
      for (PooledSession s : drained) {
        CatalogBackend.closeQuietly(
            s.session, "impersonation session (user_evicted) for user '" + userName + "'");
        impersonationIdleSessions.decrementAndGet();
        if (metrics != null) {
          metrics.recordImpersonationSessionEviction(config.name(), "user_evicted");
        }
      }
      publishImpersonationGauges();
    }

    private void closeQuietly() {
      try {
        evict();
      } catch (RuntimeException e) {
        LOG.warn("Failed to close cached impersonation client for user '{}' in catalog '{}'",
            userName, config.name(), e);
      }
    }

    private BackendInvocationSession openSession() throws MetaException {
      BackendInvocationSession s = runtime.openImpersonationSession(
          adapter.runtimeProfile(), userName, groupNames);
      LOG.debug("Opened impersonation session for user '{}' in catalog '{}'", userName, config.name());
      return s;
    }
  }

  private static final class PooledSession {
    final BackendInvocationSession session;
    final long releasedAtMs;

    PooledSession(BackendInvocationSession session, long releasedAtMs) {
      this.session = session;
      this.releasedAtMs = releasedAtMs;
    }
  }

  @FunctionalInterface
  private interface SessionCall {
    Object apply(BackendInvocationSession session) throws Throwable;
  }
}
