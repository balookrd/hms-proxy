package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.util.TimeoutValueParser;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;

final class SyntheticReadLockManager implements AutoCloseable {
  static final String SYNTHETIC_BACKEND_NAME = "proxy-synthetic";
  static final long SYNTHETIC_LOCK_ID_FLOOR = Long.MAX_VALUE / 2;
  private static final String ALL_CATALOGS = "all";

  private static final Logger LOG = LoggerFactory.getLogger(SyntheticReadLockManager.class);
  private static final long DEFAULT_TXN_TIMEOUT_MS = 300_000L;
  private static final long CLEANUP_INTERVAL_MS = 30_000L;

  private final String defaultCatalog;
  private final long timeoutMs;
  private final PrometheusMetrics metrics;
  private final SyntheticReadLockStore store;
  private final String storeMode;
  private final String instanceId;
  private final AtomicLong activeLocks = new AtomicLong();
  private final ScheduledExecutorService sweepExecutor;

  SyntheticReadLockManager(ProxyConfig config, PrometheusMetrics metrics) {
    this.defaultCatalog = config.defaultCatalog();
    this.timeoutMs = parseTimeoutMs(config);
    this.metrics = metrics;
    this.storeMode = config.syntheticReadLockStore().mode().name().toLowerCase(Locale.ROOT);
    this.instanceId = UUID.randomUUID().toString();
    metrics.setSyntheticReadLockStoreMode(storeMode);
    metrics.setSyntheticReadLocksActive(storeMode, 0L);
    this.store = openStore(config);
    publishInitialActiveLockCount();
    this.sweepExecutor = Executors.newSingleThreadScheduledExecutor(
        namedThreadFactory("hms-proxy-synthetic-lock-sweep"));
    // Expiry cleanup runs off the request path: a client RPC must never pay for a full store scan.
    sweepExecutor.scheduleWithFixedDelay(
        this::runExpiredLockSweep,
        CLEANUP_INTERVAL_MS,
        CLEANUP_INTERVAL_MS,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Tells whether the request would be served by the synthetic shim, without touching the state
   * store. Callers use it to run request admission (rate limiting) before {@link #tryAcquire}
   * persists lock state that a rejected client could never release. Expired locks are reclaimed by
   * the background sweep, so this stays a local decision.
   */
  boolean isSyntheticReadLockCandidate(LockRequest request, CatalogRouter.ResolvedNamespace namespace)
      throws MetaException {
    return isEligibleSyntheticReadLock(request, namespace);
  }


  SyntheticLockState tryAcquire(LockRequest request, CatalogRouter.ResolvedNamespace namespace) throws MetaException {
    if (!isEligibleSyntheticReadLock(request, namespace)) {
      return null;
    }

    long now = System.currentTimeMillis();
    SyntheticLockState state = runWithStorage(
        "create synthetic read lock",
        "acquire",
        namespace.catalogName(),
        () -> store.create(
            request.isSetTxnid() ? request.getTxnid() : 0L,
            namespace.catalogName(),
            namespace.backendDbName(),
            namespace.externalDbName(),
            instanceId,
            now));
    metrics.recordSyntheticReadLockEvent("acquire", state.catalogName(), storeMode, "acquired");
    adjustActiveLockGauge(1L);
    return state;
  }

  SyntheticLockState syntheticLockForCheck(CheckLockRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid(), "check_lock");
  }

  SyntheticLockState syntheticLockForUnlock(UnlockRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid(), "unlock");
  }

  SyntheticLockState syntheticLockForHeartbeat(HeartbeatRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid(), "heartbeat");
  }

  LockResponse acquiredResponse(long lockId) {
    return new LockResponse(lockId, LockState.ACQUIRED);
  }

  void releaseLock(SyntheticLockState state) throws MetaException {
    if (state == null) {
      return;
    }
    runWithStorage("release synthetic read lock", "unlock", state.catalogName(), () -> {
      store.releaseLock(state);
      return null;
    });
    metrics.recordSyntheticReadLockEvent("unlock", state.catalogName(), storeMode, "released");
    adjustActiveLockGauge(-1L);
  }

  void releaseTxn(long txnId) throws MetaException {
    if (txnId <= 0) {
      return;
    }
    SyntheticReadLockStore.ReleaseSummary summary = runWithStorage(
        "release synthetic read locks for txn " + txnId,
        "release_txn",
        ALL_CATALOGS,
        () -> store.releaseTxn(txnId, instanceId));
    if (summary.releasedCount() > 0) {
      metrics.recordSyntheticReadLockEvent(
          "release_txn",
          ALL_CATALOGS,
          storeMode,
          "released",
          summary.releasedCount());
      adjustActiveLockGauge(-summary.releasedCount());
    }
    if (summary.remoteOwnerCount() > 0) {
      metrics.recordSyntheticReadLockHandoff(
          "release_txn",
          ALL_CATALOGS,
          storeMode,
          summary.remoteOwnerCount());
    }
  }

  void touch(SyntheticLockState state) throws MetaException {
    if (state == null) {
      return;
    }
    long now = System.currentTimeMillis();
    runWithStorage("heartbeat synthetic read lock", "heartbeat", state.catalogName(), () -> {
      store.touch(state.lockId(), now);
      return null;
    });
    metrics.recordSyntheticReadLockEvent("heartbeat", state.catalogName(), storeMode, "touched");
  }

  void recordHeartbeatForwarded(SyntheticLockState state) {
    if (state != null) {
      metrics.recordSyntheticReadLockEvent("heartbeat", state.catalogName(), storeMode, "txn_forwarded");
    }
  }

  void recordHeartbeatWithoutTxn(SyntheticLockState state) {
    if (state != null) {
      metrics.recordSyntheticReadLockEvent("heartbeat", state.catalogName(), storeMode, "no_txn");
    }
  }

  HeartbeatRequest txnOnlyHeartbeat(HeartbeatRequest request) {
    if (request == null || !request.isSetTxnid() || request.getTxnid() <= 0) {
      return null;
    }
    HeartbeatRequest forwarded = new HeartbeatRequest();
    forwarded.setTxnid(request.getTxnid());
    return forwarded;
  }

  /** Package-private for tests: the effective synthetic lock timeout derived from Hive config. */
  long timeoutMs() {
    return timeoutMs;
  }

  boolean isSyntheticLockId(long lockId) {
    return lockId > SYNTHETIC_LOCK_ID_FLOOR;
  }

  @Override
  public void close() throws MetaException {
    sweepExecutor.shutdownNow();
    try {
      if (!sweepExecutor.awaitTermination(5L, TimeUnit.SECONDS)) {
        LOG.warn("Executor 'synthetic-lock-sweep' did not terminate within 5s after shutdown");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    runWithStorage("close synthetic read-lock store", "close", ALL_CATALOGS, () -> {
      store.close();
      return null;
    });
  }

  static long lockIdForSequence(long sequence) {
    return SYNTHETIC_LOCK_ID_FLOOR + sequence + 1;
  }

  static long sequenceForLockId(long lockId) {
    long sequence = lockId - SYNTHETIC_LOCK_ID_FLOOR - 1;
    if (sequence < 0) {
      throw new IllegalArgumentException("Synthetic read lock id is out of range: " + lockId);
    }
    return sequence;
  }

  private SyntheticLockState syntheticLock(long lockId, String operation) throws MetaException, NoSuchLockException {
    if (!isSyntheticLockId(lockId)) {
      return null;
    }
    SyntheticLockState observed = runWithStorage(
        "lookup synthetic read lock",
        operation,
        null,
        () -> store.get(lockId));
    if (observed == null) {
      metrics.recordSyntheticReadLockEvent(operation, null, storeMode, "miss");
      throw noSuchLock(lockId);
    }
    SyntheticLockState state = observed;
    long now = System.currentTimeMillis();
    if (observed.isExpired(now, timeoutMs)) {
      // Conditional delete: a heartbeat that renewed the lock between our read and this call must
      // not lose its lock, so the store returns the surviving state instead of removing it.
      SyntheticLockState refreshed = runWithStorage(
          "expire synthetic read lock",
          operation,
          observed.catalogName(),
          () -> store.releaseIfExpired(lockId, now, timeoutMs));
      if (refreshed == null) {
        metrics.recordSyntheticReadLockEvent(operation, observed.catalogName(), storeMode, "expired");
        adjustActiveLockGauge(-1L);
        throw noSuchLock(lockId);
      }
      state = refreshed;
    }
    recordHandoffIfNeeded(operation, state);
    if ("check_lock".equals(operation)) {
      metrics.recordSyntheticReadLockEvent(operation, state.catalogName(), storeMode, "hit");
    }
    return state;
  }

  private SyntheticReadLockStore openStore(ProxyConfig config) {
    try {
      if (config.syntheticReadLockStore().zooKeeperEnabled()) {
        return new ZooKeeperSyntheticReadLockStore(config);
      }
      LOG.warn("Synthetic read-lock state store is using in-memory mode. "
              + "Non-default catalog SELECT locks will not survive proxy restarts or load-balancer failover. "
              + "Set synthetic-read-lock.store.mode=ZOOKEEPER for multi-instance deployments.");
      return new InMemorySyntheticReadLockStore();
    } catch (Exception e) {
      metrics.recordSyntheticReadLockStoreFailure("init", storeMode, e);
      throw new IllegalStateException(
          "Unable to initialize synthetic read-lock store: "
              + e.getClass().getSimpleName()
              + (e.getMessage() == null ? "" : " - " + e.getMessage()),
          e);
    }
  }

  private boolean isEligibleSyntheticReadLock(LockRequest request, CatalogRouter.ResolvedNamespace namespace) {
    if (request == null || namespace == null || namespace.catalogName().equals(defaultCatalog)) {
      return false;
    }
    if (request.getComponent() == null || request.getComponent().isEmpty()) {
      return false;
    }
    boolean sawRealComponent = false;
    for (LockComponent component : request.getComponent()) {
      // Hive locks _dummy_database._dummy_table alongside the target table of an INSERT ... VALUES.
      // It exists in no metastore, so it neither routes nor needs a lock, and its lock type says
      // nothing about what the statement does to the real tables of the request.
      if (component != null && HivePlaceholderNamespace.isPlaceholderDbName(component.getDbname())) {
        continue;
      }
      if (!isEligibleSyntheticReadLock(component)) {
        return false;
      }
      sawRealComponent = true;
    }
    return sawRealComponent;
  }

  private boolean isEligibleSyntheticReadLock(LockComponent component) {
    if (component == null || !component.isSetOperationType()) {
      return false;
    }
    if (component.getOperationType() == DataOperationType.SELECT) {
      if (component.getType() != LockType.SHARED_READ) {
        return false;
      }
      return component.isSetIsTransactional() && !component.isIsTransactional();
    }
    if (component.getOperationType() == DataOperationType.NO_TXN) {
      // Hive can issue non-transactional DDL locks (for example CREATE TABLE or partition
      // rename/drop flows) before the actual catalog-scoped write request. These locks still
      // carry the default-backend txn id even though the namespace resolves to another catalog.
      // The lock type is deliberately not restricted here: check_lock/unlock/heartbeat are pinned
      // to the default backend, so a non-default-catalog EXCLUSIVE or SHARED_WRITE lock cannot be
      // forwarded to its owning metastore without stranding the returned lock id. Components of
      // the default catalog never reach this path - LockHandler resolves the namespace over all
      // components and rejects requests that mix namespaces.
      return !component.isSetIsTransactional() || !component.isIsTransactional();
    }
    if (isWriteOperation(component.getOperationType())) {
      // Writes into a non-default catalog are always non-transactional: the proxy refuses to create
      // transactional tables there and refuses allocate_table_write_ids/get_valid_write_ids for
      // non-default catalogs, so an ACID table cannot exist behind this namespace. A component that
      // still claims isTransactional=true is left to the backend, which fails it deterministically
      // rather than being handed a lock whose write ids are unreachable. The lock type is not
      // restricted for the same reason as NO_TXN above - and Hive takes EXCLUSIVE, not SHARED_WRITE,
      // for an INSERT into a non-ACID table under the default hive.txn.strict.locking.mode.
      return !component.isSetIsTransactional() || !component.isIsTransactional();
    }
    return false;
  }

  /** Package-private for the lock handler: write components need the catalog access-mode check. */
  static boolean isWriteOperation(DataOperationType operationType) {
    return operationType == DataOperationType.INSERT
        || operationType == DataOperationType.UPDATE
        || operationType == DataOperationType.DELETE;
  }

  /** Package-private so tests can drive one sweep without waiting for the scheduler. */
  void runExpiredLockSweep() {
    long now = System.currentTimeMillis();
    SyntheticReadLockStore.CleanupSummary summary;
    try {
      summary = store.cleanupExpiredLocks(now, timeoutMs, instanceId);
    } catch (Exception e) {
      if (sweepExecutor.isShutdown()) {
        // Interrupted mid-sweep by close(); not a store failure worth alerting on.
        LOG.debug("Synthetic read-lock expiry sweep interrupted during shutdown", e);
        return;
      }
      metrics.recordSyntheticReadLockStoreFailure("cleanup", storeMode, e);
      LOG.warn("Synthetic read-lock expiry sweep failed for store mode {}", storeMode, e);
      return;
    }
    if (summary.expiredCount() > 0) {
      metrics.recordSyntheticReadLockEvent(
          "cleanup",
          ALL_CATALOGS,
          storeMode,
          "expired",
          summary.expiredCount());
    }
    if (summary.remoteOwnerCount() > 0) {
      metrics.recordSyntheticReadLockHandoff(
          "cleanup",
          ALL_CATALOGS,
          storeMode,
          summary.remoteOwnerCount());
    }
    // The sweep already walked the store, so this is the authoritative count that corrects any
    // drift the per-request deltas accumulated (including locks other instances released).
    setActiveLockGauge(summary.activeCount());
  }

  private void publishInitialActiveLockCount() {
    try {
      setActiveLockGauge(store.activeLockCount());
    } catch (Exception e) {
      metrics.recordSyntheticReadLockStoreFailure("active_count", storeMode, e);
      LOG.debug("Unable to publish initial synthetic read-lock active gauge for store mode {}", storeMode, e);
    }
  }

  private void adjustActiveLockGauge(long delta) {
    metrics.setSyntheticReadLocksActive(
        storeMode,
        activeLocks.updateAndGet(current -> Math.max(0L, current + delta)));
  }

  private void setActiveLockGauge(long activeLockCount) {
    long normalized = Math.max(0L, activeLockCount);
    activeLocks.set(normalized);
    metrics.setSyntheticReadLocksActive(storeMode, normalized);
  }

  private long parseTimeoutMs(ProxyConfig config) {
    CatalogConfig defaultCatalogConfig = config.catalogs().get(config.defaultCatalog());
    String configuredTimeout = defaultCatalogConfig == null
        ? null
        : defaultCatalogConfig.hiveConf().get("metastore.txn.timeout");
    if (configuredTimeout == null || configuredTimeout.isBlank()) {
      return DEFAULT_TXN_TIMEOUT_MS;
    }
    // Hive writes this value with a unit suffix ("300s"); a bare number still means seconds.
    long parsedMs = TimeoutValueParser.parseDurationMs(configuredTimeout, -1L);
    if (parsedMs <= 0L) {
      LOG.warn("Unrecognized metastore.txn.timeout value '{}' for default catalog {}; "
              + "synthetic read locks fall back to {} ms",
          configuredTimeout, config.defaultCatalog(), DEFAULT_TXN_TIMEOUT_MS);
      return DEFAULT_TXN_TIMEOUT_MS;
    }
    return Math.max(1000L, parsedMs);
  }

  private void recordHandoffIfNeeded(String operation, SyntheticLockState state) {
    if (state != null && !state.ownerInstanceId().equals(instanceId)) {
      metrics.recordSyntheticReadLockHandoff(operation, state.catalogName(), storeMode);
    }
  }

  private static ThreadFactory namedThreadFactory(String prefix) {
    return runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName(prefix + "-" + thread.getId());
      thread.setDaemon(true);
      return thread;
    };
  }

  private NoSuchLockException noSuchLock(long lockId) {
    return new NoSuchLockException("Synthetic read lock " + lockId + " does not exist");
  }

  private MetaException storageFailure(String action, Exception error) {
    MetaException metaException = new MetaException(
        "Synthetic read-lock store failed to " + action + ": "
            + error.getClass().getSimpleName()
            + (error.getMessage() == null ? "" : " - " + error.getMessage()));
    metaException.initCause(error);
    return metaException;
  }

  private <T> T runWithStorage(String action, String operation, String catalog, StorageCall<T> call) throws MetaException {
    try {
      return call.run();
    } catch (MetaException e) {
      throw e;
    } catch (Exception e) {
      metrics.recordSyntheticReadLockStoreFailure(operation, storeMode, e);
      throw storageFailure(action, e);
    }
  }

  @FunctionalInterface
  private interface StorageCall<T> {
    T run() throws Exception;
  }

  static record SyntheticLockState(
      long lockId,
      long txnId,
      String catalogName,
      String backendDbName,
      String externalDbName,
      String ownerInstanceId,
      long lastTouchedAtMs
  ) {
    CatalogRouter.ResolvedNamespace namespace(CatalogRouter router) {
      return router.resolveCatalog(catalogName, backendDbName);
    }

    SyntheticLockState touched(long nowMs) {
      return new SyntheticLockState(
          lockId,
          txnId,
          catalogName,
          backendDbName,
          externalDbName,
          ownerInstanceId,
          Math.max(lastTouchedAtMs, nowMs));
    }

    SyntheticLockState withLockId(long updatedLockId) {
      return new SyntheticLockState(
          updatedLockId,
          txnId,
          catalogName,
          backendDbName,
          externalDbName,
          ownerInstanceId,
          lastTouchedAtMs);
    }

    boolean isExpired(long nowMs, long timeoutMs) {
      return nowMs - lastTouchedAtMs > timeoutMs;
    }
  }
}
