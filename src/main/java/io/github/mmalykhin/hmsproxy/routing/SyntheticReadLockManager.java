package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import java.util.Locale;
import java.util.UUID;
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

final class SyntheticReadLockManager implements AutoCloseable {
  static final String SYNTHETIC_BACKEND_NAME = "proxy-synthetic";
  static final long SYNTHETIC_LOCK_ID_FLOOR = Long.MAX_VALUE / 2;
  private static final String ALL_CATALOGS = "all";

  private static final Logger LOG = LoggerFactory.getLogger(SyntheticReadLockManager.class);
  private static final long DEFAULT_TXN_TIMEOUT_SECONDS = 300L;
  private static final long CLEANUP_INTERVAL_MS = 30_000L;

  private final String defaultCatalog;
  private final long timeoutMs;
  private final PrometheusMetrics metrics;
  private final SyntheticReadLockStore store;
  private final String storeMode;
  private final String instanceId;
  private final AtomicLong lastCleanupAtMs = new AtomicLong();

  SyntheticReadLockManager(ProxyConfig config, PrometheusMetrics metrics) {
    this.defaultCatalog = config.defaultCatalog();
    this.timeoutMs = parseTimeoutMs(config);
    this.metrics = metrics;
    this.storeMode = config.syntheticReadLockStore().mode().name().toLowerCase(Locale.ROOT);
    this.instanceId = UUID.randomUUID().toString();
    metrics.setSyntheticReadLockStoreMode(storeMode);
    metrics.setSyntheticReadLocksActive(storeMode, 0L);
    this.store = openStore(config);
    syncActiveLockGauge();
  }

  SyntheticLockState tryAcquire(LockRequest request, CatalogRouter.ResolvedNamespace namespace) throws MetaException {
    cleanupExpiredLocks();
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
    syncActiveLockGauge();
    return state;
  }

  SyntheticLockState syntheticLock(CheckLockRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid(), "check_lock");
  }

  SyntheticLockState syntheticLock(UnlockRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid(), "unlock");
  }

  SyntheticLockState syntheticLock(HeartbeatRequest request) throws MetaException, NoSuchLockException {
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
      store.releaseLock(state.lockId());
      return null;
    });
    metrics.recordSyntheticReadLockEvent("unlock", state.catalogName(), storeMode, "released");
    syncActiveLockGauge();
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
      syncActiveLockGauge();
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

  boolean isSyntheticLockId(long lockId) {
    return lockId > SYNTHETIC_LOCK_ID_FLOOR;
  }

  @Override
  public void close() throws MetaException {
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
    cleanupExpiredLocks();
    if (!isSyntheticLockId(lockId)) {
      return null;
    }
    SyntheticLockState state = runWithStorage(
        "lookup synthetic read lock",
        operation,
        null,
        () -> store.get(lockId));
    if (state == null) {
      metrics.recordSyntheticReadLockEvent(operation, null, storeMode, "miss");
      throw noSuchLock(lockId);
    }
    if (state.isExpired(System.currentTimeMillis(), timeoutMs)) {
      runWithStorage("expire synthetic read lock", operation, state.catalogName(), () -> {
        store.releaseLock(lockId);
        return null;
      });
      metrics.recordSyntheticReadLockEvent(operation, state.catalogName(), storeMode, "expired");
      syncActiveLockGauge();
      throw noSuchLock(lockId);
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
    for (LockComponent component : request.getComponent()) {
      if (!isEligibleSyntheticReadLock(component)) {
        return false;
      }
    }
    return true;
  }

  private boolean isEligibleSyntheticReadLock(LockComponent component) {
    if (component == null || component.getType() != LockType.SHARED_READ || !component.isSetOperationType()) {
      return false;
    }
    if (component.getOperationType() == DataOperationType.SELECT) {
      return component.isSetIsTransactional() && !component.isIsTransactional();
    }
    if (component.getOperationType() == DataOperationType.NO_TXN) {
      // Hive can issue DB-level SHARED_READ locks for non-transactional DDL before the
      // actual catalog-scoped write request. Those locks still carry the default-backend txn id.
      return !component.isSetIsTransactional() || !component.isIsTransactional();
    }
    return false;
  }

  private void cleanupExpiredLocks() throws MetaException {
    long now = System.currentTimeMillis();
    long previousCleanupAt = lastCleanupAtMs.get();
    if (now - previousCleanupAt < CLEANUP_INTERVAL_MS) {
      return;
    }
    if (!lastCleanupAtMs.compareAndSet(previousCleanupAt, now)) {
      return;
    }
    SyntheticReadLockStore.CleanupSummary summary = runWithStorage(
        "cleanup synthetic read locks",
        "cleanup",
        ALL_CATALOGS,
        () -> store.cleanupExpiredLocks(now, timeoutMs, instanceId));
    if (summary.expiredCount() > 0) {
      metrics.recordSyntheticReadLockEvent(
          "cleanup",
          ALL_CATALOGS,
          storeMode,
          "expired",
          summary.expiredCount());
      syncActiveLockGauge();
    }
    if (summary.remoteOwnerCount() > 0) {
      metrics.recordSyntheticReadLockHandoff(
          "cleanup",
          ALL_CATALOGS,
          storeMode,
          summary.remoteOwnerCount());
    }
  }

  private void syncActiveLockGauge() {
    try {
      metrics.setSyntheticReadLocksActive(storeMode, store.activeLockCount());
    } catch (Exception e) {
      metrics.recordSyntheticReadLockStoreFailure("active_count", storeMode, e);
      LOG.debug("Unable to refresh synthetic read-lock active gauge for store mode {}", storeMode, e);
    }
  }

  private long parseTimeoutMs(ProxyConfig config) {
    ProxyConfig.CatalogConfig defaultCatalogConfig = config.catalogs().get(config.defaultCatalog());
    String configuredTimeout = defaultCatalogConfig == null ? null : defaultCatalogConfig.hiveConf().get("metastore.txn.timeout");
    long timeoutSeconds = DEFAULT_TXN_TIMEOUT_SECONDS;
    if (configuredTimeout != null && !configuredTimeout.isBlank()) {
      try {
        timeoutSeconds = Long.parseLong(configuredTimeout.trim());
      } catch (NumberFormatException ignored) {
        timeoutSeconds = DEFAULT_TXN_TIMEOUT_SECONDS;
      }
    }
    return Math.max(1L, timeoutSeconds) * 1000L;
  }

  private void recordHandoffIfNeeded(String operation, SyntheticLockState state) {
    if (state != null && !state.ownerInstanceId().equals(instanceId)) {
      metrics.recordSyntheticReadLockHandoff(operation, state.catalogName(), storeMode);
    }
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
