package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
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

  private static final Logger LOG = LoggerFactory.getLogger(SyntheticReadLockManager.class);
  private static final long DEFAULT_TXN_TIMEOUT_SECONDS = 300L;
  private static final long CLEANUP_INTERVAL_MS = 30_000L;

  private final String defaultCatalog;
  private final long timeoutMs;
  private final SyntheticReadLockStore store;
  private final AtomicLong lastCleanupAtMs = new AtomicLong();

  SyntheticReadLockManager(ProxyConfig config) {
    this.defaultCatalog = config.defaultCatalog();
    this.timeoutMs = parseTimeoutMs(config);
    this.store = openStore(config);
  }

  SyntheticLockState tryAcquire(LockRequest request, CatalogRouter.ResolvedNamespace namespace) throws MetaException {
    cleanupExpiredLocks();
    if (!isEligibleSyntheticReadLock(request, namespace)) {
      return null;
    }

    long now = System.currentTimeMillis();
    return runWithStorage(
        "create synthetic read lock",
        () -> store.create(
            request.isSetTxnid() ? request.getTxnid() : 0L,
            namespace.catalogName(),
            namespace.backendDbName(),
            namespace.externalDbName(),
            now));
  }

  SyntheticLockState syntheticLock(CheckLockRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid());
  }

  SyntheticLockState syntheticLock(UnlockRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid());
  }

  SyntheticLockState syntheticLock(HeartbeatRequest request) throws MetaException, NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid());
  }

  SyntheticLockState syntheticLock(long lockId) throws MetaException, NoSuchLockException {
    cleanupExpiredLocks();
    if (!isSyntheticLockId(lockId)) {
      return null;
    }
    SyntheticLockState state = runWithStorage("lookup synthetic read lock", () -> store.get(lockId));
    if (state == null) {
      throw noSuchLock(lockId);
    }
    if (state.isExpired(System.currentTimeMillis(), timeoutMs)) {
      releaseLock(lockId);
      throw noSuchLock(lockId);
    }
    return state;
  }

  LockResponse acquiredResponse(long lockId) {
    return new LockResponse(lockId, LockState.ACQUIRED);
  }

  void releaseLock(long lockId) throws MetaException {
    runWithStorage("release synthetic read lock", () -> {
      store.releaseLock(lockId);
      return null;
    });
  }

  void releaseTxn(long txnId) throws MetaException {
    if (txnId <= 0) {
      return;
    }
    runWithStorage("release synthetic read locks for txn " + txnId, () -> {
      store.releaseTxn(txnId);
      return null;
    });
  }

  void touch(SyntheticLockState state) throws MetaException {
    if (state == null) {
      return;
    }
    long now = System.currentTimeMillis();
    runWithStorage("heartbeat synthetic read lock", () -> {
      store.touch(state.lockId(), now);
      return null;
    });
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
    runWithStorage("close synthetic read-lock store", () -> {
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
      if (component == null
          || component.getType() != LockType.SHARED_READ
          || !component.isSetOperationType()
          || component.getOperationType() != DataOperationType.SELECT
          || !component.isSetIsTransactional()
          || component.isIsTransactional()) {
        return false;
      }
    }
    return true;
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
    runWithStorage("cleanup synthetic read locks", () -> {
      store.cleanupExpiredLocks(now, timeoutMs);
      return null;
    });
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

  private <T> T runWithStorage(String action, StorageCall<T> call) throws MetaException {
    try {
      return call.run();
    } catch (MetaException e) {
      throw e;
    } catch (Exception e) {
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
          Math.max(lastTouchedAtMs, nowMs));
    }

    SyntheticLockState withLockId(long updatedLockId) {
      return new SyntheticLockState(
          updatedLockId,
          txnId,
          catalogName,
          backendDbName,
          externalDbName,
          lastTouchedAtMs);
    }

    boolean isExpired(long nowMs, long timeoutMs) {
      return nowMs - lastTouchedAtMs > timeoutMs;
    }
  }
}
