package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;

final class SyntheticReadLockManager {
  static final String SYNTHETIC_BACKEND_NAME = "proxy-synthetic";
  private static final long DEFAULT_TXN_TIMEOUT_SECONDS = 300L;
  private static final long SYNTHETIC_LOCK_ID_FLOOR = Long.MAX_VALUE / 2;
  private static final long CLEANUP_INTERVAL_MS = 30_000L;

  private final String defaultCatalog;
  private final long timeoutMs;
  private final AtomicLong nextLockId = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong lastCleanupAtMs = new AtomicLong();
  private final ConcurrentMap<Long, SyntheticLockState> locks = new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, Set<Long>> txnLocks = new ConcurrentHashMap<>();

  SyntheticReadLockManager(ProxyConfig config) {
    this.defaultCatalog = config.defaultCatalog();
    this.timeoutMs = parseTimeoutMs(config);
  }

  SyntheticLockState tryAcquire(LockRequest request, CatalogRouter.ResolvedNamespace namespace) {
    cleanupExpiredLocks();
    if (!isEligibleSyntheticReadLock(request, namespace)) {
      return null;
    }

    long now = System.currentTimeMillis();
    long lockId = nextLockId.getAndDecrement();
    SyntheticLockState state = new SyntheticLockState(
        lockId,
        request.isSetTxnid() ? request.getTxnid() : 0L,
        namespace.catalogName(),
        namespace.backendDbName(),
        namespace.externalDbName(),
        now);
    locks.put(lockId, state);
    if (state.txnId() > 0) {
      txnLocks.computeIfAbsent(state.txnId(), ignored -> ConcurrentHashMap.newKeySet()).add(lockId);
    }
    return state;
  }

  SyntheticLockState syntheticLock(CheckLockRequest request) throws NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid());
  }

  SyntheticLockState syntheticLock(UnlockRequest request) throws NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid());
  }

  SyntheticLockState syntheticLock(HeartbeatRequest request) throws NoSuchLockException {
    return syntheticLock(request == null ? 0L : request.getLockid());
  }

  SyntheticLockState syntheticLock(long lockId) throws NoSuchLockException {
    cleanupExpiredLocks();
    if (!isSyntheticLockId(lockId)) {
      return null;
    }
    SyntheticLockState state = locks.get(lockId);
    if (state == null) {
      throw noSuchLock(lockId);
    }
    if (state.isExpired(System.currentTimeMillis(), timeoutMs)) {
      removeLock(state);
      throw noSuchLock(lockId);
    }
    return state;
  }

  LockResponse acquiredResponse(long lockId) {
    return new LockResponse(lockId, LockState.ACQUIRED);
  }

  void releaseLock(long lockId) {
    SyntheticLockState state = locks.remove(lockId);
    if (state == null || state.txnId() <= 0) {
      return;
    }
    txnLocks.computeIfPresent(state.txnId(), (ignored, lockIds) -> {
      lockIds.remove(lockId);
      return lockIds.isEmpty() ? null : lockIds;
    });
  }

  void releaseTxn(long txnId) {
    if (txnId <= 0) {
      return;
    }
    Set<Long> lockIds = txnLocks.remove(txnId);
    if (lockIds == null) {
      return;
    }
    for (Long lockId : lockIds) {
      locks.remove(lockId);
    }
  }

  void touch(SyntheticLockState state) {
    if (state != null) {
      state.touch(System.currentTimeMillis());
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
    return lockId >= SYNTHETIC_LOCK_ID_FLOOR;
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

  private void cleanupExpiredLocks() {
    long now = System.currentTimeMillis();
    long previousCleanupAt = lastCleanupAtMs.get();
    if (now - previousCleanupAt < CLEANUP_INTERVAL_MS
        && (previousCleanupAt != 0 || !locks.isEmpty())) {
      return;
    }
    if (!lastCleanupAtMs.compareAndSet(previousCleanupAt, now)) {
      return;
    }
    for (SyntheticLockState state : locks.values()) {
      if (state.isExpired(now, timeoutMs)) {
        removeLock(state);
      }
    }
  }

  private void removeLock(SyntheticLockState state) {
    if (state == null || !locks.remove(state.lockId(), state) || state.txnId() <= 0) {
      return;
    }
    txnLocks.computeIfPresent(state.txnId(), (ignored, lockIds) -> {
      lockIds.remove(state.lockId());
      return lockIds.isEmpty() ? null : lockIds;
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

  static final class SyntheticLockState {
    private final long lockId;
    private final long txnId;
    private final String catalogName;
    private final String backendDbName;
    private final String externalDbName;
    private volatile long lastTouchedAtMs;

    private SyntheticLockState(
        long lockId,
        long txnId,
        String catalogName,
        String backendDbName,
        String externalDbName,
        long createdAtMs
    ) {
      this.lockId = lockId;
      this.txnId = txnId;
      this.catalogName = catalogName;
      this.backendDbName = backendDbName;
      this.externalDbName = externalDbName;
      this.lastTouchedAtMs = createdAtMs;
    }

    long lockId() {
      return lockId;
    }

    long txnId() {
      return txnId;
    }

    String externalDbName() {
      return externalDbName;
    }

    CatalogRouter.ResolvedNamespace namespace(CatalogRouter router) {
      return router.resolveCatalog(catalogName, backendDbName);
    }

    void touch(long nowMs) {
      lastTouchedAtMs = nowMs;
    }

    boolean isExpired(long nowMs, long timeoutMs) {
      return nowMs - lastTouchedAtMs > timeoutMs;
    }
  }
}
