package io.github.mmalykhin.hmsproxy.routing;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

final class InMemorySyntheticReadLockStore implements SyntheticReadLockStore {
  private final AtomicLong nextSequence = new AtomicLong();
  private final ConcurrentMap<Long, SyntheticReadLockManager.SyntheticLockState> locks = new ConcurrentHashMap<>();
  private final ConcurrentMap<Long, Set<Long>> txnLocks = new ConcurrentHashMap<>();

  @Override
  public SyntheticReadLockManager.SyntheticLockState create(
      long txnId,
      String catalogName,
      String backendDbName,
      String externalDbName,
      String ownerInstanceId,
      long createdAtMs
  ) {
    long lockId = SyntheticReadLockManager.lockIdForSequence(nextSequence.getAndIncrement());
    SyntheticReadLockManager.SyntheticLockState state = new SyntheticReadLockManager.SyntheticLockState(
        lockId,
        txnId,
        catalogName,
        backendDbName,
        externalDbName,
        ownerInstanceId,
        createdAtMs);
    locks.put(lockId, state);
    if (txnId > 0) {
      txnLocks.computeIfAbsent(txnId, ignored -> ConcurrentHashMap.newKeySet()).add(lockId);
    }
    return state;
  }

  @Override
  public SyntheticReadLockManager.SyntheticLockState get(long lockId) {
    return locks.get(lockId);
  }

  @Override
  public void touch(long lockId, long nowMs) {
    locks.computeIfPresent(lockId, (ignored, state) -> state.touched(nowMs));
  }

  @Override
  public void releaseLock(long lockId) {
    SyntheticReadLockManager.SyntheticLockState removed = locks.remove(lockId);
    if (removed != null) {
      removeTxnRef(removed);
    }
  }

  @Override
  public ReleaseSummary releaseTxn(long txnId, String currentInstanceId) {
    if (txnId <= 0) {
      return new ReleaseSummary(0L, 0L);
    }
    Set<Long> lockIds = txnLocks.remove(txnId);
    if (lockIds == null) {
      return new ReleaseSummary(0L, 0L);
    }
    long releasedCount = 0L;
    long remoteOwnerCount = 0L;
    for (Long lockId : lockIds) {
      SyntheticReadLockManager.SyntheticLockState state = locks.remove(lockId);
      if (state == null) {
        continue;
      }
      releasedCount++;
      if (!state.ownerInstanceId().equals(currentInstanceId)) {
        remoteOwnerCount++;
      }
    }
    return new ReleaseSummary(releasedCount, remoteOwnerCount);
  }

  @Override
  public CleanupSummary cleanupExpiredLocks(long nowMs, long timeoutMs, String currentInstanceId) {
    long expiredCount = 0L;
    long remoteOwnerCount = 0L;
    for (SyntheticReadLockManager.SyntheticLockState state : locks.values()) {
      if (state.isExpired(nowMs, timeoutMs)) {
        SyntheticReadLockManager.SyntheticLockState removed = removeLock(state);
        if (removed != null) {
          expiredCount++;
          if (!removed.ownerInstanceId().equals(currentInstanceId)) {
            remoteOwnerCount++;
          }
        }
      }
    }
    return new CleanupSummary(expiredCount, remoteOwnerCount);
  }

  @Override
  public long activeLockCount() {
    return locks.size();
  }

  private SyntheticReadLockManager.SyntheticLockState removeLock(SyntheticReadLockManager.SyntheticLockState state) {
    if (state == null || !locks.remove(state.lockId(), state)) {
      return null;
    }
    removeTxnRef(state);
    return state;
  }

  private void removeTxnRef(SyntheticReadLockManager.SyntheticLockState state) {
    if (state.txnId() <= 0) {
      return;
    }
    txnLocks.computeIfPresent(state.txnId(), (ignored, lockIds) -> {
      lockIds.remove(state.lockId());
      return lockIds.isEmpty() ? null : lockIds;
    });
  }
}
