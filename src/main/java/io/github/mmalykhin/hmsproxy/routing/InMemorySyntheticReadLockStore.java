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
      long createdAtMs
  ) {
    long lockId = SyntheticReadLockManager.lockIdForSequence(nextSequence.getAndIncrement());
    SyntheticReadLockManager.SyntheticLockState state = new SyntheticReadLockManager.SyntheticLockState(
        lockId,
        txnId,
        catalogName,
        backendDbName,
        externalDbName,
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
    removeLock(locks.remove(lockId));
  }

  @Override
  public void releaseTxn(long txnId) {
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

  @Override
  public void cleanupExpiredLocks(long nowMs, long timeoutMs) {
    for (SyntheticReadLockManager.SyntheticLockState state : locks.values()) {
      if (state.isExpired(nowMs, timeoutMs)) {
        removeLock(state);
      }
    }
  }

  private void removeLock(SyntheticReadLockManager.SyntheticLockState state) {
    if (state == null || !locks.remove(state.lockId(), state) || state.txnId() <= 0) {
      return;
    }
    txnLocks.computeIfPresent(state.txnId(), (ignored, lockIds) -> {
      lockIds.remove(state.lockId());
      return lockIds.isEmpty() ? null : lockIds;
    });
  }
}
