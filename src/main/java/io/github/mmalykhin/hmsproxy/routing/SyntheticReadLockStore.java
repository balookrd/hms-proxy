package io.github.mmalykhin.hmsproxy.routing;

interface SyntheticReadLockStore extends AutoCloseable {
  SyntheticReadLockManager.SyntheticLockState create(
      long txnId,
      String catalogName,
      String backendDbName,
      String externalDbName,
      String ownerInstanceId,
      long createdAtMs
  ) throws Exception;

  SyntheticReadLockManager.SyntheticLockState get(long lockId) throws Exception;

  void touch(long lockId, long nowMs) throws Exception;

  void releaseLock(SyntheticReadLockManager.SyntheticLockState state) throws Exception;

  /**
   * Removes the lock only while it is still the expired state the caller observed. Returns the
   * lock state that survived a concurrent heartbeat, or {@code null} when the lock is gone.
   */
  SyntheticReadLockManager.SyntheticLockState releaseIfExpired(long lockId, long nowMs, long timeoutMs)
      throws Exception;

  ReleaseSummary releaseTxn(long txnId, String currentInstanceId) throws Exception;

  CleanupSummary cleanupExpiredLocks(long nowMs, long timeoutMs, String currentInstanceId) throws Exception;

  long activeLockCount() throws Exception;

  @Override
  default void close() throws Exception {
  }

  record ReleaseSummary(
      long releasedCount,
      long remoteOwnerCount
  ) {
  }

  /**
   * @param activeCount locks still alive once the sweep finished; the manager republishes it as the
   *                    active-lock gauge so hot paths never have to list the store.
   */
  record CleanupSummary(
      long expiredCount,
      long remoteOwnerCount,
      long activeCount
  ) {
  }
}
