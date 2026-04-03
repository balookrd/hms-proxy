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

  void releaseLock(long lockId) throws Exception;

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

  record CleanupSummary(
      long expiredCount,
      long remoteOwnerCount
  ) {
  }
}
