package io.github.mmalykhin.hmsproxy.routing;

interface SyntheticReadLockStore extends AutoCloseable {
  SyntheticReadLockManager.SyntheticLockState create(
      long txnId,
      String catalogName,
      String backendDbName,
      String externalDbName,
      long createdAtMs
  ) throws Exception;

  SyntheticReadLockManager.SyntheticLockState get(long lockId) throws Exception;

  void touch(long lockId, long nowMs) throws Exception;

  void releaseLock(long lockId) throws Exception;

  void releaseTxn(long txnId) throws Exception;

  void cleanupExpiredLocks(long nowMs, long timeoutMs) throws Exception;

  @Override
  default void close() throws Exception {
  }
}
