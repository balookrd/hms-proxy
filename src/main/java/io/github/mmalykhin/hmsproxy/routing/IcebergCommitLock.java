package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The table lock an Iceberg commit takes, requested by the proxy in exactly the shape Iceberg
 * requests it, so that both sides contend for the same object.
 *
 * <p>The shape is copied from {@code org.apache.iceberg.hive.MetastoreLock#createLock}, which is
 * identical in Iceberg 1.6.1 (the {@code iceberg-hive-runtime} inside HiveServer2) and 1.9.2 (the
 * proxy's own REST path): a single EXCLUSIVE, TABLE-level {@link LockComponent} naming the
 * database and the table, no {@code txnid} and no operation type. The database has to be the
 * backend one - {@link LockRequestSplit} rewrites the components of a client's lock request into
 * backend names before forwarding it, so anything else would name a different object and buy no
 * mutual exclusion at all.
 *
 * <p>Waiting is polled with {@code check_lock}, the way Iceberg polls it. The backoff bounds are
 * deliberately not configurable: they belong to the same timer as the caller's budget, and a
 * second knob could only contradict it.
 *
 * <p>Every call here bypasses admission control ({@link RoutingSupport#invokeDirectUnmetered}): a
 * rate-limit or circuit-breaker rejection landing between {@code lock} and {@code unlock} would
 * strand an EXCLUSIVE lock on the table. A standalone 3.1 metastore without the ACID housekeeping
 * service never reaps such a lock, and every later commit on that table blocks on it.
 */
final class IcebergCommitLock {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCommitLock.class);
  private static final long MIN_WAIT_MS = 50L;
  private static final long MAX_WAIT_MS = 500L;
  private static final Method LOCK = method("lock", LockRequest.class);
  private static final Method CHECK_LOCK = method("check_lock", CheckLockRequest.class);
  private static final Method UNLOCK = method("unlock", UnlockRequest.class);
  private static final String HOST_NAME = resolveHostName();

  private final RoutingSupport support;

  IcebergCommitLock(RoutingSupport support) {
    this.support = support;
  }

  enum Outcome {
    ACQUIRED,
    /** The lock stayed WAITING for the whole budget; it has been given back. */
    TIMED_OUT,
    /** The metastore refused the request or the call failed; nothing is held. */
    FAILED
  }

  /** A lock id is only meaningful when {@link #acquired()}. */
  record Attempt(long lockId, Outcome outcome) {
    boolean acquired() {
      return outcome == Outcome.ACQUIRED;
    }
  }

  /**
   * Requests the lock and waits at most {@code timeoutMs} for it, giving it back if the wait runs
   * out. Never throws: the caller has to be able to continue without the lock, because failing the
   * request it protects would break an ordinary Hive write whenever the lock table hiccups.
   */
  Attempt acquire(CatalogBackend backend, String backendDbName, String tableName, long timeoutMs) {
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    long lockId;
    LockState state;
    try {
      LockResponse response = (LockResponse) support.invokeDirectUnmetered(
          backend, LOCK, new Object[] {lockRequest(backendDbName, tableName)});
      if (response == null) {
        LOG.warn("requestId={} the metastore answered no lock response for '{}.{}'",
            RequestContext.currentRequestId(), backendDbName, tableName);
        return new Attempt(0L, Outcome.FAILED);
      }
      lockId = response.getLockid();
      state = response.getState();
    } catch (Throwable throwable) {
      LOG.warn("requestId={} could not lock '{}.{}', continuing without the lock: {}",
          RequestContext.currentRequestId(), backendDbName, tableName, throwable.toString());
      return new Attempt(0L, Outcome.FAILED);
    }

    long waitMs = MIN_WAIT_MS;
    while (state == LockState.WAITING) {
      long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
      if (remainingMs <= 0L) {
        release(backend, lockId);
        return new Attempt(lockId, Outcome.TIMED_OUT);
      }
      try {
        Thread.sleep(Math.min(waitMs, remainingMs));
        waitMs = Math.min(waitMs * 2L, MAX_WAIT_MS);
        LockResponse response = (LockResponse) support.invokeDirectUnmetered(
            backend, CHECK_LOCK, new Object[] {new CheckLockRequest(lockId)});
        state = response == null ? null : response.getState();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        release(backend, lockId);
        return new Attempt(lockId, Outcome.FAILED);
      } catch (Throwable throwable) {
        LOG.warn("requestId={} could not check the lock on '{}.{}', continuing without it: {}",
            RequestContext.currentRequestId(), backendDbName, tableName, throwable.toString());
        release(backend, lockId);
        return new Attempt(lockId, Outcome.FAILED);
      }
    }
    if (state == LockState.ACQUIRED) {
      return new Attempt(lockId, Outcome.ACQUIRED);
    }
    // NOT_ACQUIRED or ABORT: the metastore has decided, and the lock row may still exist.
    release(backend, lockId);
    return new Attempt(lockId, Outcome.FAILED);
  }

  /**
   * Gives the lock back, retrying once. Returns false only when the metastore may still hold it -
   * an ERROR worth alerting on, because nothing else will clear it.
   */
  boolean release(CatalogBackend backend, long lockId) {
    Throwable last = null;
    for (int attempt = 1; attempt <= 2; attempt++) {
      try {
        support.invokeDirectUnmetered(backend, UNLOCK, new Object[] {new UnlockRequest(lockId)});
        return true;
      } catch (Throwable throwable) {
        last = throwable;
      }
    }
    LOG.error(
        "requestId={} failed to release Iceberg table lock {} on catalog '{}' after two attempts;"
            + " commits on that table may block until the metastore expires it: {}",
        RequestContext.currentRequestId(), lockId, backend.name(), String.valueOf(last));
    return false;
  }

  private LockRequest lockRequest(String backendDbName, String tableName) {
    LockComponent component = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, backendDbName);
    component.setTablename(tableName);
    LockRequest request = new LockRequest(List.of(component), lockUser(), HOST_NAME);
    // agentInfo is what show_locks displays: a stranded lock has to name what took it.
    request.setAgentInfo("hms-proxy-iceberg-pointer-guard-" + RequestContext.currentRequestId());
    return request;
  }

  /**
   * The user the metastore records for the lock. Informational for HMS - it is what
   * {@code show_locks} displays - so the impersonated user is used when there is one, and no UGI
   * lookup is done for it.
   */
  private String lockUser() {
    try {
      return support.impersonationResolver.resolve()
          .map(impersonation -> impersonation.userName())
          .filter(user -> user != null && !user.isBlank())
          .orElse("hms-proxy");
    } catch (Exception e) {
      // The backend call itself resolves impersonation again and will fail there if it matters;
      // a display field must not be what decides whether the table gets locked.
      return "hms-proxy";
    }
  }

  private static String resolveHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      // Only ever displayed by show_locks; a failed lookup must not cost a lock.
      return "hms-proxy";
    }
  }

  private static Method method(String name, Class<?> argumentType) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, argumentType);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("The frontend metastore API has no " + name, e);
    }
  }
}
