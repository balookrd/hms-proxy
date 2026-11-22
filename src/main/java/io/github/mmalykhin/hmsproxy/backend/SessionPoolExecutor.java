package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import io.github.mmalykhin.hmsproxy.observability.PrometheusMetrics;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared borrow/probe/replay loop for pooled backend sessions (both the shared pool in
 * {@link BackendRuntime} and per-user impersonation pools in {@link CatalogBackend}).
 */
final class SessionPoolExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SessionPoolExecutor.class);

  private SessionPoolExecutor() {
  }

  @FunctionalInterface
  interface SessionCall {
    Object apply(BackendInvocationSession session) throws Throwable;
  }

  interface PoolAdapter {
    String catalogName();

    String poolKind();

    String callContextDescription();

    PrometheusMetrics metrics();

    long sessionValidateAfterIdleMs();

    boolean isReplayBlocked();

    BackendInvocationSession borrow() throws MetaException;

    void release(BackendInvocationSession session);

    void discard(BackendInvocationSession session, String reason);

    void closeIdleSessions();
  }

  static Object invoke(PoolAdapter pool, String label, SessionCall call) throws Throwable {
    boolean replayable = HmsOperationPolicy.replayableAfterLostReply(label);
    BackendInvocationSession session = replayable ? pool.borrow() : borrowValidated(pool);
    try {
      Object result = call.apply(session);
      pool.release(session);
      session = null;
      return result;
    } catch (Throwable cause) {
      if (!ThriftFailureClassifier.isTransportFailure(cause) || pool.isReplayBlocked()) {
        if (session != null) {
          // A protocol desync poisons the connection, but the call must not be replayed.
          if (ThriftFailureClassifier.isProtocolDesync(cause)) {
            pool.discard(session, "protocol_desync");
          } else {
            pool.release(session);
          }
          session = null;
        }
        throw cause;
      }
      pool.discard(session, "transport_failure");
      session = null;
      if (!ThriftFailureClassifier.isReadTimeout(cause)) {
        pool.closeIdleSessions();
      }
      PrometheusMetrics metrics = pool.metrics();
      if (!ThriftFailureClassifier.isSafeToReplay(cause, replayable)) {
        if (metrics != null) {
          metrics.recordBackendCallNotReplayed(
              pool.catalogName(), label, ThriftFailureClassifier.transportFailurePhase(cause));
        }
        LOG.warn("Backend catalog '{}' transport failed{} in method {}; the backend may already have applied it,"
                + " not replaying the call",
            pool.catalogName(), pool.callContextDescription(), label, cause);
        throw cause;
      }
      LOG.warn("Backend catalog '{}' transport failed{} in method {}, discarding session and retrying once",
          pool.catalogName(), pool.callContextDescription(), label, cause);
      BackendInvocationSession retry = pool.borrow();
      try {
        Object result = call.apply(retry);
        pool.release(retry);
        retry = null;
        return result;
      } catch (Throwable retryError) {
        if (retry != null) {
          if (ThriftFailureClassifier.isTransportFailure(retryError)) {
            pool.discard(retry, "transport_failure");
          } else if (ThriftFailureClassifier.isProtocolDesync(retryError)) {
            pool.discard(retry, "protocol_desync");
          } else {
            pool.release(retry);
          }
        }
        throw retryError;
      }
    } finally {
      if (session != null) {
        pool.release(session);
      }
    }
  }

  /**
   * Borrow for a call that cannot be replayed. A session idle past the threshold is probed first:
   * a connection the backend dropped while it sat in the pool fails the probe, not the write, and
   * the write then goes out once on a fresh session.
   */
  static BackendInvocationSession borrowValidated(PoolAdapter pool) throws MetaException {
    BackendInvocationSession session = pool.borrow();
    long thresholdMs = pool.sessionValidateAfterIdleMs();
    if (thresholdMs <= 0L || session.idleMs() <= thresholdMs) {
      return session;
    }
    try {
      session.probe();
    } catch (Throwable probeFailure) {
      if (probeFailure instanceof Error error) {
        pool.discard(session, "probe_error");
        throw error;
      }
      if (ThriftFailureClassifier.isTransportFailure(probeFailure)
          || ThriftFailureClassifier.isProtocolDesync(probeFailure)) {
        LOG.info("Backend catalog '{}' {} session idle for {} ms failed the liveness probe ({}), replacing it",
            pool.catalogName(), pool.poolKind(), session.idleMs(), probeFailure.toString());
        recordValidation(pool, "stale");
        pool.discard(session, "stale_probe");
        if (!ThriftFailureClassifier.isReadTimeout(probeFailure)) {
          pool.closeIdleSessions();
        }
        return pool.borrow();
      }
      // Any other answer came from a live backend over this very connection.
    }
    recordValidation(pool, "alive");
    return session;
  }

  private static void recordValidation(PoolAdapter pool, String outcome) {
    PrometheusMetrics metrics = pool.metrics();
    if (metrics != null) {
      metrics.recordBackendSessionValidation(pool.catalogName(), pool.poolKind(), outcome);
    }
  }
}
