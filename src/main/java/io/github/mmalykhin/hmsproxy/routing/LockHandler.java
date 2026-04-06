package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Second handler in the invocation chain. Manages synthetic read lock lifecycle
 * (acquire/check/release) and ensures transaction cleanup after commit/abort.
 * Delegates non-synthetic lock requests and all other methods to the next handler.
 */
final class LockHandler implements InvocationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LockHandler.class);

  private final SyntheticReadLockManager syntheticReadLockManager;
  private final RequestRateLimiter requestRateLimiter;
  private final CatalogRouter router;
  private final FederationLayer federationLayer;
  private final ProxyObservability observability;
  private final InvocationHandler next;

  LockHandler(
      SyntheticReadLockManager syntheticReadLockManager,
      RequestRateLimiter requestRateLimiter,
      CatalogRouter router,
      FederationLayer federationLayer,
      ProxyObservability observability,
      InvocationHandler next
  ) {
    this.syntheticReadLockManager = syntheticReadLockManager;
    this.requestRateLimiter = requestRateLimiter;
    this.router = router;
    this.federationLayer = federationLayer;
    this.observability = observability;
    this.next = next;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    return switch (method.getName()) {
      case "lock" -> handleLock(proxy, method, args);
      case "check_lock" -> handleCheckLock(proxy, method, args);
      case "unlock" -> handleUnlock(proxy, method, args);
      case "heartbeat" -> handleHeartbeat(proxy, method, args);
      case "commit_txn" -> handleCommitTxn(proxy, method, args);
      case "abort_txn" -> handleAbortTxn(proxy, method, args);
      case "abort_txns" -> handleAbortTxns(proxy, method, args);
      default -> next.invoke(proxy, method, args);
    };
  }

  private Object handleLock(Object proxy, Method method, Object[] args) throws Throwable {
    CatalogRouter.ResolvedNamespace namespace = args == null ? null : findNamespaceInArgs(args);
    if (namespace != null) {
      SyntheticReadLockManager.SyntheticLockState syntheticState =
          syntheticReadLockManager.tryAcquire((LockRequest) args[0], namespace);
      if (syntheticState != null) {
        RequestContext.currentObservation().recordNamespace(namespace);
        enforceCatalogRateLimit(method.getName(), namespace.catalogName());
        RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
        LockResponse response = syntheticReadLockManager.acquiredResponse(syntheticState.lockId());
        if (LOG.isInfoEnabled()) {
          LOG.info("requestId={} synthetic read lock acquired catalog={} db={} txnId={} lockId={}",
              RequestContext.currentRequestId(),
              namespace.catalogName(),
              syntheticState.externalDbName(),
              syntheticState.txnId(),
              syntheticState.lockId());
        }
        return response;
      }
    }
    return next.invoke(proxy, method, args);
  }

  private Object handleCheckLock(Object proxy, Method method, Object[] args) throws Throwable {
    SyntheticReadLockManager.SyntheticLockState syntheticState =
        syntheticReadLockManager.syntheticLock((CheckLockRequest) args[0]);
    if (syntheticState == null) {
      return next.invoke(proxy, method, args);
    }
    RequestContext.currentObservation().recordNamespace(syntheticState.namespace(router));
    enforceCatalogRateLimit(method.getName(), syntheticState.namespace(router).catalogName());
    RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    return syntheticReadLockManager.acquiredResponse(syntheticState.lockId());
  }

  private Object handleUnlock(Object proxy, Method method, Object[] args) throws Throwable {
    SyntheticReadLockManager.SyntheticLockState syntheticState =
        syntheticReadLockManager.syntheticLock((org.apache.hadoop.hive.metastore.api.UnlockRequest) args[0]);
    if (syntheticState == null) {
      return next.invoke(proxy, method, args);
    }
    RequestContext.currentObservation().recordNamespace(syntheticState.namespace(router));
    enforceCatalogRateLimit(method.getName(), syntheticState.namespace(router).catalogName());
    RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    syntheticReadLockManager.releaseLock(syntheticState);
    return null;
  }

  private Object handleHeartbeat(Object proxy, Method method, Object[] args) throws Throwable {
    HeartbeatRequest request = (HeartbeatRequest) args[0];
    SyntheticReadLockManager.SyntheticLockState syntheticState = syntheticReadLockManager.syntheticLock(request);
    if (syntheticState == null) {
      return next.invoke(proxy, method, args);
    }
    RequestContext.currentObservation().recordNamespace(syntheticState.namespace(router));
    enforceCatalogRateLimit(method.getName(), syntheticState.namespace(router).catalogName());
    RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    syntheticReadLockManager.touch(syntheticState);

    HeartbeatRequest txnOnlyHeartbeat = syntheticReadLockManager.txnOnlyHeartbeat(request);
    if (txnOnlyHeartbeat == null) {
      syntheticReadLockManager.recordHeartbeatWithoutTxn(syntheticState);
      return null;
    }
    Object result = next.invoke(proxy, method, new Object[]{txnOnlyHeartbeat});
    syntheticReadLockManager.recordHeartbeatForwarded(syntheticState);
    return result;
  }

  private Object handleCommitTxn(Object proxy, Method method, Object[] args) throws Throwable {
    long txnId = ((CommitTxnRequest) args[0]).getTxnid();
    try {
      return next.invoke(proxy, method, args);
    } finally {
      syntheticReadLockManager.releaseTxn(txnId);
    }
  }

  private Object handleAbortTxn(Object proxy, Method method, Object[] args) throws Throwable {
    long txnId = ((AbortTxnRequest) args[0]).getTxnid();
    try {
      return next.invoke(proxy, method, args);
    } finally {
      syntheticReadLockManager.releaseTxn(txnId);
    }
  }

  private Object handleAbortTxns(Object proxy, Method method, Object[] args) throws Throwable {
    List<Long> txnIds = ((AbortTxnsRequest) args[0]).getTxn_ids();
    try {
      return next.invoke(proxy, method, args);
    } finally {
      if (txnIds != null) {
        for (Long txnId : txnIds) {
          syntheticReadLockManager.releaseTxn(txnId == null ? 0L : txnId);
        }
      }
    }
  }

  private CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException {
    try {
      return federationLayer.findNamespaceInArgs(args);
    } catch (MetaException e) {
      observability.metrics().recordRoutingAmbiguous();
      throw e;
    }
  }

  private void enforceCatalogRateLimit(String methodName, String catalogName) throws RateLimitExceededException {
    if (!RequestContext.currentObservation().shouldRateLimitCatalog(catalogName)) {
      return;
    }
    requestRateLimiter.enforceCatalog(methodName, catalogName);
  }
}
