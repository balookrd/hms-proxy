package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockComponent;
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

  private final ProxyConfig config;
  private final SyntheticReadLockManager syntheticReadLockManager;
  private final AdmissionGate admissionGate;
  private final CatalogRouter router;
  private final FederationOperations federationLayer;
  private final ProxyObservability observability;
  private final InvocationHandler next;

  LockHandler(
      ProxyConfig config,
      SyntheticReadLockManager syntheticReadLockManager,
      AdmissionGate admissionGate,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      InvocationHandler next
  ) {
    this.config = config;
    this.syntheticReadLockManager = syntheticReadLockManager;
    this.admissionGate = admissionGate;
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
    LockRequestSplit split = args == null ? null : findLockSplit(args);
    CatalogRouter.ResolvedNamespace namespace = split == null ? null : split.primary();
    if (namespace != null) {
      LockRequest request = (LockRequest) args[0];
      // Admission must run before the lock state is persisted: a throttled client never learns the
      // lockId and could not release the lock, so it would linger until the txn timeout expires.
      if (syntheticReadLockManager.isSyntheticReadLockCandidate(request, namespace)) {
        RequestContext.currentObservation().recordNamespace(namespace);
        validateSyntheticWriteAccess(split, method.getName());
        admissionGate.enforceRateLimit(method.getName(), namespace.catalogName());
        SyntheticReadLockManager.SyntheticLockState syntheticState =
            syntheticReadLockManager.tryAcquire(request, namespace);
        if (syntheticState != null) {
          RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
          LockResponse response = syntheticReadLockManager.acquiredResponse(syntheticState.lockId());
          if (LOG.isInfoEnabled()) {
            LockComponent routing = routingComponent(request);
            LOG.info("requestId={} synthetic lock acquired catalog={} db={} txnId={} lockId={} "
                    + "components={} operationType={} lockType={}",
                RequestContext.currentRequestId(),
                namespace.catalogName(),
                syntheticState.externalDbName(),
                syntheticState.txnId(),
                syntheticState.lockId(),
                request.getComponentSize(),
                routing == null ? null : routing.getOperationType(),
                routing == null ? null : routing.getType());
          }
          return response;
        }
      }
    }
    return next.invoke(proxy, method, args);
  }

  private Object handleCheckLock(Object proxy, Method method, Object[] args) throws Throwable {
    SyntheticReadLockManager.SyntheticLockState syntheticState =
        syntheticReadLockManager.syntheticLockForCheck((CheckLockRequest) args[0]);
    if (syntheticState == null) {
      return next.invoke(proxy, method, args);
    }
    RequestContext.currentObservation().recordNamespace(syntheticState.namespace(router));
    admissionGate.enforceRateLimit(method.getName(), syntheticState.namespace(router).catalogName());
    RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    return syntheticReadLockManager.acquiredResponse(syntheticState.lockId());
  }

  private Object handleUnlock(Object proxy, Method method, Object[] args) throws Throwable {
    SyntheticReadLockManager.SyntheticLockState syntheticState =
        syntheticReadLockManager.syntheticLockForUnlock((org.apache.hadoop.hive.metastore.api.UnlockRequest) args[0]);
    if (syntheticState == null) {
      return next.invoke(proxy, method, args);
    }
    RequestContext.currentObservation().recordNamespace(syntheticState.namespace(router));
    admissionGate.enforceRateLimit(method.getName(), syntheticState.namespace(router).catalogName());
    RequestContext.currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    syntheticReadLockManager.releaseLock(syntheticState);
    return null;
  }

  private Object handleHeartbeat(Object proxy, Method method, Object[] args) throws Throwable {
    HeartbeatRequest request = (HeartbeatRequest) args[0];
    SyntheticReadLockManager.SyntheticLockState syntheticState = syntheticReadLockManager.syntheticLockForHeartbeat(request);
    if (syntheticState == null) {
      return next.invoke(proxy, method, args);
    }
    RequestContext.currentObservation().recordNamespace(syntheticState.namespace(router));
    admissionGate.enforceRateLimit(method.getName(), syntheticState.namespace(router).catalogName());
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

  /** The component the request was routed by, so the log line does not describe the pseudo source. */
  private static LockComponent routingComponent(LockRequest request) {
    LockComponent first = null;
    for (LockComponent component : request.getComponent()) {
      if (first == null) {
        first = component;
      }
      if (!HivePlaceholderNamespace.isPlaceholderDbName(component.getDbname())) {
        return component;
      }
    }
    return first;
  }

  /**
   * The synthetic path never reaches the backend, so the catalog access mode has to be enforced
   * here. Only write components are checked: {@code lock} counts as a mutating method by name, so
   * validating every synthetic lock would reject plain {@code SELECT} reads of a READ_ONLY catalog.
   *
   * <p>Each component is checked against its own catalog. A request may span several of them, and a
   * component whose catalog is not the one the request routes by is dropped rather than locked -
   * dropping it must not drop the access-mode check with it.
   */
  private void validateSyntheticWriteAccess(LockRequestSplit split, String methodName)
      throws MetaException {
    for (LockRequestSplit.Component component : split.components()) {
      if (component.namespace() == null) {
        continue;
      }
      if (SyntheticReadLockManager.isWriteOperation(component.component().getOperationType())) {
        CatalogAccessModeGuard.validate(
            config.catalogs().get(component.namespace().catalogName()),
            methodName,
            component.namespace().backendDbName());
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

  private LockRequestSplit findLockSplit(Object[] args) throws MetaException {
    if (args.length == 0 || !(args[0] instanceof LockRequest request)) {
      CatalogRouter.ResolvedNamespace namespace = findNamespaceInArgs(args);
      return namespace == null ? null : LockRequestSplit.ofResolvedNamespace(namespace);
    }
    try {
      return LockRequestSplit.of(request, router, config.defaultCatalog());
    } catch (MetaException e) {
      observability.metrics().recordRoutingAmbiguous();
      throw e;
    }
  }
}
