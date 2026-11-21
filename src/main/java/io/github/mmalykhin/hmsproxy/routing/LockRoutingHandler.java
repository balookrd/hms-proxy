package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes {@code lock} to a backend. A lock request carries every table a statement touches, so its
 * components can resolve to several databases and even several catalogs; the generic routing path
 * cannot serve it, because it internalizes the whole argument list against one namespace and would
 * rewrite every component to a single backend database.
 *
 * <p>{@link LockRequestSplit} picks the catalog the request is routed by and rewrites each surviving
 * component against its own namespace, which is what makes a lock spanning two databases of one
 * catalog work at all. Components of other catalogs are dropped from the request - see that class
 * for why nothing that was held is lost.
 */
final class LockRoutingHandler implements SpecialCaseHandler {
  private static final String METHOD_NAME = "lock";
  private static final Logger LOG = LoggerFactory.getLogger(LockRoutingHandler.class);

  private final RoutingSupport support;
  private final NamespaceFallback fallback;

  LockRoutingHandler(RoutingSupport support, NamespaceFallback fallback) {
    this.support = support;
    this.fallback = fallback;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    if (args == null || args.length == 0 || !(args[0] instanceof LockRequest request)) {
      return fallback.routeByNamespaceOrFail(method, args);
    }
    LockRequestSplit split =
        LockRequestSplit.of(request, support.router, support.config.defaultCatalog());
    if (split == null) {
      return fallback.routeByNamespaceOrFail(method, args);
    }

    CatalogRouter.ResolvedNamespace primary = split.primary();
    RequestContext.currentObservation().recordNamespace(primary);
    validateComponentAccess(split);

    // Follow-up check_lock, heartbeat, and unlock route strictly to defaultCatalog by lockId
    // (AcidOps). Forwarding a real lock to a non-default backend strands the lock there and lets a
    // colliding numeric lockId unlock an unrelated transaction in defaultCatalog.
    if (!primary.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("Backend lock request for non-default catalog '"
          + primary.catalogName() + "' cannot be forwarded: only default catalog '"
          + support.config.defaultCatalog() + "' owns real backend locks");
    }

    LockRequest prepared = split.backendRequest(request, support.federationLayer);
    if (split.isSplit()) {
      recordSplit(split);
    }
    return support.invokeDirect(primary.backend(), method, new Object[] {prepared});
  }

  /**
   * Validates catalog access mode across every component in the request, both routed and dropped.
   * Checking every component enforces READ_WRITE_DB_WHITELIST on multi-DB requests and blocks
   * NO_TXN/DDL EXCLUSIVE locks on READ_ONLY catalogs while allowing plain SELECT SHARED_READ locks.
   */
  private void validateComponentAccess(LockRequestSplit split) throws Exception {
    for (LockRequestSplit.Component component : split.components()) {
      if (component.namespace() == null) {
        continue;
      }
      if (SyntheticReadLockManager.requiresWriteAccess(component.component())) {
        CatalogAccessModeGuard.validate(
            support.config.catalogs().get(component.namespace().catalogName()),
            METHOD_NAME,
            component.namespace().backendDbName());
      }
    }
  }

  private void recordSplit(LockRequestSplit split) {
    support.observability.metrics().recordLockRequestSplit(split.primary().catalogName());
    if (LOG.isInfoEnabled()) {
      StringBuilder droppedComponents = new StringBuilder();
      for (LockRequestSplit.Component dropped : split.dropped()) {
        if (droppedComponents.length() > 0) {
          droppedComponents.append(", ");
        }
        droppedComponents
            .append(dropped.namespace().externalDbName())
            .append('.')
            .append(dropped.component().getTablename());
      }
      LOG.info("requestId={} lock request spans several catalogs: routed to catalog={} db={}, "
              + "components left unlocked=[{}]. Non-default catalogs are served by the synthetic "
              + "shim, which records locks without enforcing them, so no held lock is lost",
          RequestContext.currentRequestId(),
          split.primary().catalogName(),
          split.primary().externalDbName(),
          droppedComponents);
    }
  }
}
