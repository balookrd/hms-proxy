package io.github.mmalykhin.hmsproxy.routing;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * A lock request groups every table a statement touches, so a query that reads across catalogs -
 * or merely across two databases of one catalog - arrives as a single request whose components
 * resolve to different namespaces.
 *
 * <p>The proxy acquires and acknowledges such a request as one unit and hands back one lock id, so
 * it cannot forward the whole request to more than one backend. It splits instead: the components
 * of one catalog are routed to that catalog's metastore, and the rest are dropped from the request
 * that reaches the backend.
 *
 * <p>Dropping them costs nothing that was ever held. Non-default catalogs are served by
 * {@link SyntheticReadLockManager}, whose store records a lock and always reports it as acquired
 * without ever testing for a conflict - it is bookkeeping, not mutual exclusion. A component the
 * proxy drops here therefore loses an entry in that ledger, not a guarantee. The default catalog is
 * different: it owns the TxnHandler, its locks are real, and it is picked as the primary whenever it
 * is present so that ACID statements keep the exclusion they depend on. Access-mode enforcement does
 * not travel with the routing decision - {@link #components()} still reports the dropped components
 * so a write into a READ_ONLY catalog is refused whether or not it survived the split.
 */
final class LockRequestSplit {
  private final CatalogRouter.ResolvedNamespace primary;
  private final List<Component> components;

  private LockRequestSplit(CatalogRouter.ResolvedNamespace primary, List<Component> components) {
    this.primary = primary;
    this.components = components;
  }

  /**
   * Resolves the namespace of every component and picks the catalog the request is routed to.
   *
   * @return {@code null} when no component names a database the router can resolve
   */
  static LockRequestSplit of(LockRequest request, CatalogRouter router, String defaultCatalog)
      throws MetaException {
    List<LockComponent> rawComponents = request == null ? null : request.getComponent();
    if (rawComponents == null || rawComponents.isEmpty()) {
      return null;
    }

    List<Component> resolved = new ArrayList<>(rawComponents.size());
    CatalogRouter.ResolvedNamespace firstRealNamespace = null;
    CatalogRouter.ResolvedNamespace defaultCatalogNamespace = null;
    CatalogRouter.ResolvedNamespace placeholderNamespace = null;

    for (LockComponent component : rawComponents) {
      String dbName = NamespaceTranslator.extractDbName(component);
      if (dbName == null) {
        resolved.add(new Component(component, null, true));
        continue;
      }
      // Hive locks its INSERT ... VALUES placeholder alongside the real target. It exists in no
      // metastore, belongs to no catalog and must not pick one, but it still has to reach the
      // backend unrewritten - every unproxied Hive sends exactly this name.
      if (HivePlaceholderNamespace.isPlaceholderDbName(dbName)) {
        if (placeholderNamespace == null) {
          placeholderNamespace = router.resolveDatabase(dbName);
        }
        resolved.add(new Component(component, null, true));
        continue;
      }
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      resolved.add(new Component(component, namespace, false));
      if (firstRealNamespace == null) {
        firstRealNamespace = namespace;
      }
      if (defaultCatalogNamespace == null && namespace.catalogName().equals(defaultCatalog)) {
        defaultCatalogNamespace = namespace;
      }
    }

    // The default catalog wins whenever it is present: it owns the TxnHandler, and a transactional
    // statement that lost its real lock there would corrupt what the synthetic ledger only records.
    CatalogRouter.ResolvedNamespace primary =
        defaultCatalogNamespace != null ? defaultCatalogNamespace : firstRealNamespace;
    if (primary == null) {
      // Placeholder-only requests carry no namespace of their own; they land on the catalog that
      // owns the TxnHandler, the same target they reached before catalog federation existed.
      return placeholderNamespace == null
          ? null
          : new LockRequestSplit(placeholderNamespace, resolved);
    }
    return new LockRequestSplit(primary, resolved);
  }

  /**
   * A split over a namespace that was resolved without a lock request - the caller reached the lock
   * path with something other than a {@code LockRequest} argument, so there are no components to
   * group and nothing can be dropped.
   */
  static LockRequestSplit ofResolvedNamespace(CatalogRouter.ResolvedNamespace namespace) {
    return new LockRequestSplit(namespace, List.of());
  }

  /** The namespace the request is routed by; its catalog owns every component kept in the request. */
  CatalogRouter.ResolvedNamespace primary() {
    return primary;
  }

  /** Every component of the original request, each tagged with the namespace it resolved to. */
  List<Component> components() {
    return components;
  }

  /** True when components of more than one catalog were present and some had to be dropped. */
  boolean isSplit() {
    for (Component component : components) {
      if (component.isDropped(primary)) {
        return true;
      }
    }
    return false;
  }

  /** The components dropped from the request that reaches the backend, for logging and metrics. */
  List<Component> dropped() {
    List<Component> result = new ArrayList<>();
    for (Component component : components) {
      if (component.isDropped(primary)) {
        result.add(component);
      }
    }
    return result;
  }

  /**
   * Builds the request for the backend: the components of the primary catalog plus the untouched
   * placeholders, each internalized against its <em>own</em> namespace. The generic argument
   * internalization cannot do this - it rewrites every component it walks to one backend database,
   * which is precisely what made a two-database request unroutable.
   */
  LockRequest backendRequest(LockRequest original, FederationOperations federationLayer) {
    List<LockComponent> routed = new ArrayList<>(components.size());
    for (Component component : components) {
      if (component.isDropped(primary)) {
        continue;
      }
      // Internalization copies the value it is handed and returns the rewritten copy, so the
      // client's request object is never mutated. Components with no namespace of their own (Hive's
      // placeholder) are copied here for the same reason.
      routed.add(component.namespace() == null
          ? component.component().deepCopy()
          : (LockComponent) federationLayer.internalizeArgument(
              component.component(), component.namespace()));
    }
    LockRequest prepared = new LockRequest(original);
    prepared.setComponent(routed);
    return prepared;
  }

  /**
   * @param namespace the namespace the component resolved to, {@code null} for Hive's placeholder
   *                  and for components that name no database at all
   * @param neutral   true when the component belongs to no catalog and therefore travels with
   *                  whichever backend the rest of the request selected
   */
  record Component(
      LockComponent component,
      CatalogRouter.ResolvedNamespace namespace,
      boolean neutral
  ) {
    boolean isDropped(CatalogRouter.ResolvedNamespace primary) {
      return !neutral && namespace != null && !namespace.catalogName().equals(primary.catalogName());
    }
  }
}
