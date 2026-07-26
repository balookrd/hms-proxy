package io.github.mmalykhin.hmsproxy.routing;

import java.util.List;
import java.util.Locale;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;

/**
 * Hive plans {@code INSERT ... VALUES} through an internal placeholder table
 * ({@code SemanticAnalyzer.DUMMY_DATABASE}/{@code DUMMY_TABLE}, {@code _dummy_database} and
 * {@code _dummy_table} in every Hive line the proxy supports - Apache 2.3, 3.1, 4.x and the
 * HDP 3.1 fork), so such a statement sends a lock request whose components span the placeholder
 * plus the real target table. The placeholder exists in no metastore and belongs to no catalog:
 * it must neither select a catalog, nor count as a second namespace, nor be rewritten into a real
 * backend database.
 */
final class HivePlaceholderNamespace {
  static final String DUMMY_DATABASE = "_dummy_database";

  private HivePlaceholderNamespace() {}

  static boolean isPlaceholderDbName(String dbName) {
    return dbName != null && DUMMY_DATABASE.equals(dbName.trim().toLowerCase(Locale.ROOT));
  }

  /**
   * A lock request is acquired, routed and acknowledged as a single unit: either the synthetic
   * shim answers for all of its components or the whole request goes to one backend, where
   * namespace internalization rewrites every component to the resolved database. Resolving the
   * namespace from the first component alone would therefore silently drop or rewrite components
   * of the other databases - including default-catalog DDL locks that must reach a real metastore.
   * Mixed requests are rejected instead, so the caller can split them per namespace.
   *
   * <p>Placeholder components carry no namespace of their own. A request made only of them still
   * resolves through the placeholder name, which lands on the default catalog that owns the
   * TxnHandler - the same target such a lock reached before catalog federation existed.
   */
  static CatalogRouter.ResolvedNamespace resolveLockNamespace(LockRequest request, CatalogRouter router)
      throws MetaException {
    List<LockComponent> components = request.getComponent();
    if (components == null || components.isEmpty()) {
      return null;
    }
    CatalogRouter.ResolvedNamespace resolved = null;
    CatalogRouter.ResolvedNamespace placeholderNamespace = null;
    for (LockComponent component : components) {
      String dbName = NamespaceTranslator.extractDbName(component);
      if (dbName == null) {
        continue;
      }
      if (isPlaceholderDbName(dbName)) {
        if (placeholderNamespace == null) {
          placeholderNamespace = router.resolveDatabase(dbName);
        }
        continue;
      }
      CatalogRouter.ResolvedNamespace candidate = router.resolveDatabase(dbName);
      if (resolved == null) {
        resolved = candidate;
        continue;
      }
      if (!sameNamespace(resolved, candidate)) {
        throw new MetaException("Lock request spans multiple namespaces: '"
            + resolved.externalDbName() + "' and '" + candidate.externalDbName()
            + "'. The proxy acquires and acknowledges a lock request as a whole, so it cannot"
            + " split components across catalogs; issue one lock request per namespace");
      }
    }
    return resolved != null ? resolved : placeholderNamespace;
  }

  private static boolean sameNamespace(
      CatalogRouter.ResolvedNamespace left,
      CatalogRouter.ResolvedNamespace right
  ) {
    return left.catalogName().equals(right.catalogName())
        && left.backendDbName().equals(right.backendDbName());
  }
}
