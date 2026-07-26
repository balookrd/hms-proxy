package io.github.mmalykhin.hmsproxy.routing;

import java.util.Locale;
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
   * The namespace a lock request is routed by. Components that resolve to another catalog are
   * dropped from the request that reaches the backend - see {@link LockRequestSplit} for why that
   * is safe and how the primary catalog is chosen.
   */
  static CatalogRouter.ResolvedNamespace resolveLockNamespace(
      LockRequest request,
      CatalogRouter router,
      String defaultCatalog
  ) throws MetaException {
    LockRequestSplit split = LockRequestSplit.of(request, router, defaultCatalog);
    return split == null ? null : split.primary();
  }
}
