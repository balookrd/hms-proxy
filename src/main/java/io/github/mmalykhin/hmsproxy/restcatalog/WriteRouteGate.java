package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalogAdapter.Route;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Refuses Iceberg REST writes whose namespace resolves to any catalog other than the proxy's
 * default one. Only the default catalog's tables are backed by a real HMS lock; every other
 * catalog is served by the synthetic lock shim, which grants an EXCLUSIVE lock unconditionally
 * and so provides no writer isolation - a commit routed there would believe it owns the table
 * while racing (and possibly losing to) a concurrent writer.
 *
 * <p>The check below never trusts the request's URL prefix: under the default catalog's own
 * prefix, other catalogs' databases are exposed as federated names of the form
 * "{@code <catalog><separator><db>}" and resolve, by name, straight into that catalog's shim.
 * The {@code catalogForNamespace} collaborator supplied at construction is what actually
 * resolves an external database name to the catalog that owns it; for the default catalog's own
 * service that is the federated name as-is, and for a name-translated service {@link
 * IcebergRestService} composes it with that service's own translation so it always answers with
 * that service's own (non-default) catalog. This gate fails closed: a namespace whose owning
 * catalog cannot be determined is refused, not allowed - an unknown catalog ownership is exactly
 * the ambiguous case the synthetic lock shim's lack of conflict checking makes unsafe to guess
 * about. Today {@code CatalogRouter.resolveDatabase} is total and never actually produces this
 * case, but the gate does not rely on that holding forever.
 */
final class WriteRouteGate {
  // Every write route RESTCatalogAdapter.Route exposes. Read-only routes (LIST_*, LOAD_*,
  // *_EXISTS, CONFIG, TOKENS...) and REPORT_METRICS (phase-3 metrics bookkeeping, not a
  // metadata write) are deliberately left out and always pass through unchecked.
  private static final Set<Route> WRITE_ROUTES = EnumSet.of(
      Route.CREATE_TABLE, Route.UPDATE_TABLE, Route.DROP_TABLE, Route.RENAME_TABLE, Route.REGISTER_TABLE,
      Route.CREATE_VIEW, Route.UPDATE_VIEW, Route.DROP_VIEW, Route.RENAME_VIEW,
      Route.CREATE_NAMESPACE, Route.UPDATE_NAMESPACE, Route.DROP_NAMESPACE,
      Route.COMMIT_TRANSACTION);
  private static final String NAMESPACE_VAR = "namespace";

  /**
   * Test-only accessor so {@code WriteRouteGateTest} can assert every {@link Route} constant is
   * explicitly classified as a write or a deliberate non-write - never widen this beyond
   * package-private.
   */
  static Set<Route> writeRoutesForTesting() {
    return WRITE_ROUTES;
  }

  private final String defaultCatalogName;
  private final Function<String, String> catalogForNamespace;

  WriteRouteGate(String defaultCatalogName, Function<String, String> catalogForNamespace) {
    this.defaultCatalogName = Objects.requireNonNull(defaultCatalogName, "defaultCatalogName");
    this.catalogForNamespace = Objects.requireNonNull(catalogForNamespace, "catalogForNamespace");
  }

  /**
   * Returns {@code null} when the request may proceed, or a refusal message naming the resolved
   * catalog and the reason when it may not.
   */
  String check(Route route, Map<String, String> vars, Object body) {
    if (!WRITE_ROUTES.contains(route)) {
      return null;
    }
    if (route == Route.RENAME_TABLE || route == Route.RENAME_VIEW) {
      // Both routes carry the same RenameTableRequest {source, destination} body shape; either
      // side landing in a non-default catalog must refuse the whole rename.
      RenameTableRequest request = (RenameTableRequest) body;
      String sourceRefusal = refusalFor(externalDbName(request.source()));
      return sourceRefusal != null ? sourceRefusal : refusalFor(externalDbName(request.destination()));
    }
    if (route == Route.CREATE_NAMESPACE) {
      CreateNamespaceRequest request = (CreateNamespaceRequest) body;
      return refusalFor(externalDbName(request.namespace()));
    }
    if (route == Route.COMMIT_TRANSACTION) {
      // A multi-table atomic commit; every table change must resolve to the default catalog,
      // or the whole request is refused - one federated table riding along with default-catalog
      // ones must not slip through.
      CommitTransactionRequest request = (CommitTransactionRequest) body;
      for (UpdateTableRequest tableChange : request.tableChanges()) {
        String refusal = refusalFor(externalDbName(tableChange.identifier()));
        if (refusal != null) {
          return refusal;
        }
      }
      return null;
    }
    // Remaining write routes (table and view CRUD, DROP_NAMESPACE, UPDATE_NAMESPACE) carry the
    // namespace as a path variable.
    return refusalFor(externalDbName(vars.get(NAMESPACE_VAR)));
  }

  private static String externalDbName(TableIdentifier identifier) {
    return externalDbName(identifier.namespace());
  }

  private static String externalDbName(Namespace namespace) {
    return namespace.isEmpty() ? null : namespace.level(0);
  }

  private static String externalDbName(String encodedNamespace) {
    if (encodedNamespace == null) {
      return null;
    }
    Namespace namespace = RESTUtil.decodeNamespace(encodedNamespace);
    return namespace.isEmpty() ? null : namespace.level(0);
  }

  private String refusalFor(String externalDbName) {
    if (externalDbName == null) {
      return null;
    }
    String resolvedCatalog = catalogForNamespace.apply(externalDbName);
    if (resolvedCatalog == null) {
      // Fail closed: an unresolved namespace's owning catalog - and so whether it is backed by
      // a real HMS lock or the synthetic shim - cannot be determined, so the write is refused
      // rather than permitted by default.
      return "Writes are only supported in the default catalog '" + defaultCatalogName
          + "'; namespace '" + externalDbName + "' could not be resolved to any catalog, so "
          + "whether it is safe to write could not be determined.";
    }
    if (resolvedCatalog.equals(defaultCatalogName)) {
      return null;
    }
    return "Writes are only supported in the default catalog '" + defaultCatalogName
        + "'; namespace '" + externalDbName + "' belongs to catalog '" + resolvedCatalog
        + "', which is served by the synthetic lock shim and provides no writer isolation.";
  }
}
