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
import org.apache.iceberg.rest.requests.RenameTableRequest;

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
 * that service's own (non-default) catalog. A namespace that does not resolve at all is not this
 * gate's business - {@code null} is returned so the normal dispatch can produce its usual 404.
 */
final class WriteRouteGate {
  private static final Set<Route> WRITE_ROUTES = EnumSet.of(
      Route.CREATE_TABLE, Route.UPDATE_TABLE, Route.DROP_TABLE, Route.RENAME_TABLE, Route.REGISTER_TABLE);
  private static final String NAMESPACE_VAR = "namespace";

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
    if (route == Route.RENAME_TABLE) {
      RenameTableRequest request = (RenameTableRequest) body;
      String sourceRefusal = refusalFor(externalDbName(request.source()));
      return sourceRefusal != null ? sourceRefusal : refusalFor(externalDbName(request.destination()));
    }
    return refusalFor(externalDbName(vars.get(NAMESPACE_VAR)));
  }

  private static String externalDbName(TableIdentifier identifier) {
    Namespace namespace = identifier.namespace();
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
    if (resolvedCatalog == null || resolvedCatalog.equals(defaultCatalogName)) {
      return null;
    }
    return "Writes are only supported in the default catalog '" + defaultCatalogName
        + "'; namespace '" + externalDbName + "' belongs to catalog '" + resolvedCatalog
        + "', which is served by the synthetic lock shim and provides no writer isolation.";
  }
}
