package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;

final class RefreshPrivilegesHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  RefreshPrivilegesHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    CatalogRouter.ResolvedNamespace namespace = args != null && args.length > 0
        ? support.federationLayer.findNamespaceInArgs(args)
        : null;
    if (namespace == null) {
      namespace = support.router.resolveCatalog(support.config.defaultCatalog(), "");
    }

    RequestContext.currentObservation().recordNamespace(namespace);
    if (namespace.backendDbName() != null && !namespace.backendDbName().isBlank()) {
      support.recordDefaultCatalogRouteIfImplicit(
          "refresh_privileges", namespace.externalDbName(), namespace);
    }

    CatalogBackend backend = namespace.backend();
    CatalogConfig catalogConfig = support.config.catalogs().get(backend.name());

    if (support.config.latencyRouting().refreshPrivilegesSyntheticSuccess()
        || isReadOnly(catalogConfig, namespace.backendDbName())) {
      GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
      response.setSuccess(true);
      return response;
    }

    Object[] routedArgs = support.federationLayer.internalizeObjectArguments(args, namespace);
    try {
      return support.invokeDirect(backend, method, routedArgs);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)
          || cause instanceof NoSuchMethodException) {
        GrantRevokePrivilegeResponse response = new GrantRevokePrivilegeResponse();
        response.setSuccess(true);
        return response;
      }
      throw cause;
    }
  }

  private static boolean isReadOnly(CatalogConfig catalogConfig, String backendDbName) {
    if (catalogConfig == null) {
      return false;
    }
    if (catalogConfig.accessMode() == CatalogAccessMode.READ_ONLY) {
      return true;
    }
    if (catalogConfig.accessMode() == CatalogAccessMode.READ_WRITE_DB_WHITELIST) {
      String normalized = backendDbName != null ? backendDbName.trim() : null;
      return normalized == null || normalized.isEmpty() || !catalogConfig.writeDbWhitelist().contains(normalized);
    }
    return false;
  }
}
