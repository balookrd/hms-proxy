package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import io.github.mmalykhin.hmsproxy.routing.NamespaceTranslator;
import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableMeta;

public final class FederationLayer {
  private final ProxyConfig config;
  private final CatalogRouter router;
  private final ViewDefinitionCompatibility viewDefinitionCompatibility;

  public FederationLayer(ProxyConfig config, CatalogRouter router) {
    this.config = config;
    this.router = router;
    this.viewDefinitionCompatibility = new ViewDefinitionCompatibility(config, router);
  }

  public boolean preserveBackendCatalogName() {
    return config.federation().preserveBackendCatalogName();
  }

  public String externalDatabaseName(String catalog, String backendDbName) {
    return router.externalDatabaseName(catalog, backendDbName);
  }

  public CatalogRouter.ResolvedNamespace resolveCatalog(String catalog, String backendDbName) {
    return router.resolveCatalog(catalog, backendDbName);
  }

  public Optional<CatalogRouter.ResolvedNamespace> resolveCatalogIfKnown(String catalog, String backendDbName) {
    return router.resolveCatalogIfKnown(catalog, backendDbName);
  }

  public CatalogRouter.ResolvedNamespace resolveDatabase(String dbName) throws MetaException {
    return router.resolveDatabase(dbName);
  }

  public Optional<CatalogRouter.ResolvedNamespace> resolvePattern(String dbPattern) {
    return router.resolvePattern(dbPattern);
  }

  public Object externalizeResult(Object value, CatalogRouter.ResolvedNamespace namespace) {
    Object externalized = NamespaceTranslator.externalizeResult(value, namespace, preserveBackendCatalogName());
    return viewDefinitionCompatibility.externalizeResult(externalized, namespace);
  }

  public TableMeta externalizeTableMeta(TableMeta value, CatalogRouter.ResolvedNamespace namespace) {
    return NamespaceTranslator.externalizeTableMeta(value, namespace, preserveBackendCatalogName());
  }

  public Object internalizeArgument(Object value, CatalogRouter.ResolvedNamespace namespace) {
    Object internalized = NamespaceTranslator.internalizeArgument(value, namespace, preserveBackendCatalogName());
    return viewDefinitionCompatibility.internalizeArgument(internalized, namespace);
  }

  public Object[] internalizeDbStringArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace) {
    Object[] routedArgs = Arrays.copyOf(args, args.length);
    routedArgs[0] = namespace.backendDbName();
    for (int index = 1; index < routedArgs.length; index++) {
      routedArgs[index] = internalizeArgument(routedArgs[index], namespace);
    }
    return routedArgs;
  }

  public Object[] internalizeObjectArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace) {
    Object[] routedArgs = Arrays.copyOf(args, args.length);
    for (int index = 0; index < routedArgs.length; index++) {
      routedArgs[index] = routedArgs[index] instanceof String dbName
          ? NamespaceTranslator.internalizeStringArgument(dbName, namespace)
          : internalizeArgument(routedArgs[index], namespace);
    }
    return routedArgs;
  }

  public CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException {
    CatalogRouter.ResolvedNamespace resolvedCandidate = null;
    for (Object argument : args) {
      String extractedDbName = NamespaceTranslator.extractDbName(argument);
      if (extractedDbName != null) {
        CatalogRouter.ResolvedNamespace candidate = resolveDatabase(extractedDbName);
        if (resolvedCandidate == null || sameNamespace(resolvedCandidate, candidate)) {
          resolvedCandidate = candidate;
        } else {
          throw new MetaException("Request contains conflicting namespace hints: '"
              + resolvedCandidate.externalDbName() + "' and '" + candidate.externalDbName() + "'");
        }
      }
    }
    for (Object argument : args) {
      if (!(argument instanceof String candidateString)) {
        continue;
      }
      CatalogRouter.ResolvedNamespace explicitNamespace = resolvePattern(candidateString).orElse(null);
      if (explicitNamespace == null) {
        continue;
      }
      if (resolvedCandidate == null || sameNamespace(resolvedCandidate, explicitNamespace)) {
        resolvedCandidate = explicitNamespace;
      } else {
        throw new MetaException("Request contains conflicting namespace hints: '"
            + resolvedCandidate.externalDbName() + "' and '" + explicitNamespace.externalDbName() + "'");
      }
    }
    return resolvedCandidate;
  }

  public CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException {
    if (catName != null && !catName.isBlank()) {
      Optional<CatalogRouter.ResolvedNamespace> explicitNamespace = resolveCatalogIfKnown(catName, dbName);
      if (explicitNamespace.isPresent()) {
        CatalogRouter.ResolvedNamespace resolvedByDb = resolvePattern(dbName).orElse(null);
        if (resolvedByDb != null) {
          if (!resolvedByDb.catalogName().equals(catName)) {
            throw new MetaException("Request has conflicting catalog and database namespace: catName='"
                + catName + "', dbName='" + dbName + "'");
          }
          return resolvedByDb;
        }
        return explicitNamespace.get();
      }
    }
    return resolveDatabase(dbName);
  }

  public Object internalizeTableRequest(GetTableRequest request, CatalogRouter.ResolvedNamespace namespace) {
    return internalizeArgument(request, namespace);
  }

  public Object internalizeTablesRequest(GetTablesRequest request, CatalogRouter.ResolvedNamespace namespace) {
    return internalizeArgument(request, namespace);
  }

  private boolean sameNamespace(CatalogRouter.ResolvedNamespace left, CatalogRouter.ResolvedNamespace right) {
    return left.catalogName().equals(right.catalogName()) && left.backendDbName().equals(right.backendDbName());
  }
}
