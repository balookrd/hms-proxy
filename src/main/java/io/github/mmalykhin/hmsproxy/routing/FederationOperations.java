package io.github.mmalykhin.hmsproxy.routing;

import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableMeta;

public interface FederationOperations {
  boolean preserveBackendCatalogName();

  String externalDatabaseName(String catalog, String backendDbName);

  CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException;

  Optional<CatalogRouter.ResolvedNamespace> resolveCatalogIfKnown(String catalog, String backendDbName);

  CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException;

  boolean isDatabaseExposed(CatalogRouter.ResolvedNamespace namespace);

  boolean isDatabaseExposed(String catalogName, String backendDbName);

  boolean isTableExposed(CatalogRouter.ResolvedNamespace namespace, String tableName);

  boolean isTableExposed(String catalogName, String backendDbName, String tableName);

  TableMeta externalizeTableMeta(TableMeta value, CatalogRouter.ResolvedNamespace namespace);

  Object externalizeResult(Object value, CatalogRouter.ResolvedNamespace namespace);

  /**
   * Internalizes a single value. Lock requests need this: their components may resolve to different
   * databases, and each has to be rewritten against its own namespace rather than against one
   * namespace chosen for the whole argument list.
   */
  Object internalizeArgument(Object value, CatalogRouter.ResolvedNamespace namespace);

  Object[] internalizeDbStringArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace);

  Object[] internalizeObjectArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace);

  Object internalizeTableRequest(GetTableRequest request, CatalogRouter.ResolvedNamespace namespace);

  Object internalizeTablesRequest(GetTablesRequest request, CatalogRouter.ResolvedNamespace namespace);
}
