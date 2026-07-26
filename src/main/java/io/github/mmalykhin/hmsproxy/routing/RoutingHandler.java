package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.routing.DefaultBackendRoutingPolicy;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.operation.OperationMetadata;

/**
 * Terminal handler in the invocation chain. Performs namespace-aware routing to catalog backends.
 *
 * <p>Per-RPC special cases are implemented as {@link SpecialCaseHandler}s in sibling files and
 * wired into the dispatch map in {@link #buildSpecialCaseHandlers}. Adding a new RPC-specific
 * behavior means adding a new handler class and one entry to that map — this class owns the
 * generic namespace-routing fallback only.
 */
final class RoutingHandler implements InvocationHandler, NamespaceFallback {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingHandler.class);

  private final ProxyConfig config;
  private final CatalogRouter router;
  private final CompatibilityLayer compatibilityLayer;
  private final ProxyObservability observability;
  private final RoutingSupport support;
  private final ExternalTableLocationRewriter externalTableLocationRewriter;
  private final DropTableHandler dropTableHandler;
  private final Map<String, SpecialCaseHandler> specialCaseHandlers;

  RoutingHandler(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      CompatibilityLayer compatibilityLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver
  ) {
    this(
        config,
        router,
        federationLayer,
        compatibilityLayer,
        observability,
        dispatcher,
        impersonationResolver,
        new FileSystemExternalTableDropPurger(config));
  }

  RoutingHandler(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      CompatibilityLayer compatibilityLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      ExternalTableDropPurger externalTableDropPurger
  ) {
    this.config = config;
    this.router = router;
    this.compatibilityLayer = compatibilityLayer;
    this.observability = observability;
    this.support = new RoutingSupport(
        config, router, federationLayer, observability, dispatcher, impersonationResolver);
    this.externalTableLocationRewriter = new ExternalTableLocationRewriter(config.federation());
    this.dropTableHandler = new DropTableHandler(support, this, externalTableDropPurger);
    this.specialCaseHandlers = buildSpecialCaseHandlers(dropTableHandler);
  }

  /** Stops the background external-table purge workers; pending purges are awaited. */
  void close() {
    dropTableHandler.close();
  }

  private Map<String, SpecialCaseHandler> buildSpecialCaseHandlers(SpecialCaseHandler dropTable) {
    SpecialCaseHandler setUgi = new SetUgiHandler(support, this);
    SpecialCaseHandler getAllDatabases = new GetAllDatabasesHandler(support);
    SpecialCaseHandler getDatabases = new GetDatabasesHandler(support);
    SpecialCaseHandler getTableMeta = new GetTableMetaHandler(support);
    SpecialCaseHandler getTableReq = new GetTableReqHandler(support);
    SpecialCaseHandler getTablesReq = new GetTablesReqHandler(support);
    SpecialCaseHandler addWriteNotificationLog = new AddWriteNotificationLogHandler(support);
    SpecialCaseHandler getTablesExt = new GetTablesExtHandler(support);
    SpecialCaseHandler getAllMvForRewriting = new GetAllMaterializedViewObjectsForRewritingHandler(support);
    return Map.ofEntries(
        Map.entry("set_ugi", setUgi),
        Map.entry("get_all_databases", getAllDatabases),
        Map.entry("get_databases", getDatabases),
        Map.entry("get_table_meta", getTableMeta),
        Map.entry("get_table_req", getTableReq),
        Map.entry("get_table_objects_by_name_req", getTablesReq),
        Map.entry("addWriteNotificationLog", addWriteNotificationLog),
        Map.entry("getTablesExt", getTablesExt),
        Map.entry("getAllMaterializedViewObjectsForRewriting", getAllMvForRewriting),
        Map.entry("drop_table", dropTable),
        Map.entry("drop_table_with_environment_context", dropTable)
    );
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    SpecialCaseHandler handler = specialCaseHandlers.get(method.getName());
    return handler != null ? handler.handle(method, args) : routeByNamespaceOrFail(method, args);
  }

  @Override
  public Object routeByNamespaceOrFail(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    OperationMetadata operation = HmsOperationPolicy.describe(methodName);
    if (args == null || args.length == 0) {
      return invokeGlobal(method, args);
    }
    return switch (operation.namespaceStrategy()) {
      case NONE -> invokeGlobal(method, args);
      case DB_STRING_ARG0 -> routeByDbStringArgument(method, args);
      case DB_FIRST_STRING_ARG0 -> routeByDbFirstStringArguments(method, args);
      case EXTRACT_FROM_ARGS -> routeByExtractedNamespace(method, args);
    };
  }

  @Override
  public Object invokeGlobal(Method method, Object[] args) throws Throwable {
    Optional<Object> compatibilityFallback = compatibilityLayer.fallback(
        method.getName(),
        new MetaException("Operation " + method.getName()
            + " does not carry explicit namespace ownership for deterministic routing"));
    if (!DefaultBackendRoutingPolicy.routesToDefaultBackend(method.getName())
        && !router.singleCatalog()
        && compatibilityFallback.isPresent()) {
      RequestContext.currentObservation().markFallback();
      LOG.warn("requestId={} method={} has no explicit namespace ownership, returning compatibility fallback",
          RequestContext.currentRequestId(), method.getName());
      return compatibilityFallback.get();
    }
    if (DefaultBackendRoutingPolicy.routesToDefaultBackend(method.getName())) {
      RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
      observability.metrics().recordDefaultCatalogRoute(method.getName());
      support.validateCatalogAccess(router.defaultBackend(), method.getName(), null);
      return support.invokeDirect(router.defaultBackend(), method, args);
    }
    if (!router.singleCatalog()) {
      throw new MetaException("Operation " + method.getName()
          + " requires explicit namespace ownership for deterministic routing; use explicit catalog.db naming"
          + " or a catalog-aware request so the proxy can fail safely instead of guessing a target catalog");
    }
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    observability.metrics().recordDefaultCatalogRoute(method.getName());
    support.validateCatalogAccess(router.defaultBackend(), method.getName(), null);
    return support.invokeDirect(router.defaultBackend(), method, args);
  }

  private Object routeByDbStringArgument(Method method, Object[] args) throws Throwable {
    if (!(args[0] instanceof String dbName)) {
      return invokeGlobal(method, args);
    }
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    support.validateCatalogAccess(namespace.backend(), method.getName(), namespace.backendDbName());
    validateReadExposure(method.getName(), namespace, args);
    Object[] routedArgs = support.federationLayer.internalizeDbStringArguments(args, namespace);
    Object result = support.invokeDirect(namespace.backend(), method, routedArgs);
    result = filterReadResult(method.getName(), namespace, result);
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private Object routeByDbFirstStringArguments(Method method, Object[] args) throws Throwable {
    if (args.length <= 1 || !(args[0] instanceof String dbName) || !(args[1] instanceof String)) {
      return invokeGlobal(method, args);
    }
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    support.validateCatalogAccess(namespace.backend(), method.getName(), namespace.backendDbName());
    validateReadExposure(method.getName(), namespace, args);
    Object[] routedArgs = support.federationLayer.internalizeDbStringArguments(args, namespace);
    Object result = support.invokeDirect(namespace.backend(), method, routedArgs);
    result = filterReadResult(method.getName(), namespace, result);
    return support.federationLayer.externalizeResult(result, namespace);
  }

  private Object routeByExtractedNamespace(Method method, Object[] args) throws Throwable {
    CatalogRouter.ResolvedNamespace extractedNamespace = findNamespaceInArgs(args);
    if (extractedNamespace == null) {
      return invokeGlobal(method, args);
    }
    String methodName = method.getName();
    OperationMetadata operation = HmsOperationPolicy.describe(methodName);
    RequestContext.currentObservation().recordNamespace(extractedNamespace);
    support.validateCatalogAccess(extractedNamespace.backend(), methodName, extractedNamespace.backendDbName());
    validateReadExposure(methodName, extractedNamespace, args);
    validateAcidNotOnNonDefaultCatalog(operation, extractedNamespace, methodName);
    validateTransactionalTableCreationOnDefaultCatalog(methodName, extractedNamespace, args);
    Object[] routedArgs = support.federationLayer.internalizeObjectArguments(args, extractedNamespace);
    externalTableLocationRewriter.rewriteObjectArguments(routedArgs, extractedNamespace, methodName);
    Object result = support.invokeDirect(extractedNamespace.backend(), method, routedArgs);
    result = filterReadResult(methodName, extractedNamespace, result);
    return support.federationLayer.externalizeResult(result, extractedNamespace);
  }

  /**
   * Methods that require the TxnHandler that owns the transaction and therefore must be routed
   * to the default catalog regardless of which catalog owns the table. Other ACID_NAMESPACE_BOUND_WRITE
   * operations (e.g. lock, compact) are legitimately namespace-bound and may target any catalog.
   */
  private static final java.util.Set<String> TXN_HANDLER_REQUIRED_METHODS = java.util.Set.of(
      "allocate_table_write_ids",
      "get_valid_write_ids"
  );

  private void validateAcidNotOnNonDefaultCatalog(
      OperationMetadata operation,
      CatalogRouter.ResolvedNamespace namespace,
      String methodName
  ) throws MetaException {
    if (!TXN_HANDLER_REQUIRED_METHODS.contains(methodName)) {
      return;
    }
    if (namespace.catalogName().equals(config.defaultCatalog())) {
      return;
    }
    throw new MetaException(
        "ACID transactional operation '" + methodName + "' is not supported for non-default catalog '"
            + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
            + config.defaultCatalog() + "'");
  }

  private void validateTransactionalTableCreationOnDefaultCatalog(
      String methodName,
      CatalogRouter.ResolvedNamespace namespace,
      Object[] args
  ) throws MetaException {
    if (namespace.catalogName().equals(config.defaultCatalog())) {
      return;
    }
    if (!methodName.startsWith("create_table")) {
      return;
    }
    if (args == null) {
      return;
    }
    for (Object arg : args) {
      if (arg instanceof Table table) {
        java.util.Map<String, String> params = table.getParameters();
        if (params != null && "true".equalsIgnoreCase(params.get("transactional"))) {
          throw new MetaException(
              "Cannot create transactional table '"
                  + table.getDbName() + "." + table.getTableName()
                  + "' in non-default catalog '" + namespace.catalogName()
                  + "'. Transactional (ACID) tables are only supported in the default catalog '"
                  + config.defaultCatalog() + "'");
        }
      }
    }
  }

  private void validateReadExposure(String methodName, CatalogRouter.ResolvedNamespace namespace, Object[] args)
      throws TException {
    OperationMetadata operation = HmsOperationPolicy.describe(methodName);
    if (operation.mutating()) {
      return;
    }
    support.validateExposedDatabaseAccess(methodName, namespace);
    String tableName = extractExplicitTableReadName(operation, args);
    if (tableName != null) {
      support.validateExposedTableAccess(methodName, namespace, tableName);
    }
  }

  private Object filterReadResult(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result)
      throws TException {
    OperationMetadata operation = HmsOperationPolicy.describe(methodName);
    if (operation.mutating() || result == null) {
      return result;
    }
    return switch (operation.readResultFilterKind()) {
      case NONE -> result;
      case TABLE_NAME_LIST -> filterTableNameList(methodName, namespace, result);
      case SINGLE_TABLE -> support.filterSingleTableResult(methodName, namespace, result);
      case TABLE_COLLECTION -> support.filterTableCollectionResult(methodName, namespace, result);
    };
  }

  private Object filterTableNameList(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result) {
    if (!(result instanceof List<?> names)) {
      return result;
    }
    List<String> filtered = new ArrayList<>(names.size());
    for (Object candidate : names) {
      if (!(candidate instanceof String tableName)) {
        continue;
      }
      if (!support.federationLayer.isTableExposed(namespace, tableName)) {
        support.recordFilteredObject(methodName, namespace.catalogName(), "table");
        continue;
      }
      filtered.add(tableName);
    }
    return filtered;
  }

  private static String extractExplicitTableReadName(
      OperationMetadata operation,
      Object[] args
  ) {
    if (args == null || args.length == 0) {
      return null;
    }
    return switch (operation.tableExposureMode()) {
      case NONE -> null;
      case TABLE_REQUEST -> args[0] instanceof GetTableRequest request
          ? RoutingSupport.blankToNull(request.getTblName()) : null;
      case TABLE_ARG1 -> args.length >= 2 && args[1] instanceof String tableName
          ? RoutingSupport.blankToNull(tableName) : null;
    };
  }

  CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException {
    return support.resolveRequestNamespace(catName, dbName);
  }

  private CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException {
    try {
      // Lock components can carry Hive's INSERT ... VALUES placeholder, which the generic argument
      // scan would take for the target namespace because it is the first component of the request.
      if (args.length > 0 && args[0] instanceof LockRequest lockRequest) {
        return HivePlaceholderNamespace.resolveLockNamespace(lockRequest, router);
      }
      return support.federationLayer.findNamespaceInArgs(args);
    } catch (MetaException e) {
      observability.metrics().recordRoutingAmbiguous();
      throw e;
    }
  }
}
