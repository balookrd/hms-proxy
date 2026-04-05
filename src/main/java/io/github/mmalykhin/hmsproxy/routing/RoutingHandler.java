package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Terminal handler in the invocation chain. Performs namespace-aware routing to catalog backends.
 */
final class RoutingHandler implements InvocationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingHandler.class);

  private final ProxyConfig config;
  private final CatalogRouter router;
  private final FederationLayer federationLayer;
  private final CompatibilityLayer compatibilityLayer;
  private final ProxyObservability observability;
  private final BackendCallDispatcher dispatcher;
  private final ImpersonationResolver impersonationResolver;

  RoutingHandler(
      ProxyConfig config,
      CatalogRouter router,
      FederationLayer federationLayer,
      CompatibilityLayer compatibilityLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver
  ) {
    this.config = config;
    this.router = router;
    this.federationLayer = federationLayer;
    this.compatibilityLayer = compatibilityLayer;
    this.observability = observability;
    this.dispatcher = dispatcher;
    this.impersonationResolver = impersonationResolver;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    return switch (method.getName()) {
      case "set_ugi" -> handleSetUgi(method, args);
      case "get_all_databases" -> handleGetAllDatabases(method);
      case "get_databases" -> handleGetDatabases(method, args);
      case "get_table_meta" -> handleGetTableMeta(method, args);
      case "get_table_req" -> handleGetTableReq(method, args);
      case "get_table_objects_by_name_req" -> handleGetTablesReq(method, args);
      case "addWriteNotificationLog" -> handleAddWriteNotificationLog(args);
      case "getTablesExt" -> handleGetTablesExt(args);
      case "getAllMaterializedViewObjectsForRewriting" -> handleGetAllMaterializedViewObjectsForRewriting();
      default -> routeByNamespaceOrFail(method, args);
    };
  }

  private Object handleSetUgi(Method method, Object[] args) throws Throwable {
    if (!router.defaultBackend().impersonationEnabled()) {
      return invokeGlobal(method, args);
    }
    ImpersonationContext impersonation = impersonationResolver.resolve().orElseThrow(() ->
        new MetaException("Kerberos caller identity is unavailable for impersonation"));
    if (args != null && args.length > 0 && args[0] instanceof String requestedUser
        && !requestedUser.isBlank()
        && !requestedUser.equals(impersonation.userName())) {
      LOG.warn("requestId={} ignoring client-requested set_ugi user '{}' and using authenticated user '{}'",
          RequestContext.currentRequestId(), requestedUser, impersonation.userName());
    }
    return invokeGlobal(method, new Object[]{impersonation.userName(), impersonation.groupNames()});
  }

  private Object handleGetAllDatabases(Method method) throws Throwable {
    RequestContext.currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (BackendCallDispatcher.FanoutBackendResult<List<String>> fanoutResult : invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<String> result = (List<String>) dispatcher.invokeBackend(
              backend, method, null, impersonation, requestId, false, false);
          return result;
        })) {
      List<String> backendDatabases = fanoutResult.value();
      for (String database : backendDatabases) {
        if (!federationLayer.isDatabaseExposed(fanoutResult.backend().name(), database)) {
          recordFilteredObject(method.getName(), fanoutResult.backend().name(), "database");
          continue;
        }
        databases.add(federationLayer.externalDatabaseName(fanoutResult.backend().name(), database));
      }
    }
    return databases;
  }

  private Object handleGetDatabases(Method method, Object[] args) throws Throwable {
    String pattern = (String) args[0];
    CatalogRouter.ResolvedNamespace resolved = router.resolvePattern(pattern).orElse(null);
    if (resolved != null) {
      RequestContext.currentObservation().recordNamespace(resolved);
      @SuppressWarnings("unchecked")
      List<String> backendDatabases =
          (List<String>) invokeBackend(resolved.backend(), method, new Object[]{resolved.backendDbName()});
      List<String> databases = new ArrayList<>();
      for (String backendDatabase : backendDatabases) {
        if (!federationLayer.isDatabaseExposed(resolved.catalogName(), backendDatabase)) {
          recordFilteredObject(method.getName(), resolved.catalogName(), "database");
          continue;
        }
        databases.add(federationLayer.externalDatabaseName(resolved.catalogName(), backendDatabase));
      }
      return databases;
    }

    RequestContext.currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (BackendCallDispatcher.FanoutBackendResult<List<String>> fanoutResult : invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<String> result = (List<String>) dispatcher.invokeBackend(
              backend, method, new Object[]{pattern}, impersonation, requestId, false, false);
          return result;
        })) {
      List<String> backendDatabases = fanoutResult.value();
      for (String database : backendDatabases) {
        if (!federationLayer.isDatabaseExposed(fanoutResult.backend().name(), database)) {
          recordFilteredObject(method.getName(), fanoutResult.backend().name(), "database");
          continue;
        }
        databases.add(federationLayer.externalDatabaseName(fanoutResult.backend().name(), database));
      }
    }
    return databases;
  }

  private Object handleGetTableMeta(Method method, Object[] args) throws Throwable {
    String dbPattern = (String) args[0];
    String tablePattern = (String) args[1];
    @SuppressWarnings("unchecked")
    List<String> tableTypes = (List<String>) args[2];

    CatalogRouter.ResolvedNamespace resolved = router.resolvePattern(dbPattern).orElse(null);
    if (resolved != null) {
      RequestContext.currentObservation().recordNamespace(resolved);
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults = (List<TableMeta>) invokeBackend(
          resolved.backend(), method, new Object[]{resolved.backendDbName(), tablePattern, tableTypes});
      List<TableMeta> results = new ArrayList<>();
      for (TableMeta result : backendResults) {
        if (!federationLayer.isTableExposed(resolved, result.getTableName())) {
          recordFilteredObject(method.getName(), resolved.catalogName(), "table");
          continue;
        }
        results.add(federationLayer.externalizeTableMeta(result, resolved));
      }
      return results;
    }

    RequestContext.currentObservation().recordFanout();
    List<TableMeta> results = new ArrayList<>();
    for (BackendCallDispatcher.FanoutBackendResult<List<TableMeta>> fanoutResult : invokeFanoutRead(
        method.getName(),
        (backend, impersonation, requestId) -> {
          @SuppressWarnings("unchecked")
          List<TableMeta> result = (List<TableMeta>) dispatcher.invokeBackend(
              backend, method, new Object[]{dbPattern, tablePattern, tableTypes},
              impersonation, requestId, false, false);
          return result;
        })) {
      List<TableMeta> backendResults = fanoutResult.value();
      for (TableMeta result : backendResults) {
        if (!federationLayer.isTableExposed(
            fanoutResult.backend().name(),
            result.getDbName(),
            result.getTableName())) {
          recordFilteredObject(method.getName(), fanoutResult.backend().name(), "table");
          continue;
        }
        results.add(NamespaceTranslator.externalizeTableMeta(
            result,
            router.resolveCatalog(fanoutResult.backend().name(), result.getDbName()),
            federationLayer.preserveBackendCatalogName()));
      }
    }
    return results;
  }

  private Object handleGetTableReq(Method method, Object[] args) throws Throwable {
    GetTableRequest request = (GetTableRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = federationLayer.resolveRequestNamespace(request.getCatName(),
        request.getDbName());
    RequestContext.currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit(method.getName(), request.getCatName(), request.getDbName(), namespace);
    CatalogBackend backend = namespace.backend();
    validateExposedDatabaseAccess(method.getName(), namespace);
    validateExposedTableAccess(method.getName(), namespace, request.getTblName());
    GetTableRequest routedRequest = (GetTableRequest) federationLayer.internalizeTableRequest(request, namespace);
    Object result = invokeBackendRequest(backend, routedRequest, method.getName());
    result = filterSingleTableResult(method.getName(), namespace, result);
    return federationLayer.externalizeResult(result, namespace);
  }

  private Object handleGetTablesReq(Method method, Object[] args) throws Throwable {
    GetTablesRequest request = (GetTablesRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = federationLayer.resolveRequestNamespace(request.getCatName(),
        request.getDbName());
    RequestContext.currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit(method.getName(), request.getCatName(), request.getDbName(), namespace);
    CatalogBackend backend = namespace.backend();
    validateExposedDatabaseAccess(method.getName(), namespace);
    GetTablesRequest routedRequest = (GetTablesRequest) federationLayer.internalizeTablesRequest(request, namespace);
    Object result = invokeBackendRequest(backend, routedRequest, method.getName());
    result = filterTableCollectionResult(method.getName(), namespace, result);
    return federationLayer.externalizeResult(result, namespace);
  }

  private Object handleAddWriteNotificationLog(Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = (String) request.getClass().getMethod("getDb").invoke(request);
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit("add_write_notification_log", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    validateCatalogAccess(backend, "add_write_notification_log", namespace.backendDbName());
    if (!backend.runtimeProfile().isHortonworks()) {
      throw new MetaException(
          "Hortonworks add_write_notification_log requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    Object routedRequest = request.getClass().getConstructor(request.getClass()).newInstance(request);
    routedRequest.getClass().getMethod("setDb", String.class).invoke(routedRequest, namespace.backendDbName());
    return invokeBackendNamed(backend, "add_write_notification_log", routedRequest);
  }

  private Object handleGetTablesExt(Object[] args) throws Throwable {
    Object request = args[0];
    String catalogName = (String) request.getClass().getMethod("getCatalog").invoke(request);
    String dbName = (String) request.getClass().getMethod("getDatabase").invoke(request);
    CatalogRouter.ResolvedNamespace namespace = resolveRequestNamespace(catalogName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit("get_tables_ext", catalogName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    if (!backend.runtimeProfile().isHortonworks()) {
      throw new MetaException(
          "Hortonworks get_tables_ext requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    validateCatalogAccess(backend, "get_tables_ext", namespace.backendDbName());
    validateExposedDatabaseAccess("get_tables_ext", namespace);
    Object routedRequest = request.getClass().getConstructor(request.getClass()).newInstance(request);
    routedRequest.getClass().getMethod("setDatabase", String.class).invoke(routedRequest, namespace.backendDbName());
    String internalCatalog = NamespaceTranslator.internalCatalogName(catalogName, dbName, namespace,
        federationLayer.preserveBackendCatalogName());
    routedRequest.getClass().getMethod("setCatalog", String.class)
        .invoke(routedRequest, internalCatalog == null ? catalogName : internalCatalog);
    return filterTableCollectionResult(
        "get_tables_ext",
        namespace,
        invokeBackendNamed(backend, "get_tables_ext", routedRequest));
  }

  private Object handleGetAllMaterializedViewObjectsForRewriting() throws Throwable {
    CatalogBackend backend = router.defaultBackend();
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    RequestContext.currentObservation().markDefaultCatalogRoute();
    observability.metrics().recordDefaultCatalogRoute("get_all_materialized_view_objects_for_rewriting");
    if (!backend.runtimeProfile().isHortonworks()) {
      throw new MetaException(
          "Hortonworks get_all_materialized_view_objects_for_rewriting requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    validateCatalogAccess(backend, "get_all_materialized_view_objects_for_rewriting", null);
    Object result = invokeBackendByName(backend, "get_all_materialized_view_objects_for_rewriting",
        new Class<?>[0], new Object[0]);
    if (result instanceof List<?> tables) {
      List<Object> externalized = new ArrayList<>(tables.size());
      for (Object table : tables) {
        String dbName = NamespaceTranslator.extractDbName(table);
        CatalogRouter.ResolvedNamespace namespace = router.resolveCatalog(config.defaultCatalog(), dbName);
        String tableName = extractTableName(table);
        if (!federationLayer.isDatabaseExposed(namespace)
            || (tableName != null && !federationLayer.isTableExposed(namespace, tableName))) {
          recordFilteredObject("get_all_materialized_view_objects_for_rewriting", namespace.catalogName(), "table");
          continue;
        }
        externalized.add(federationLayer.externalizeResult(table, namespace));
      }
      return externalized;
    }
    return result;
  }

  private Object routeByNamespaceOrFail(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe(methodName);
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

  private Object invokeGlobal(Method method, Object[] args) throws Throwable {
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
      validateCatalogAccess(router.defaultBackend(), method.getName(), null);
      return invokeBackend(router.defaultBackend(), method, args);
    }
    if (!router.singleCatalog()) {
      throw new MetaException("Operation " + method.getName()
          + " requires explicit namespace ownership for deterministic routing; use explicit catalog.db naming"
          + " or a catalog-aware request so the proxy can fail safely instead of guessing a target catalog");
    }
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    observability.metrics().recordDefaultCatalogRoute(method.getName());
    validateCatalogAccess(router.defaultBackend(), method.getName(), null);
    return invokeBackend(router.defaultBackend(), method, args);
  }

  private Object routeByDbStringArgument(Method method, Object[] args) throws Throwable {
    if (!(args[0] instanceof String dbName)) {
      return invokeGlobal(method, args);
    }
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    validateCatalogAccess(namespace.backend(), method.getName(), namespace.backendDbName());
    validateReadExposure(method.getName(), namespace, args);
    Object[] routedArgs = federationLayer.internalizeDbStringArguments(args, namespace);
    Object result = invokeBackend(namespace.backend(), method, routedArgs);
    result = filterReadResult(method.getName(), namespace, result);
    return federationLayer.externalizeResult(result, namespace);
  }

  private Object routeByDbFirstStringArguments(Method method, Object[] args) throws Throwable {
    if (args.length <= 1 || !(args[0] instanceof String dbName) || !(args[1] instanceof String)) {
      return invokeGlobal(method, args);
    }
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit(method.getName(), dbName, namespace);
    validateCatalogAccess(namespace.backend(), method.getName(), namespace.backendDbName());
    validateReadExposure(method.getName(), namespace, args);
    Object[] routedArgs = federationLayer.internalizeDbStringArguments(args, namespace);
    Object result = invokeBackend(namespace.backend(), method, routedArgs);
    result = filterReadResult(method.getName(), namespace, result);
    return federationLayer.externalizeResult(result, namespace);
  }

  private Object routeByExtractedNamespace(Method method, Object[] args) throws Throwable {
    CatalogRouter.ResolvedNamespace extractedNamespace = findNamespaceInArgs(args);
    if (extractedNamespace == null) {
      return invokeGlobal(method, args);
    }
    RequestContext.currentObservation().recordNamespace(extractedNamespace);
    validateCatalogAccess(extractedNamespace.backend(), method.getName(), extractedNamespace.backendDbName());
    validateReadExposure(method.getName(), extractedNamespace, args);
    Object[] routedArgs = federationLayer.internalizeObjectArguments(args, extractedNamespace);
    Object result = invokeBackend(extractedNamespace.backend(), method, routedArgs);
    result = filterReadResult(method.getName(), extractedNamespace, result);
    return federationLayer.externalizeResult(result, extractedNamespace);
  }

  private void validateReadExposure(String methodName, CatalogRouter.ResolvedNamespace namespace, Object[] args)
      throws TException {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe(methodName);
    if (operation.mutating()) {
      return;
    }
    validateExposedDatabaseAccess(methodName, namespace);
    String tableName = extractExplicitTableReadName(operation, args);
    if (tableName != null) {
      validateExposedTableAccess(methodName, namespace, tableName);
    }
  }

  private void validateExposedDatabaseAccess(String methodName, CatalogRouter.ResolvedNamespace namespace)
      throws NoSuchObjectException {
    if (federationLayer.isDatabaseExposed(namespace)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "database");
    throw new NoSuchObjectException(
        "Database '" + namespace.externalDbName() + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  private void validateExposedTableAccess(
      String methodName,
      CatalogRouter.ResolvedNamespace namespace,
      String tableName
  ) throws NoSuchObjectException {
    if (federationLayer.isTableExposed(namespace, tableName)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "table");
    throw new NoSuchObjectException(
        "Table '" + namespace.externalDbName() + "." + tableName + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  private Object filterReadResult(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result)
      throws TException {
    HmsOperationRegistry.OperationMetadata operation = HmsOperationRegistry.describe(methodName);
    if (operation.mutating() || result == null) {
      return result;
    }
    return switch (operation.readResultFilterKind()) {
      case NONE -> result;
      case TABLE_NAME_LIST -> filterTableNameList(methodName, namespace, result);
      case SINGLE_TABLE -> filterSingleTableResult(methodName, namespace, result);
      case TABLE_COLLECTION -> filterTableCollectionResult(methodName, namespace, result);
    };
  }

  private Object filterSingleTableResult(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result)
      throws TException {
    Object tableCarrier = result instanceof Table ? result : tryInvokeNoArgs(result, "getTable");
    String tableName = extractTableName(tableCarrier);
    if (tableName != null) {
      validateExposedTableAccess(methodName, namespace, tableName);
    }
    return result;
  }

  private Object filterTableCollectionResult(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result)
      throws TException {
    if (result instanceof List<?> list) {
      return filterTableObjectList(methodName, namespace, list);
    }
    Object tables = tryInvokeNoArgs(result, "getTables");
    if (tables instanceof List<?> list) {
      tryInvokeSetList(result, "setTables", filterTableObjectList(methodName, namespace, list));
    }
    return result;
  }

  private List<Object> filterTableObjectList(String methodName, CatalogRouter.ResolvedNamespace namespace, List<?> tables) {
    List<Object> filtered = new ArrayList<>(tables.size());
    for (Object candidate : tables) {
      String tableName = extractTableName(candidate);
      if (tableName != null && !federationLayer.isTableExposed(namespace, tableName)) {
        recordFilteredObject(methodName, namespace.catalogName(), "table");
        continue;
      }
      filtered.add(candidate);
    }
    return filtered;
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
      if (!federationLayer.isTableExposed(namespace, tableName)) {
        recordFilteredObject(methodName, namespace.catalogName(), "table");
        continue;
      }
      filtered.add(tableName);
    }
    return filtered;
  }

  private void recordFilteredObject(String methodName, String catalogName, String objectType) {
    observability.metrics().recordFilteredObject(methodName, catalogName, objectType);
  }

  private static String extractExplicitTableReadName(
      HmsOperationRegistry.OperationMetadata operation,
      Object[] args
  ) {
    if (args == null || args.length == 0) {
      return null;
    }
    return switch (operation.tableExposureMode()) {
      case NONE -> null;
      case TABLE_REQUEST -> args[0] instanceof GetTableRequest request ? blankToNull(request.getTblName()) : null;
      case TABLE_ARG1 -> args.length >= 2 && args[1] instanceof String tableName ? blankToNull(tableName) : null;
    };
  }

  private static String extractTableName(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Table table) {
      return blankToNull(table.getTableName());
    }
    String directName = blankToNull(readStringProperty(value, "getTableName"));
    if (directName != null) {
      return directName;
    }
    directName = blankToNull(readStringProperty(value, "getTblName"));
    if (directName != null) {
      return directName;
    }
    directName = blankToNull(readStringProperty(value, "getName"));
    if (directName != null) {
      return directName;
    }
    String fullTableName = blankToNull(readStringProperty(value, "getFullTableName"));
    if (fullTableName == null) {
      fullTableName = blankToNull(readStringProperty(value, "getFull_table_name"));
    }
    if (fullTableName == null) {
      return null;
    }
    int separator = fullTableName.lastIndexOf('.');
    return separator >= 0 && separator + 1 < fullTableName.length()
        ? blankToNull(fullTableName.substring(separator + 1))
        : blankToNull(fullTableName);
  }

  private static Object tryInvokeNoArgs(Object target, String methodName) {
    if (target == null) {
      return null;
    }
    try {
      return target.getClass().getMethod(methodName).invoke(target);
    } catch (ReflectiveOperationException ignored) {
      return null;
    }
  }

  private static void tryInvokeSetList(Object target, String methodName, List<?> values) {
    if (target == null) {
      return;
    }
    try {
      target.getClass().getMethod(methodName, List.class).invoke(target, values);
    } catch (ReflectiveOperationException ignored) {
    }
  }

  private static String readStringProperty(Object target, String methodName) {
    Object value = tryInvokeNoArgs(target, methodName);
    return value instanceof String stringValue ? stringValue : null;
  }

  private static String blankToNull(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }

  private void validateCatalogAccess(CatalogBackend backend, String methodName, String backendDbName)
      throws MetaException {
    CatalogAccessModeGuard.validate(config.catalogs().get(backend.name()), methodName, backendDbName);
  }

  private void recordDefaultCatalogRouteIfImplicit(
      String methodName,
      String dbName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    if (namespace.catalogName().equals(config.defaultCatalog()) && router.resolvePattern(dbName).isEmpty()) {
      RequestContext.currentObservation().markDefaultCatalogRoute();
      observability.metrics().recordDefaultCatalogRoute(methodName);
    }
  }

  private void recordDefaultCatalogRouteIfImplicit(
      String methodName,
      String catName,
      String dbName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    if ((catName == null || catName.isBlank())
        && namespace.catalogName().equals(config.defaultCatalog())
        && router.resolvePattern(dbName).isEmpty()) {
      RequestContext.currentObservation().markDefaultCatalogRoute();
      observability.metrics().recordDefaultCatalogRoute(methodName);
    }
  }

  private CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException {
    try {
      return federationLayer.findNamespaceInArgs(args);
    } catch (MetaException e) {
      observability.metrics().recordRoutingAmbiguous();
      throw e;
    }
  }

  CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException {
    try {
      return federationLayer.resolveRequestNamespace(catName, dbName);
    } catch (MetaException e) {
      if (e.getMessage() != null && e.getMessage().contains("conflicting catalog and database namespace")) {
        observability.metrics().recordRoutingAmbiguous();
      } else if (catName != null && !catName.isBlank() && LOG.isDebugEnabled()
          && federationLayer.resolveCatalogIfKnown(catName, dbName).isEmpty()) {
        LOG.debug("requestId={} ignoring unknown request catalog '{}' and resolving by dbName='{}'",
            RequestContext.currentRequestId(), catName, dbName);
      }
      throw e;
    }
  }

  // --- Backend invocation bridges ---

  private Object invokeBackend(CatalogBackend backend, Method method, Object[] args) throws Throwable {
    return dispatcher.invokeBackend(
        backend, method, args,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId(),
        true, true);
  }

  private Object invokeBackendRequest(CatalogBackend backend, Object request, String methodName) throws Throwable {
    return dispatcher.invokeBackendRequest(
        backend, request, methodName,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  private Object invokeBackendNamed(CatalogBackend backend, String methodName, Object request) throws Throwable {
    return dispatcher.invokeBackendByName(
        backend, methodName,
        new Class<?>[]{request.getClass()}, new Object[]{request},
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  private Object invokeBackendByName(
      CatalogBackend backend,
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args
  ) throws Throwable {
    return dispatcher.invokeBackendByName(
        backend, methodName, parameterTypes, args,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  private <T> List<BackendCallDispatcher.FanoutBackendResult<T>> invokeFanoutRead(
      String methodName,
      BackendCallDispatcher.FanoutBackendCall<T> call
  ) throws Throwable {
    return dispatcher.invokeFanoutRead(
        methodName, call,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }
}
