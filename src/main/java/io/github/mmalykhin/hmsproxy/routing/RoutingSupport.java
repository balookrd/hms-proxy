package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.mmalykhin.hmsproxy.security.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.security.ranger.MetadataAuthorizer;
import io.github.mmalykhin.hmsproxy.security.ranger.NoOpMetadataAuthorizer;

/**
 * Shared collaborators and helpers used by the namespace-aware routing handler and its
 * special-case sub-handlers.
 */
final class RoutingSupport {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingSupport.class);

  volatile ProxyConfig config;
  final CatalogRouter router;
  final FederationOperations federationLayer;
  final ProxyObservability observability;
  final BackendCallDispatcher dispatcher;
  final ImpersonationResolver impersonationResolver;
  final DatabaseListCache databaseListCache;
  final DatabaseMetadataCache databaseMetadataCache;
  final TableMetadataCache tableMetadataCache;
  final PartitionMetadataCache partitionMetadataCache;
  final MetadataAuthorizer metadataAuthorizer;
  final CacheInvalidator cacheInvalidator;
  private volatile ExternalTableLocationRewriter externalTableLocationRewriter;

  RoutingSupport(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      DatabaseListCache databaseListCache
  ) {
    this(
        config,
        router,
        federationLayer,
        observability,
        dispatcher,
        impersonationResolver,
        databaseListCache,
        new DatabaseMetadataCache(config != null && config.latencyRouting() != null
            ? config.latencyRouting().databaseMetadataCache()
            : io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig.disabled(), databaseListCache),
        NoOpMetadataAuthorizer.INSTANCE);
  }

  RoutingSupport(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache
  ) {
    this(
        config,
        router,
        federationLayer,
        observability,
        dispatcher,
        impersonationResolver,
        databaseListCache,
        databaseMetadataCache,
        NoOpMetadataAuthorizer.INSTANCE);
  }

  RoutingSupport(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      MetadataAuthorizer metadataAuthorizer
  ) {
    this(
        config,
        router,
        federationLayer,
        observability,
        dispatcher,
        impersonationResolver,
        databaseListCache,
        databaseMetadataCache,
        new TableMetadataCache(config != null && config.latencyRouting() != null
            ? config.latencyRouting().tableMetadataCache()
            : io.github.mmalykhin.hmsproxy.config.routing.TableMetadataCacheConfig.disabled()),
        new PartitionMetadataCache(config != null && config.latencyRouting() != null
            ? config.latencyRouting().partitionMetadataCache()
            : io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig.disabled()),
        metadataAuthorizer);
  }

  RoutingSupport(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      TableMetadataCache tableMetadataCache,
      PartitionMetadataCache partitionMetadataCache,
      MetadataAuthorizer metadataAuthorizer
  ) {
    this(config, router, federationLayer, observability, dispatcher, impersonationResolver,
        databaseListCache, databaseMetadataCache, tableMetadataCache, partitionMetadataCache,
        metadataAuthorizer, null);
  }

  RoutingSupport(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      TableMetadataCache tableMetadataCache,
      PartitionMetadataCache partitionMetadataCache,
      MetadataAuthorizer metadataAuthorizer,
      CacheInvalidator cacheInvalidator
  ) {
    this.config = config;
    this.router = router;
    this.federationLayer = federationLayer;
    this.observability = observability;
    this.dispatcher = dispatcher;
    this.impersonationResolver = impersonationResolver;
    this.databaseListCache = databaseListCache;
    this.databaseMetadataCache = databaseMetadataCache;
    this.tableMetadataCache = tableMetadataCache != null
        ? tableMetadataCache
        : (config != null && config.latencyRouting() != null
            ? new TableMetadataCache(config.latencyRouting().tableMetadataCache())
            : new TableMetadataCache(io.github.mmalykhin.hmsproxy.config.routing.TableMetadataCacheConfig.disabled()));
    this.partitionMetadataCache = partitionMetadataCache != null
        ? partitionMetadataCache
        : (config != null && config.latencyRouting() != null
            ? new PartitionMetadataCache(config.latencyRouting().partitionMetadataCache())
            : new PartitionMetadataCache(io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig.disabled()));
    this.metadataAuthorizer = metadataAuthorizer == null ? NoOpMetadataAuthorizer.INSTANCE : metadataAuthorizer;
    this.cacheInvalidator = cacheInvalidator != null
        ? cacheInvalidator
        : new LocalCacheInvalidator(databaseListCache, databaseMetadataCache, this.tableMetadataCache, this.partitionMetadataCache);
    this.externalTableLocationRewriter = newExternalTableLocationRewriter(config);
  }

  ExternalTableLocationRewriter externalTableLocationRewriter() {
    return externalTableLocationRewriter;
  }

  private static ExternalTableLocationRewriter newExternalTableLocationRewriter(ProxyConfig config) {
    return config != null && config.federation() != null
        ? new ExternalTableLocationRewriter(config.federation())
        : null;
  }

  void invalidateTable(String catalogName, String backendDbName, String tableName) {
    if (cacheInvalidator != null) {
      cacheInvalidator.invalidateTable(catalogName, backendDbName, tableName);
    }
  }

  void invalidateDatabase(String catalogName, String backendDbName) {
    if (cacheInvalidator != null) {
      cacheInvalidator.invalidateDatabase(catalogName, backendDbName);
    }
  }

  void invalidateCatalog(String catalogName) {
    if (cacheInvalidator != null) {
      cacheInvalidator.invalidateCatalog(catalogName);
    }
  }

  // --- Backend invocation bridges ---

  Object invokeDirect(CatalogBackend backend, Method method, Object[] args) throws Throwable {
    return dispatcher.invokeDirect(
        backend, method, args,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId(),
        true, true);
  }

  /**
   * Backend call that admission control must not refuse: the lock lifecycle
   * {@link IcebergTablePointerGuard} runs around a repair. A rate-limit or circuit-breaker
   * rejection landing between {@code lock} and {@code unlock} would strand an EXCLUSIVE lock on
   * the table, which blocks every later commit on it - far worse than the two calls it saves.
   */
  Object invokeDirectUnmetered(CatalogBackend backend, Method method, Object[] args) throws Throwable {
    return dispatcher.invokeDirect(
        backend, method, args,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId(),
        false, false);
  }

  Object invokeViaRequest(CatalogBackend backend, Object request, String methodName) throws Throwable {
    return dispatcher.invokeViaRequest(
        backend, request, methodName,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  Object invokeBackendNamed(CatalogBackend backend, String methodName, Object request) throws Throwable {
    return dispatcher.invokeByReflection(
        backend, methodName,
        new Class<?>[]{request.getClass()}, new Object[]{request},
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  Object invokeByReflection(
      CatalogBackend backend,
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args
  ) throws Throwable {
    return dispatcher.invokeByReflection(
        backend, methodName, parameterTypes, args,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  <T> List<FanoutExecutor.FanoutBackendResult<T>> invokeFanoutRead(
      String methodName,
      FanoutExecutor.FanoutBackendCall<T> call
  ) throws Throwable {
    return dispatcher.invokeFanoutRead(
        methodName, call,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId());
  }

  // --- Observability helpers ---

  void recordFilteredObject(String methodName, String catalogName, String objectType) {
    observability.metrics().recordFilteredObject(methodName, catalogName, objectType);
  }

  void recordDefaultCatalogRouteIfImplicit(
      String methodName,
      String dbName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    if (namespace.catalogName().equals(config.defaultCatalog()) && router.resolvePattern(dbName).isEmpty()) {
      RequestContext.currentObservation().markDefaultCatalogRoute();
      observability.metrics().recordDefaultCatalogRoute(methodName);
    }
  }

  void recordDefaultCatalogRouteIfImplicit(
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

  // --- Validation helpers ---

  void validateCatalogAccess(CatalogBackend backend, String methodName, String backendDbName)
      throws MetaException {
    CatalogAccessModeGuard.validate(config.catalogs().get(backend.name()), methodName, backendDbName);
  }

  ImpersonationContext currentImpersonation() {
    try {
      return impersonationResolver.resolve().orElse(null);
    } catch (Exception e) {
      LOG.warn("Failed to resolve impersonation context: {}", e.getMessage());
      return null;
    }
  }

  boolean isServicePrincipalBypass() {
    return impersonationResolver != null && impersonationResolver.isAuthenticatedServicePrincipal();
  }

  ImpersonationContext resolveImpersonationOrThrowMeta(String methodName) throws MetaException {
    if (impersonationResolver == null) {
      return null;
    }
    try {
      return impersonationResolver.resolve().orElse(null);
    } catch (Exception e) {
      LOG.warn("Failed to resolve impersonation context during {}: {}", methodName, e.getMessage());
      throw new MetaException("Access denied: unable to resolve authenticated caller identity");
    }
  }

  private ImpersonationContext resolveImpersonationOrThrow(Method method) throws TException {
    if (impersonationResolver == null) {
      return null;
    }
    try {
      return impersonationResolver.resolve().orElse(null);
    } catch (Exception e) {
      LOG.warn("Failed to resolve impersonation context during {}: {}",
          method != null ? method.getName() : "unknown", e.getMessage());
      throwAccessDeniedException(method, "Access denied: unable to resolve authenticated caller identity");
      return null;
    }
  }

  java.util.List<String> resolveGroups(String userName) {
    return impersonationResolver != null
        ? impersonationResolver.resolveGroups(userName)
        : new io.github.mmalykhin.hmsproxy.security.groups.UserGroupResolver().resolveGroups(userName);
  }

  boolean isDatabaseAccessible(String methodName, CatalogRouter.ResolvedNamespace namespace) {
    if (HivePlaceholderNamespace.isPlaceholderDbName(namespace.backendDbName())
        || SparkPlaceholderNamespace.isSparkReservedDbName(namespace.backendDbName())) {
      return false;
    }
    if (!isServicePrincipalBypass()) {
      ImpersonationContext impersonation;
      try {
        impersonation = impersonationResolver != null ? impersonationResolver.resolve().orElse(null) : null;
      } catch (Exception e) {
        LOG.warn("Failed to resolve impersonation context during {} for catalog '{}' db='{}'; "
                + "denying database access (fail-closed): {}",
            methodName, namespace.catalogName(), namespace.backendDbName(), e.getMessage());
        recordFilteredObject(methodName, namespace.catalogName(), "database");
        return false;
      }
      if (!metadataAuthorizer.isDatabaseAllowed(namespace.catalogName(), namespace.backendDbName(), impersonation)) {
        recordFilteredObject(methodName, namespace.catalogName(), "database");
        return false;
      }
    }
    if (!federationLayer.isDatabaseExposed(namespace)) {
      recordFilteredObject(methodName, namespace.catalogName(), "database");
      return false;
    }
    return true;
  }

  boolean isTableAccessible(String methodName, CatalogRouter.ResolvedNamespace namespace, String tableName) {
    if (HivePlaceholderNamespace.isPlaceholderTable(namespace.backendDbName(), tableName)
        || SparkPlaceholderNamespace.isSparkReservedTable(namespace.backendDbName(), tableName)) {
      return false;
    }
    if (!isServicePrincipalBypass()) {
      ImpersonationContext impersonation;
      try {
        impersonation = impersonationResolver != null ? impersonationResolver.resolve().orElse(null) : null;
      } catch (Exception e) {
        LOG.warn("Failed to resolve impersonation context during {} for catalog '{}' db='{}' table='{}'; "
                + "denying table access (fail-closed): {}",
            methodName, namespace.catalogName(), namespace.backendDbName(), tableName, e.getMessage());
        recordFilteredObject(methodName, namespace.catalogName(), "table");
        return false;
      }
      if (!metadataAuthorizer.isTableAllowed(namespace.catalogName(), namespace.backendDbName(), tableName, impersonation)) {
        recordFilteredObject(methodName, namespace.catalogName(), "table");
        return false;
      }
    }
    if (!federationLayer.isTableExposed(namespace, tableName)) {
      recordFilteredObject(methodName, namespace.catalogName(), "table");
      return false;
    }
    return true;
  }

  boolean maskUnauthorizedAsNotFound(String catalogName) {
    if (config.ranger() == null || !config.ranger().enabled()) {
      return io.github.mmalykhin.hmsproxy.config.security.CatalogRangerConfig.DEFAULT_MASK_UNAUTHORIZED_AS_NOT_FOUND;
    }
    return config.ranger().forCatalog(catalogName).maskUnauthorizedAsNotFound();
  }

  void validateExposedDatabaseAccess(String methodName, CatalogRouter.ResolvedNamespace namespace)
      throws TException {
    if (HivePlaceholderNamespace.isPlaceholderDbName(namespace.backendDbName())) {
      recordFilteredObject(methodName, namespace.catalogName(), "database");
      throw new NoSuchObjectException(namespace.externalDbName());
    }
    if (SparkPlaceholderNamespace.isSparkReservedDbName(namespace.backendDbName())) {
      recordFilteredObject(methodName, namespace.catalogName(), "database");
      throw SparkPlaceholderNamespace.newNoSuchDatabaseException(namespace.externalDbName());
    }
    if (!isServicePrincipalBypass()) {
      ImpersonationContext impersonation = resolveImpersonationOrThrowMeta(methodName);
      if (!metadataAuthorizer.isDatabaseAllowed(namespace.catalogName(), namespace.backendDbName(), impersonation)) {
        recordFilteredObject(methodName, namespace.catalogName(), "database");
        if (maskUnauthorizedAsNotFound(namespace.catalogName())) {
          throw new NoSuchObjectException(
              "Database '" + namespace.externalDbName() + "' is not accessible in proxy catalog '"
                  + namespace.catalogName() + "'");
        }
        throw new MetaException(
            "Access denied: Database '" + namespace.externalDbName() + "' is not accessible in proxy catalog '"
                + namespace.catalogName() + "'");
      }
    }
    if (federationLayer.isDatabaseExposed(namespace)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "database");
    throw new NoSuchObjectException(
        "Database '" + namespace.externalDbName() + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  void validateExposedDatabaseAccess(Method method, CatalogRouter.ResolvedNamespace namespace)
      throws TException {
    String methodName = method != null ? method.getName() : "unknown";
    if (HivePlaceholderNamespace.isPlaceholderDbName(namespace.backendDbName())) {
      recordFilteredObject(methodName, namespace.catalogName(), "database");
      throwAppropriateException(method, namespace.externalDbName());
    }
    if (SparkPlaceholderNamespace.isSparkReservedDbName(namespace.backendDbName())) {
      recordFilteredObject(methodName, namespace.catalogName(), "database");
      throwAppropriateException(method, "Database " + namespace.externalDbName() + " not found");
    }
    if (!isServicePrincipalBypass()) {
      ImpersonationContext impersonation = resolveImpersonationOrThrow(method);
      if (!metadataAuthorizer.isDatabaseAllowed(namespace.catalogName(), namespace.backendDbName(), impersonation)) {
        recordFilteredObject(methodName, namespace.catalogName(), "database");
        if (maskUnauthorizedAsNotFound(namespace.catalogName())) {
          throwAppropriateException(method,
              "Database '" + namespace.externalDbName() + "' is not accessible in proxy catalog '"
                  + namespace.catalogName() + "'");
        }
        throwAccessDeniedException(method,
            "Access denied: Database '" + namespace.externalDbName() + "' is not accessible in proxy catalog '"
                + namespace.catalogName() + "'");
      }
    }
    if (federationLayer.isDatabaseExposed(namespace)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "database");
    throwAppropriateException(method,
        "Database '" + namespace.externalDbName() + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  void validateExposedTableAccess(
      String methodName,
      CatalogRouter.ResolvedNamespace namespace,
      String tableName
  ) throws TException {
    if (HivePlaceholderNamespace.isPlaceholderTable(namespace.backendDbName(), tableName)) {
      recordFilteredObject(methodName, namespace.catalogName(), "table");
      throw new NoSuchObjectException(
          "Table '" + namespace.externalDbName() + "." + tableName + "' is not accessible in proxy catalog '"
              + namespace.catalogName() + "'");
    }
    if (SparkPlaceholderNamespace.isSparkReservedTable(namespace.backendDbName(), tableName)) {
      recordFilteredObject(methodName, namespace.catalogName(), "table");
      throw SparkPlaceholderNamespace.newNoSuchTableException(namespace.catalogName(), namespace.externalDbName(), tableName);
    }
    if (!isServicePrincipalBypass()) {
      ImpersonationContext impersonation = resolveImpersonationOrThrowMeta(methodName);
      if (!metadataAuthorizer.isTableAllowed(namespace.catalogName(), namespace.backendDbName(), tableName, impersonation)) {
        recordFilteredObject(methodName, namespace.catalogName(), "table");
        if (maskUnauthorizedAsNotFound(namespace.catalogName())) {
          throw new NoSuchObjectException(
              "Table '" + namespace.externalDbName() + "." + tableName + "' is not accessible in proxy catalog '"
                  + namespace.catalogName() + "'");
        }
        throw new MetaException(
            "Access denied: Table '" + namespace.externalDbName() + "." + tableName + "' is not accessible in proxy catalog '"
                + namespace.catalogName() + "'");
      }
    }
    if (federationLayer.isTableExposed(namespace, tableName)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "table");
    throw new NoSuchObjectException(
        "Table '" + namespace.externalDbName() + "." + tableName + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  void validateExposedTableAccess(
      Method method,
      CatalogRouter.ResolvedNamespace namespace,
      String tableName
  ) throws TException {
    String methodName = method != null ? method.getName() : "unknown";
    if (HivePlaceholderNamespace.isPlaceholderTable(namespace.backendDbName(), tableName)) {
      recordFilteredObject(methodName, namespace.catalogName(), "table");
      throwAppropriateException(method, namespace.externalDbName() + "." + tableName + " table not found");
    }
    if (SparkPlaceholderNamespace.isSparkReservedTable(namespace.backendDbName(), tableName)) {
      recordFilteredObject(methodName, namespace.catalogName(), "table");
      throwAppropriateException(method,
          namespace.catalogName() + "." + namespace.externalDbName() + "." + tableName + " table not found");
    }
    if (!isServicePrincipalBypass()) {
      ImpersonationContext impersonation = resolveImpersonationOrThrow(method);
      if (!metadataAuthorizer.isTableAllowed(namespace.catalogName(), namespace.backendDbName(), tableName, impersonation)) {
        recordFilteredObject(methodName, namespace.catalogName(), "table");
        if (maskUnauthorizedAsNotFound(namespace.catalogName())) {
          throwAppropriateException(method,
              "Table '" + namespace.externalDbName() + "." + tableName + "' is not accessible in proxy catalog '"
                  + namespace.catalogName() + "'");
        }
        throwAccessDeniedException(method,
            "Access denied: Table '" + namespace.externalDbName() + "." + tableName + "' is not accessible in proxy catalog '"
                + namespace.catalogName() + "'");
      }
    }
    if (federationLayer.isTableExposed(namespace, tableName)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "table");
    throwAppropriateException(method,
        "Table '" + namespace.externalDbName() + "." + tableName + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  private static void throwAppropriateException(Method method, String message) throws TException {
    if (method != null) {
      for (Class<?> exc : method.getExceptionTypes()) {
        if (NoSuchObjectException.class.isAssignableFrom(exc)) {
          throw new NoSuchObjectException(message);
        }
      }
      for (Class<?> exc : method.getExceptionTypes()) {
        if (MetaException.class.isAssignableFrom(exc)) {
          throw new MetaException(message);
        }
      }
    }
    throw new NoSuchObjectException(message);
  }

  private static void throwAccessDeniedException(Method method, String message) throws TException {
    if (method != null) {
      for (Class<?> exc : method.getExceptionTypes()) {
        if (MetaException.class.isAssignableFrom(exc)) {
          throw new MetaException(message);
        }
      }
      for (Class<?> exc : method.getExceptionTypes()) {
        if (NoSuchObjectException.class.isAssignableFrom(exc)) {
          throw new NoSuchObjectException(message);
        }
      }
    }
    throw new MetaException(message);
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

  // --- Result filtering ---

  /**
   * Listing shape shared by the database and table listing handlers, on both the resolved-namespace
   * and the fanout path: hidden objects are counted once against the catalog that returned them and
   * dropped, survivors are translated into their client-visible form. Keeping the shape here stops
   * an exposure-policy change from having to be repeated per handler.
   */
  <S, R> List<R> filterExposed(
      String methodName,
      String catalogName,
      String objectType,
      List<S> backendObjects,
      Predicate<S> exposed,
      Function<S, R> externalize
  ) {
    List<R> visible = new ArrayList<>(backendObjects.size());
    for (S backendObject : backendObjects) {
      if (!exposed.test(backendObject)) {
        recordFilteredObject(methodName, catalogName, objectType);
        continue;
      }
      visible.add(externalize.apply(backendObject));
    }
    return visible;
  }

  List<String> exposedDatabaseNames(String methodName, String catalogName, List<String> backendDatabases) {
    List<String> authorizedDatabases;
    if (isServicePrincipalBypass()) {
      authorizedDatabases = backendDatabases;
    } else {
      ImpersonationContext impersonation;
      try {
        impersonation = impersonationResolver != null ? impersonationResolver.resolve().orElse(null) : null;
      } catch (Exception e) {
        LOG.warn("Failed to resolve impersonation context during {} for catalog '{}'; "
                + "returning empty database list (fail-closed): {}",
            methodName, catalogName, e.getMessage());
        return List.of();
      }
      authorizedDatabases = metadataAuthorizer.filterDatabases(catalogName, backendDatabases, impersonation);
    }
    return filterExposed(
        methodName,
        catalogName,
        "database",
        authorizedDatabases,
        backendDatabase -> federationLayer.isDatabaseExposed(catalogName, backendDatabase),
        backendDatabase -> federationLayer.externalDatabaseName(catalogName, backendDatabase));
  }

  Object filterSingleTableResult(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result)
      throws TException {
    Object tableCarrier = result instanceof Table ? result : ThriftReflectionCache.invokeGetter(result, "getTable");
    String tableName = extractTableName(tableCarrier);
    if (tableName != null) {
      validateExposedTableAccess(methodName, namespace, tableName);
    }
    return result;
  }

  Object filterTableCollectionResult(String methodName, CatalogRouter.ResolvedNamespace namespace, Object result)
      throws TException {
    if (result instanceof List<?> list) {
      return filterTableObjectList(methodName, namespace, list);
    }
    Object tables = ThriftReflectionCache.invokeGetter(result, "getTables");
    if (tables instanceof List<?> list) {
      ThriftReflectionCache.invokeListSetter(result, "setTables", filterTableObjectList(methodName, namespace, list));
    }
    return result;
  }

  List<Object> filterTableObjectList(String methodName, CatalogRouter.ResolvedNamespace namespace, List<?> tables) {
    return filterAuthorizedTables(
        methodName,
        namespace.catalogName(),
        namespace.backendDbName(),
        new ArrayList<>(tables),
        RoutingSupport::extractTableName);
  }

  List<String> filterTableNameList(String methodName, CatalogRouter.ResolvedNamespace namespace, List<?> names) {
    List<String> candidateNames = new ArrayList<>(names.size());
    for (Object candidate : names) {
      if (candidate instanceof String tableName) {
        candidateNames.add(tableName);
      }
    }
    return filterAuthorizedTables(
        methodName,
        namespace.catalogName(),
        namespace.backendDbName(),
        candidateNames,
        Function.identity());
  }

  private <T> List<T> filterAuthorizedTables(
      String methodName,
      String catalogName,
      String backendDbName,
      List<T> items,
      Function<T, String> tableNameExtractor
  ) {
    boolean bypassAuthorizer = isServicePrincipalBypass();
    ImpersonationContext impersonation = null;
    if (!bypassAuthorizer) {
      try {
        impersonation = impersonationResolver != null ? impersonationResolver.resolve().orElse(null) : null;
      } catch (Exception e) {
        LOG.warn("Failed to resolve impersonation context during {} for catalog '{}' db='{}'; "
                + "returning empty table list (fail-closed): {}",
            methodName, catalogName, backendDbName, e.getMessage());
        return List.of();
      }
    }
    List<T> filtered = new ArrayList<>(items.size());
    for (T candidate : items) {
      String tableName = tableNameExtractor.apply(candidate);
      if (tableName != null) {
        if (!bypassAuthorizer && !metadataAuthorizer.isTableAllowed(
            catalogName, backendDbName, tableName, impersonation)) {
          recordFilteredObject(methodName, catalogName, "table");
          continue;
        }
        if (!federationLayer.isTableExposed(catalogName, backendDbName, tableName)) {
          recordFilteredObject(methodName, catalogName, "table");
          continue;
        }
      }
      filtered.add(candidate);
    }
    return filtered;
  }

  static String extractTableName(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Table table) {
      return blankToNull(table.getTableName());
    }
    String name = blankToNull(
        ThriftReflectionCache.readString(value, "getTableName", "getTblName", "getTbl_name", "getTablename", "getName"));
    if (name != null) {
      return name;
    }
    Object statsDesc = ThriftReflectionCache.invokeGetter(value, "getStatsDesc");
    if (statsDesc != null) {
      String statsTable = extractTableName(statsDesc);
      if (statsTable != null) {
        return statsTable;
      }
    }
    String fullTableName = blankToNull(
        ThriftReflectionCache.readString(value, "getFullTableName", "getFull_table_name"));
    if (fullTableName == null) {
      return null;
    }
    int separator = fullTableName.lastIndexOf('.');
    return separator >= 0 && separator + 1 < fullTableName.length()
        ? blankToNull(fullTableName.substring(separator + 1))
        : blankToNull(fullTableName);
  }

  static Set<String> extractTableNames(Object[] args) {
    if (args == null || args.length == 0) {
      return Set.of();
    }
    Set<String> names = new LinkedHashSet<>();
    for (Object arg : args) {
      collectTableNames(arg, names);
    }
    return names;
  }

  private static void collectTableNames(Object value, Set<String> out) {
    if (value == null) {
      return;
    }
    if (value instanceof Iterable<?> iterable) {
      for (Object element : iterable) {
        collectTableNames(element, out);
      }
      return;
    }
    String name = extractTableName(value);
    if (name != null) {
      out.add(name);
      return;
    }
    Object nestedParts = ThriftReflectionCache.invokeGetter(value, "getParts");
    if (nestedParts instanceof Collection<?> parts) {
      collectTableNames(parts, out);
    }
    Object nestedPartitions = ThriftReflectionCache.invokeGetter(value, "getPartitions");
    if (nestedPartitions instanceof Collection<?> partitions) {
      collectTableNames(partitions, out);
    }
    Object nestedColStats = ThriftReflectionCache.invokeGetter(value, "getColStats");
    if (nestedColStats instanceof Collection<?> colStats) {
      collectTableNames(colStats, out);
    }
  }

  static String blankToNull(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }

  synchronized void reconfigure(ProxyConfig newConfig) {
    this.config = newConfig;
    this.externalTableLocationRewriter = newExternalTableLocationRewriter(newConfig);
    if (databaseListCache != null) {
      databaseListCache.invalidateAll();
    }
    if (databaseMetadataCache != null) {
      databaseMetadataCache.invalidateAll();
    }
    if (tableMetadataCache != null) {
      tableMetadataCache.clear();
    }
    if (partitionMetadataCache != null) {
      partitionMetadataCache.clear();
    }
  }
}
