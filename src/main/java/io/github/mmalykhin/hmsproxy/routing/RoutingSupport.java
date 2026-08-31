package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared collaborators and helpers used by the namespace-aware routing handler and its
 * special-case sub-handlers.
 */
final class RoutingSupport {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingSupport.class);

  final ProxyConfig config;
  final CatalogRouter router;
  final FederationOperations federationLayer;
  final ProxyObservability observability;
  final BackendCallDispatcher dispatcher;
  final ImpersonationResolver impersonationResolver;
  final DatabaseListCache databaseListCache;

  RoutingSupport(
      ProxyConfig config,
      CatalogRouter router,
      FederationOperations federationLayer,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      DatabaseListCache databaseListCache
  ) {
    this.config = config;
    this.router = router;
    this.federationLayer = federationLayer;
    this.observability = observability;
    this.dispatcher = dispatcher;
    this.impersonationResolver = impersonationResolver;
    this.databaseListCache = databaseListCache;
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

  void validateExposedDatabaseAccess(String methodName, CatalogRouter.ResolvedNamespace namespace)
      throws NoSuchObjectException {
    if (federationLayer.isDatabaseExposed(namespace)) {
      return;
    }
    recordFilteredObject(methodName, namespace.catalogName(), "database");
    throw new NoSuchObjectException(
        "Database '" + namespace.externalDbName() + "' is not exposed by proxy catalog '"
            + namespace.catalogName() + "'");
  }

  void validateExposedTableAccess(
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
    return filterExposed(
        methodName,
        catalogName,
        "database",
        backendDatabases,
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

  static String extractTableName(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Table table) {
      return blankToNull(table.getTableName());
    }
    String name = blankToNull(ThriftReflectionCache.readString(value, "getTableName", "getTblName", "getName"));
    if (name != null) {
      return name;
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

  static String blankToNull(String value) {
    if (value == null) {
      return null;
    }
    String normalized = value.trim();
    return normalized.isEmpty() ? null : normalized;
  }
}
