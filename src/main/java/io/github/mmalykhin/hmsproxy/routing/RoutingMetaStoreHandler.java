package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.federation.FederationLayer;
import io.github.mmalykhin.hmsproxy.frontend.HortonworksFrontendExtension;
import io.github.mmalykhin.hmsproxy.observability.AuditLogUtil;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import io.github.mmalykhin.hmsproxy.util.DebugLogUtil;
import io.github.mmalykhin.hmsproxy.util.WriteTraceUtil;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RoutingMetaStoreHandler implements InvocationHandler, HortonworksFrontendExtension, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingMetaStoreHandler.class);
  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("io.github.mmalykhin.hmsproxy.audit");
  private static final AtomicLong REQUEST_SEQUENCE = new AtomicLong();
  private static final ThreadLocal<Long> REQUEST_ID = new ThreadLocal<>();
  private static final ThreadLocal<RequestObservation> REQUEST_OBSERVATION = new ThreadLocal<>();
  private static final List<String> DB_STRING_METHODS = List.of(
      "get_database",
      "drop_database",
      "alter_database",
      "get_all_tables",
      "get_tables",
      "get_tables_by_type",
      "get_materialized_views_for_rewriting",
      "get_table",
      "get_table_objects_by_name",
      "truncate_table",
      "drop_table",
      "drop_table_with_environment_context",
      "get_fields",
      "get_fields_with_environment_context",
      "get_schema",
      "get_schema_with_environment_context",
      "get_table_names_by_filter"
  );
  private static final List<String> DB_FIRST_STRING_METHODS = List.of(
      "update_creation_metadata",
      "append_partition",
      "append_partition_with_environment_context",
      "append_partition_by_name",
      "append_partition_by_name_with_environment_context",
      "drop_partition",
      "drop_partition_with_environment_context",
      "drop_partition_by_name",
      "drop_partition_by_name_with_environment_context",
      "get_partition",
      "get_partition_with_auth",
      "get_partition_by_name",
      "get_partitions",
      "get_partitions_with_auth",
      "get_partitions_pspec",
      "get_partition_names",
      "get_partitions_ps",
      "get_partitions_ps_with_auth",
      "get_partition_names_ps",
      "get_partitions_by_filter",
      "get_part_specs_by_filter",
      "get_num_partitions_by_filter",
      "get_partitions_by_names",
      "markPartitionForEvent",
      "isPartitionMarkedForEvent",
      "get_table_column_statistics",
      "get_partition_column_statistics",
      "delete_partition_column_statistics",
      "delete_table_column_statistics",
      "drop_function",
      "alter_function",
      "get_functions",
      "get_function",
      "get_lock_materialization_rebuild",
      "heartbeat_lock_materialization_rebuild"
  );
  private final ProxyConfig config;
  private final CatalogRouter router;
  private final FrontDoorSecurity frontDoorSecurity;
  private final ProxyObservability observability;
  private final CompatibilityLayer compatibilityLayer;
  private final FederationLayer federationLayer;
  private final TransactionalTableMutationGuard transactionalTableMutationGuard;
  private final SyntheticReadLockManager syntheticReadLockManager;
  private final long aliveSince;

  public RoutingMetaStoreHandler(ProxyConfig config, CatalogRouter router, FrontDoorSecurity frontDoorSecurity) {
    this(config, router, frontDoorSecurity, new ProxyObservability(config));
  }

  public RoutingMetaStoreHandler(
      ProxyConfig config,
      CatalogRouter router,
      FrontDoorSecurity frontDoorSecurity,
      ProxyObservability observability
  ) {
    this.config = config;
    this.router = router;
    this.frontDoorSecurity = frontDoorSecurity;
    this.observability = observability;
    this.compatibilityLayer = new CompatibilityLayer(config, frontDoorSecurity);
    this.federationLayer = new FederationLayer(config, router);
    this.transactionalTableMutationGuard = new TransactionalTableMutationGuard(config);
    this.syntheticReadLockManager = new SyntheticReadLockManager(config, observability.metrics());
    this.aliveSince = System.currentTimeMillis() / 1000L;
  }

  @SuppressWarnings("unchecked")
  public static <T> T newProxy(Class<T> interfaceClass, InvocationHandler handler) {
    return newProxy(interfaceClass, handler, new Class<?>[0]);
  }

  @SuppressWarnings("unchecked")
  public static <T> T newProxy(Class<T> interfaceClass, InvocationHandler handler, Class<?>... extraInterfaces) {
    Class<?>[] interfaces = new Class<?>[1 + extraInterfaces.length];
    interfaces[0] = interfaceClass;
    System.arraycopy(extraInterfaces, 0, interfaces, 1, extraInterfaces.length);
    return (T) Proxy.newProxyInstance(
        interfaceClass.getClassLoader(),
        interfaces,
        handler);
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String name = method.getName();
    if (method.getDeclaringClass() == Object.class) {
      return method.invoke(this, args);
    }

    long requestId = REQUEST_SEQUENCE.incrementAndGet();
    long startedAt = System.nanoTime();
    RequestObservation observation = new RequestObservation(name);
    REQUEST_ID.set(requestId);
    REQUEST_OBSERVATION.set(observation);
    if (LOG.isDebugEnabled()) {
      LOG.debug("requestId={} incoming method={} args={}",
          requestId, name, DebugLogUtil.formatArgs(args));
    }
    if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(name)) {
      LOG.info("requestId={} trace stage=client-request method={} summary={}",
          requestId, name, WriteTraceUtil.summarizeArgs(args));
    }

    try {
      transactionalTableMutationGuard.validate(name, args);
      Object result = switch (name) {
        case "getName" -> config.server().name();
        case "getVersion" -> compatibilityLayer.frontendVersion();
        case "aliveSince" -> aliveSince;
        case "reinitialize", "shutdown" -> null;
        case "set_ugi" -> handleSetUgi(method, args);
        case "getStatus" -> enumConstant(method.getReturnType(), "ALIVE");
        case "get_catalogs" -> new GetCatalogsResponse(config.catalogNames());
        case "get_catalog" -> handleGetCatalog(args);
        case "get_config_value" -> handleGetConfigValue(method, args);
        case "create_catalog", "alter_catalog", "drop_catalog" ->
            throw metaException("Catalog definitions are managed by proxy config, not via HMS API");
        case "get_all_databases" -> handleGetAllDatabases(method);
        case "get_databases" -> handleGetDatabases(method, args);
        case "lock" -> handleLock(method, args);
        case "check_lock" -> handleCheckLock(method, args);
        case "unlock" -> handleUnlock(method, args);
        case "heartbeat" -> handleHeartbeat(method, args);
        case "commit_txn" -> handleCommitTxn(method, args);
        case "abort_txn" -> handleAbortTxn(method, args);
        case "abort_txns" -> handleAbortTxns(method, args);
        case "get_table_meta" -> handleGetTableMeta(method, args);
        case "get_table_req" -> handleGetTableReq(method, args);
        case "get_table_objects_by_name_req" -> handleGetTablesReq(method, args);
        default -> MetastoreCompatibility.handlesLocally(name)
            ? compatibilityLayer.handleLocalMethod(name, args)
            : routeByNamespaceOrFail(method, args);
      };
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} client-response method={} elapsedMs={} result={}",
            requestId, name, elapsedMillis(startedAt), DebugLogUtil.formatValue(result));
      }
      if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(name)) {
        LOG.info("requestId={} trace stage=client-response method={} elapsedMs={} summary={}",
            requestId, name, elapsedMillis(startedAt), WriteTraceUtil.summarizeResult(result));
      }
      return result;
    } catch (Throwable throwable) {
      observation.markError();
      long elapsedMs = elapsedMillis(startedAt);
      LOG.debug("requestId={} client-error method={} elapsedMs={} error={}",
          requestId, name, elapsedMs, throwable.toString(), throwable);
      if (throwable instanceof TException) {
        throw throwable;
      }
      if (throwable instanceof RuntimeException) {
        LOG.error("requestId={} unexpected runtime error in method={} elapsedMs={}",
            requestId, name, elapsedMs, throwable);
        throw throwable;
      }

      LOG.error("requestId={} unexpected checked error in method={} elapsedMs={}",
          requestId, name, elapsedMs, throwable);
      MetaException metaException =
          metaException("Proxy internal error in " + name + ": " + throwable.getClass().getSimpleName()
              + (throwable.getMessage() == null ? "" : " - " + throwable.getMessage()));
      metaException.initCause(throwable);
      throw metaException;
    } finally {
      observability.metrics().recordRequest(
          observation.method(),
          observation.catalog(),
          observation.backend(),
          observation.status(),
          elapsedSeconds(startedAt));
      emitAuditLog(requestId, observation, elapsedMillis(startedAt));
      REQUEST_ID.remove();
      REQUEST_OBSERVATION.remove();
    }
  }

  @Override
  public void close() throws MetaException {
    syntheticReadLockManager.close();
  }

  @Override
  public Object addWriteNotificationLog(Object request) throws Throwable {
    String dbName = (String) request.getClass().getMethod("getDb").invoke(request);
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit("add_write_notification_log", dbName, namespace);
    CatalogBackend backend = namespace.backend();
    validateCatalogAccess(backend, "add_write_notification_log", namespace.backendDbName());
    if (!backend.runtimeProfile().isHortonworks()) {
      throw metaException(
          "Hortonworks add_write_notification_log requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }

    Object routedRequest = cloneWriteNotificationLogRequest(request);
    routedRequest.getClass().getMethod("setDb", String.class).invoke(routedRequest, namespace.backendDbName());
    return invokeBackendNamed(backend, "add_write_notification_log", routedRequest);
  }

  @Override
  public Object getTablesExt(Object request) throws Throwable {
    String catalogName = (String) request.getClass().getMethod("getCatalog").invoke(request);
    String dbName = (String) request.getClass().getMethod("getDatabase").invoke(request);
    CatalogRouter.ResolvedNamespace namespace = resolveRequestNamespace(catalogName, dbName);
    currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit("get_tables_ext", catalogName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    if (!backend.runtimeProfile().isHortonworks()) {
      throw metaException(
          "Hortonworks get_tables_ext requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }
    validateCatalogAccess(backend, "get_tables_ext", namespace.backendDbName());
    Object routedRequest = request.getClass().getConstructor(request.getClass()).newInstance(request);
    routedRequest.getClass().getMethod("setDatabase", String.class).invoke(routedRequest, namespace.backendDbName());
    String internalCatalog = NamespaceTranslator.internalCatalogName(catalogName, dbName, namespace,
        federationLayer.preserveBackendCatalogName());
    routedRequest.getClass().getMethod("setCatalog", String.class)
        .invoke(routedRequest, internalCatalog == null ? catalogName : internalCatalog);
    return invokeBackendNamed(backend, "get_tables_ext", routedRequest);
  }

  @Override
  public Object getAllMaterializedViewObjectsForRewriting() throws Throwable {
    CatalogBackend backend = router.defaultBackend();
    currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    currentObservation().markDefaultCatalogRoute();
    observability.metrics().recordDefaultCatalogRoute("get_all_materialized_view_objects_for_rewriting");
    if (!backend.runtimeProfile().isHortonworks()) {
      throw metaException(
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
        externalized.add(federationLayer.externalizeResult(table, namespace));
      }
      return externalized;
    }
    return result;
  }

  private Object handleGetCatalog(Object[] args) throws NoSuchObjectException {
    GetCatalogRequest request = (GetCatalogRequest) args[0];
    if (!config.catalogs().containsKey(request.getName())) {
      throw new NoSuchObjectException("Unknown catalog: " + request.getName());
    }
    GetCatalogResponse response = new GetCatalogResponse();
    response.setCatalog(router.requireBackend(request.getName()).catalog());
    return response;
  }

  private Object handleGetAllDatabases(Method method) throws Throwable {
    currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (CatalogBackend backend : router.backends()) {
      @SuppressWarnings("unchecked")
      List<String> backendDatabases = (List<String>) invokeBackend(backend, method, null);
      for (String database : backendDatabases) {
        databases.add(federationLayer.externalDatabaseName(backend.name(), database));
      }
    }
    return databases;
  }

  private Object handleGetDatabases(Method method, Object[] args) throws Throwable {
    String pattern = (String) args[0];
    CatalogRouter.ResolvedNamespace resolved = router.resolvePattern(pattern).orElse(null);
    if (resolved != null) {
      currentObservation().recordNamespace(resolved);
      @SuppressWarnings("unchecked")
      List<String> backendDatabases =
          (List<String>) invokeBackend(resolved.backend(), method, new Object[] {resolved.backendDbName()});
      return backendDatabases.stream()
          .map(db -> federationLayer.externalDatabaseName(resolved.catalogName(), db))
          .toList();
    }

    currentObservation().recordFanout();
    List<String> databases = new ArrayList<>();
    for (CatalogBackend backend : router.backends()) {
      @SuppressWarnings("unchecked")
      List<String> backendDatabases =
          (List<String>) invokeBackend(backend, method, new Object[] {pattern});
      for (String database : backendDatabases) {
        databases.add(federationLayer.externalDatabaseName(backend.name(), database));
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
      currentObservation().recordNamespace(resolved);
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults = (List<TableMeta>) invokeBackend(
          resolved.backend(), method, new Object[] {resolved.backendDbName(), tablePattern, tableTypes});
      return backendResults.stream()
          .map(result -> federationLayer.externalizeTableMeta(result, resolved))
          .toList();
    }

    currentObservation().recordFanout();
    List<TableMeta> results = new ArrayList<>();
    for (CatalogBackend backend : router.backends()) {
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults =
          (List<TableMeta>) invokeBackend(backend, method, new Object[] {dbPattern, tablePattern, tableTypes});
      for (TableMeta result : backendResults) {
        results.add(NamespaceTranslator.externalizeTableMeta(
            result,
            router.resolveCatalog(backend.name(), result.getDbName()),
            federationLayer.preserveBackendCatalogName()));
      }
    }
    return results;
  }

  private Object handleGetTableReq(Method method, Object[] args) throws Throwable {
    GetTableRequest request = (GetTableRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = federationLayer.resolveRequestNamespace(request.getCatName(),
        request.getDbName());
    currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit(method.getName(), request.getCatName(), request.getDbName(), namespace);
    CatalogBackend backend = namespace.backend();
    GetTableRequest routedRequest = (GetTableRequest) federationLayer.internalizeTableRequest(request, namespace);
    Object result = invokeBackendRequest(backend, routedRequest, method.getName());
    return federationLayer.externalizeResult(result, namespace);
  }

  private Object handleGetTablesReq(Method method, Object[] args) throws Throwable {
    GetTablesRequest request = (GetTablesRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = federationLayer.resolveRequestNamespace(request.getCatName(),
        request.getDbName());
    currentObservation().recordNamespace(namespace);
    recordDefaultCatalogRouteIfImplicit(method.getName(), request.getCatName(), request.getDbName(), namespace);
    CatalogBackend backend = namespace.backend();
    GetTablesRequest routedRequest = (GetTablesRequest) federationLayer.internalizeTablesRequest(request, namespace);
    Object result = invokeBackendRequest(backend, routedRequest, method.getName());
    return federationLayer.externalizeResult(result, namespace);
  }

  private Object handleLock(Method method, Object[] args) throws Throwable {
    CatalogRouter.ResolvedNamespace namespace = args == null ? null : findNamespaceInArgs(args);
    if (namespace != null) {
      SyntheticReadLockManager.SyntheticLockState syntheticState =
          syntheticReadLockManager.tryAcquire((LockRequest) args[0], namespace);
      if (syntheticState != null) {
        currentObservation().recordNamespace(namespace);
        currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
        LockResponse response = syntheticReadLockManager.acquiredResponse(syntheticState.lockId());
        if (LOG.isInfoEnabled()) {
          LOG.info("requestId={} synthetic read lock acquired catalog={} db={} txnId={} lockId={}",
              currentRequestId(),
              namespace.catalogName(),
              syntheticState.externalDbName(),
              syntheticState.txnId(),
              syntheticState.lockId());
        }
        return response;
      }
    }
    return routeByNamespaceOrFail(method, args);
  }

  private Object handleCheckLock(Method method, Object[] args) throws Throwable {
    SyntheticReadLockManager.SyntheticLockState syntheticState =
        syntheticReadLockManager.syntheticLock((CheckLockRequest) args[0]);
    if (syntheticState == null) {
      return routeByNamespaceOrFail(method, args);
    }
    currentObservation().recordNamespace(syntheticState.namespace(router));
    currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    return syntheticReadLockManager.acquiredResponse(syntheticState.lockId());
  }

  private Object handleUnlock(Method method, Object[] args) throws Throwable {
    SyntheticReadLockManager.SyntheticLockState syntheticState =
        syntheticReadLockManager.syntheticLock((org.apache.hadoop.hive.metastore.api.UnlockRequest) args[0]);
    if (syntheticState == null) {
      return routeByNamespaceOrFail(method, args);
    }
    currentObservation().recordNamespace(syntheticState.namespace(router));
    currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    syntheticReadLockManager.releaseLock(syntheticState);
    return null;
  }

  private Object handleHeartbeat(Method method, Object[] args) throws Throwable {
    HeartbeatRequest request = (HeartbeatRequest) args[0];
    SyntheticReadLockManager.SyntheticLockState syntheticState = syntheticReadLockManager.syntheticLock(request);
    if (syntheticState == null) {
      return routeByNamespaceOrFail(method, args);
    }
    currentObservation().recordNamespace(syntheticState.namespace(router));
    currentObservation().recordBackend(SyntheticReadLockManager.SYNTHETIC_BACKEND_NAME);
    syntheticReadLockManager.touch(syntheticState);

    HeartbeatRequest txnOnlyHeartbeat = syntheticReadLockManager.txnOnlyHeartbeat(request);
    if (txnOnlyHeartbeat == null) {
      syntheticReadLockManager.recordHeartbeatWithoutTxn(syntheticState);
      return null;
    }
    validateCatalogAccess(router.defaultBackend(), method.getName(), null);
    Object result = invokeBackend(router.defaultBackend(), method, new Object[] {txnOnlyHeartbeat});
    syntheticReadLockManager.recordHeartbeatForwarded(syntheticState);
    return result;
  }

  private Object handleCommitTxn(Method method, Object[] args) throws Throwable {
    long txnId = ((CommitTxnRequest) args[0]).getTxnid();
    try {
      return invokeGlobal(method, args);
    } finally {
      syntheticReadLockManager.releaseTxn(txnId);
    }
  }

  private Object handleAbortTxn(Method method, Object[] args) throws Throwable {
    long txnId = ((AbortTxnRequest) args[0]).getTxnid();
    try {
      return invokeGlobal(method, args);
    } finally {
      syntheticReadLockManager.releaseTxn(txnId);
    }
  }

  private Object handleAbortTxns(Method method, Object[] args) throws Throwable {
    List<Long> txnIds = ((AbortTxnsRequest) args[0]).getTxn_ids();
    try {
      return invokeGlobal(method, args);
    } finally {
      if (txnIds != null) {
        for (Long txnId : txnIds) {
          syntheticReadLockManager.releaseTxn(txnId == null ? 0L : txnId);
        }
      }
    }
  }

  private Object routeByNamespaceOrFail(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    if (args == null || args.length == 0) {
      return invokeGlobal(method, args);
    }

    if (DB_STRING_METHODS.contains(methodName) && args[0] instanceof String dbName) {
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      currentObservation().recordNamespace(namespace);
      recordDefaultCatalogRouteIfImplicit(methodName, dbName, namespace);
      validateCatalogAccess(namespace.backend(), methodName, namespace.backendDbName());
      Object[] routedArgs = federationLayer.internalizeDbStringArguments(args, namespace);
      Object result = invokeBackend(namespace.backend(), method, routedArgs);
      return federationLayer.externalizeResult(result, namespace);
    }

    CatalogRouter.ResolvedNamespace extractedNamespace = findNamespaceInArgs(args);
    if (extractedNamespace != null) {
      currentObservation().recordNamespace(extractedNamespace);
      validateCatalogAccess(extractedNamespace.backend(), methodName, extractedNamespace.backendDbName());
      Object[] routedArgs = federationLayer.internalizeObjectArguments(args, extractedNamespace);
      Object result = invokeBackend(extractedNamespace.backend(), method, routedArgs);
      return federationLayer.externalizeResult(result, extractedNamespace);
    }
    if (DB_FIRST_STRING_METHODS.contains(methodName)
        && args.length > 1
        && args[0] instanceof String dbName
        && args[1] instanceof String) {
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      currentObservation().recordNamespace(namespace);
      recordDefaultCatalogRouteIfImplicit(methodName, dbName, namespace);
      validateCatalogAccess(namespace.backend(), methodName, namespace.backendDbName());
      Object[] routedArgs = federationLayer.internalizeDbStringArguments(args, namespace);
      Object result = invokeBackend(namespace.backend(), method, routedArgs);
      return federationLayer.externalizeResult(result, namespace);
    }

    return invokeGlobal(method, args);
  }

  private Object invokeGlobal(Method method, Object[] args) throws Throwable {
    Optional<Object> compatibilityFallback = compatibilityLayer.fallback(
        method.getName(),
        metaException("Operation " + method.getName() + " has no catalog context"));
    if (!DefaultBackendRoutingPolicy.routesToDefaultBackend(method.getName())
        && !router.singleCatalog()
        && compatibilityFallback.isPresent()) {
      currentObservation().markFallback();
      LOG.warn("requestId={} method={} has no catalog context, returning compatibility fallback",
          currentRequestId(), method.getName());
      return compatibilityFallback.get();
    }
    if (DefaultBackendRoutingPolicy.routesToDefaultBackend(method.getName())) {
      currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
      observability.metrics().recordDefaultCatalogRoute(method.getName());
      validateCatalogAccess(router.defaultBackend(), method.getName(), null);
      return invokeBackend(router.defaultBackend(), method, args);
    }
    if (!router.singleCatalog()) {
      throw metaException("Operation " + method.getName()
          + " has no catalog context; use explicit catalog.db naming or a catalog-aware request");
    }
    currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    observability.metrics().recordDefaultCatalogRoute(method.getName());
    validateCatalogAccess(router.defaultBackend(), method.getName(), null);
    return invokeBackend(router.defaultBackend(), method, args);
  }

  private Object handleGetConfigValue(Method method, Object[] args) throws Throwable {
    String requestedName = args != null && args.length > 0 ? (String) args[0] : null;
    String defaultValue = args != null && args.length > 1 ? (String) args[1] : null;
    Optional<String> compatibilityValue = compatibilityLayer.compatibleConfigValue(
        requestedName,
        defaultValue,
        config.catalogs().get(config.defaultCatalog()).hiveConf());
    if (compatibilityValue.isPresent()) {
      currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} returning compatibility config value for key '{}'",
            currentRequestId(), requestedName);
      }
      return compatibilityValue.get();
    }
    currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    observability.metrics().recordDefaultCatalogRoute(method.getName());
    return invokeBackend(router.defaultBackend(), method, args);
  }

  private CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException {
    try {
      return federationLayer.findNamespaceInArgs(args);
    } catch (MetaException e) {
      observability.metrics().recordRoutingAmbiguous();
      throw e;
    }
  }

  private CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException {
    try {
      return federationLayer.resolveRequestNamespace(catName, dbName);
    } catch (MetaException e) {
      if (e.getMessage() != null && e.getMessage().contains("conflicting catalog and database namespace")) {
        observability.metrics().recordRoutingAmbiguous();
      } else if (catName != null && !catName.isBlank() && LOG.isDebugEnabled()
          && federationLayer.resolveCatalogIfKnown(catName, dbName).isEmpty()) {
        LOG.debug("requestId={} ignoring unknown request catalog '{}' and resolving by dbName='{}'",
            currentRequestId(), catName, dbName);
      }
      throw e;
    }
  }

  private Object invokeBackend(CatalogBackend backend, Method method, Object[] args) throws Throwable {
    long startedAt = System.nanoTime();
    Long requestId = currentRequestId();
    ImpersonationContext impersonation = currentImpersonation().orElse(null);
    try {
      currentObservation().recordBackend(backend.name());
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-call catalog={} method={} impersonationUser={} args={}",
            requestId,
            backend.name(),
            method.getName(),
            impersonation == null ? "-" : impersonation.userName(),
            DebugLogUtil.formatArgs(args));
      }
      if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(method.getName())) {
        LOG.info("requestId={} trace stage=backend-request catalog={} method={} impersonationUser={} summary={}",
            requestId,
            backend.name(),
            method.getName(),
            impersonation == null ? "-" : impersonation.userName(),
            WriteTraceUtil.summarizeArgs(args));
      }
      Object result = backend.invoke(method, args, impersonation);
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-response catalog={} method={} elapsedMs={} result={}",
            requestId, backend.name(), method.getName(), elapsedMillis(startedAt),
            DebugLogUtil.formatValue(result));
      }
      if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(method.getName())) {
        LOG.info("requestId={} trace stage=backend-response catalog={} method={} elapsedMs={} summary={}",
            requestId,
            backend.name(),
            method.getName(),
            elapsedMillis(startedAt),
            WriteTraceUtil.summarizeResult(result));
      }
      observability.runtimeState().recordBackendSuccess(backend.name());
      return result;
    } catch (Throwable cause) {
      observability.metrics().recordBackendFailure(backend.name(), cause);
      observability.runtimeState().recordBackendFailure(backend.name(), cause);
      Optional<Object> compatibilityFallback = compatibilityLayer.fallback(method.getName(), cause);
      if (compatibilityFallback.isPresent()) {
        currentObservation().markFallback();
        observability.metrics().recordBackendFallback(
            method.getName(),
            backend.runtimeProfile().name(),
            compatibilityLayer.frontendRuntimeProfile().name());
        LOG.warn("requestId={} backend catalog={} failed compatibility method {}, returning fallback",
            requestId, backend.name(), method.getName(), cause);
        return compatibilityFallback.get();
      }
      if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(method.getName())) {
        LOG.info("requestId={} trace stage=backend-error catalog={} method={} elapsedMs={} error={}",
            requestId,
            backend.name(),
            method.getName(),
            elapsedMillis(startedAt),
            cause.toString());
      }
      LOG.debug("requestId={} proxy-error catalog={} method={} elapsedMs={} error={}",
          requestId, backend.name(), method.getName(), elapsedMillis(startedAt), cause.toString(), cause);
      throw normalizeBackendFailure(method, backend.name(), cause);
    }
  }

  private Object invokeBackendRequest(CatalogBackend backend, Object request, String methodName) throws Throwable {
    long startedAt = System.nanoTime();
    Long requestId = currentRequestId();
    ImpersonationContext impersonation = currentImpersonation().orElse(null);
    try {
      currentObservation().recordBackend(backend.name());
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-call catalog={} method={} impersonationUser={} args={}",
            requestId,
            backend.name(),
            methodName,
            impersonation == null ? "-" : impersonation.userName(),
            DebugLogUtil.formatArgs(new Object[] {request}));
      }
      Object adapted = backend.invokeRequest(methodName, request, impersonation);
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-response catalog={} method={} elapsedMs={} result={}",
            requestId, backend.name(), methodName, elapsedMillis(startedAt), DebugLogUtil.formatValue(adapted));
      }
      observability.runtimeState().recordBackendSuccess(backend.name());
      return adapted;
    } catch (Throwable cause) {
      observability.metrics().recordBackendFailure(backend.name(), cause);
      observability.runtimeState().recordBackendFailure(backend.name(), cause);
      Optional<Object> compatibilityFallback = compatibilityLayer.fallback(methodName, cause);
      if (compatibilityFallback.isPresent()) {
        currentObservation().markFallback();
        observability.metrics().recordBackendFallback(
            methodName,
            backend.runtimeProfile().name(),
            compatibilityLayer.frontendRuntimeProfile().name());
        LOG.warn("requestId={} backend catalog={} failed compatibility method {}, returning fallback",
            requestId, backend.name(), methodName, cause);
        return compatibilityFallback.get();
      }
      LOG.debug("requestId={} proxy-error catalog={} method={} elapsedMs={} error={}",
          requestId, backend.name(), methodName, elapsedMillis(startedAt), cause.toString(), cause);
      throw cause;
    }
  }

  private Object invokeBackendNamed(CatalogBackend backend, String methodName, Object request) throws Throwable {
    return invokeBackendByName(backend, methodName, new Class<?>[] {request.getClass()}, new Object[] {request});
  }

  private Object invokeBackendByName(
      CatalogBackend backend,
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args
  ) throws Throwable {
    long startedAt = System.nanoTime();
    Long requestId = currentRequestId();
    ImpersonationContext impersonation = currentImpersonation().orElse(null);
    try {
      currentObservation().recordBackend(backend.name());
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-call catalog={} method={} impersonationUser={} args={}",
            requestId,
            backend.name(),
            methodName,
            impersonation == null ? "-" : impersonation.userName(),
            DebugLogUtil.formatArgs(args));
      }
      Object result = backend.invokeRawByName(
          methodName,
          parameterTypes,
          args,
          impersonation);
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-response catalog={} method={} elapsedMs={} result={}",
            requestId, backend.name(), methodName, elapsedMillis(startedAt), DebugLogUtil.formatValue(result));
      }
      observability.runtimeState().recordBackendSuccess(backend.name());
      return result;
    } catch (Throwable cause) {
      observability.metrics().recordBackendFailure(backend.name(), cause);
      observability.runtimeState().recordBackendFailure(backend.name(), cause);
      LOG.debug("requestId={} proxy-error catalog={} method={} elapsedMs={} error={}",
          requestId, backend.name(), methodName, elapsedMillis(startedAt), cause.toString(), cause);
      throw cause;
    }
  }

  private static Object cloneWriteNotificationLogRequest(Object request) throws ReflectiveOperationException {
    return request.getClass().getConstructor(request.getClass()).newInstance(request);
  }

  private void validateCatalogAccess(CatalogBackend backend, String methodName, String backendDbName)
      throws MetaException {
    CatalogAccessModeGuard.validate(config.catalogs().get(backend.name()), methodName, backendDbName);
  }

  private static long currentRequestId() {
    Long requestId = REQUEST_ID.get();
    return requestId == null ? -1L : requestId;
  }

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  private static double elapsedSeconds(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000_000.0d;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object enumConstant(Class<?> enumType, String constantName) {
    return Enum.valueOf((Class<? extends Enum>) enumType.asSubclass(Enum.class), constantName);
  }

  private static MetaException metaException(String message) {
    return new MetaException(message);
  }

  private static Throwable normalizeBackendFailure(Method method, String backendName, Throwable cause) {
    if (!(cause instanceof TException) || isDeclaredMethodException(method, cause)) {
      return cause;
    }

    String message = "Backend catalog '" + backendName + "' failed in method '" + method.getName()
        + "' with " + cause.getClass().getSimpleName();
    if (cause.getMessage() != null && !cause.getMessage().isBlank()) {
      message += ": " + cause.getMessage();
    }
    MetaException metaException = metaException(message);
    metaException.initCause(cause);
    return metaException;
  }

  private static boolean isDeclaredMethodException(Method method, Throwable cause) {
    for (Class<?> declaredType : method.getExceptionTypes()) {
      if (declaredType == TException.class) {
        continue;
      }
      if (declaredType.isAssignableFrom(cause.getClass())) {
        return true;
      }
    }
    return false;
  }

  private Object handleSetUgi(Method method, Object[] args) throws Throwable {
    if (!router.defaultBackend().impersonationEnabled()) {
      return invokeGlobal(method, args);
    }

    ImpersonationContext impersonation = currentImpersonation().orElseThrow(() ->
        metaException("Kerberos caller identity is unavailable for impersonation"));

    if (args != null && args.length > 0 && args[0] instanceof String requestedUser
        && !requestedUser.isBlank()
        && !requestedUser.equals(impersonation.userName())) {
      LOG.warn("requestId={} ignoring client-requested set_ugi user '{}' and using authenticated user '{}'",
          currentRequestId(), requestedUser, impersonation.userName());
    }

    return invokeGlobal(method, new Object[] {impersonation.userName(), impersonation.groupNames()});
  }

  private Optional<ImpersonationContext> currentImpersonation() throws MetaException {
    if (config.catalogs().values().stream().noneMatch(ProxyConfig.CatalogConfig::impersonationEnabled)) {
      return Optional.empty();
    }

    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      String userName = currentUser.getShortUserName();
      if (userName == null || userName.isBlank()) {
        return Optional.empty();
      }
      if (isServicePrincipalUser(userName, config.security())) {
        return Optional.empty();
      }
      return Optional.of(new ImpersonationContext(userName, resolveGroupNames(currentUser, userName)));
    } catch (Exception e) {
      throw metaException("Unable to resolve authenticated caller for impersonation: " + e.getMessage());
    }
  }

  private List<String> resolveGroupNames(UserGroupInformation currentUser, String userName) {
    try {
      return List.of(currentUser.getGroupNames());
    } catch (RuntimeException e) {
      LOG.warn("requestId={} unable to resolve groups for authenticated user '{}', using empty group list",
          currentRequestId(), userName, e);
      return List.of();
    }
  }

  public record ImpersonationContext(String userName, List<String> groupNames) {
  }

  static boolean isDefaultBackendGlobalMethod(String methodName) {
    return DefaultBackendRoutingPolicy.routesToDefaultBackend(methodName);
  }

  public static String shortUserName(String principalOrUser) {
    if (principalOrUser == null || principalOrUser.isBlank()) {
      return principalOrUser;
    }
    int slash = principalOrUser.indexOf('/');
    int at = principalOrUser.indexOf('@');
    int end = principalOrUser.length();
    if (slash >= 0) {
      end = Math.min(end, slash);
    }
    if (at >= 0) {
      end = Math.min(end, at);
    }
    return principalOrUser.substring(0, end);
  }

  static boolean isServicePrincipalUser(String userName, ProxyConfig.SecurityConfig security) {
    if (userName == null || security == null) {
      return false;
    }
    return userName.equals(shortUserName(security.serverPrincipal()));
  }

  static boolean shouldUseCompatibilityFallback(String methodName, Throwable cause) {
    return MetastoreCompatibility.shouldUseFallback(methodName, cause);
  }

  private void recordDefaultCatalogRouteIfImplicit(
      String methodName,
      String dbName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    if (namespace.catalogName().equals(config.defaultCatalog()) && router.resolvePattern(dbName).isEmpty()) {
      currentObservation().markDefaultCatalogRoute();
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
      currentObservation().markDefaultCatalogRoute();
      observability.metrics().recordDefaultCatalogRoute(methodName);
    }
  }

  private void emitAuditLog(long requestId, RequestObservation observation, long elapsedMs) {
    if (!AUDIT_LOG.isInfoEnabled()) {
      return;
    }
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("event", "hms_proxy_audit");
    fields.put("requestId", requestId);
    fields.put("method", observation.method());
    fields.put("catalog", observation.catalog());
    fields.put("backend", observation.backend());
    fields.put("status", observation.status());
    fields.put("durationMs", elapsedMs);
    fields.put("routed", observation.routed());
    fields.put("fanout", observation.fanout());
    fields.put("fallback", observation.fallback());
    fields.put("defaultCatalogRouted", observation.defaultCatalogRouted());
    fields.put("remoteAddress", ClientRequestContext.remoteAddress().orElse(null));
    fields.put("authenticatedUser", ClientRequestContext.remoteUser().orElse(null));
    AUDIT_LOG.info(AuditLogUtil.toJson(fields));
  }

  private static RequestObservation currentObservation() {
    RequestObservation observation = REQUEST_OBSERVATION.get();
    return observation == null ? new RequestObservation("unknown") : observation;
  }

  private static final class RequestObservation {
    private final String method;
    private String catalog = "none";
    private String backend = "none";
    private String status = "ok";
    private boolean routed;
    private boolean fanout;
    private boolean fallback;
    private boolean defaultCatalogRouted;

    private RequestObservation(String method) {
      this.method = method;
    }

    private String method() {
      return method;
    }

    private String catalog() {
      return catalog;
    }

    private String backend() {
      return backend;
    }

    private String status() {
      return status;
    }

    private boolean routed() {
      return routed;
    }

    private boolean fanout() {
      return fanout;
    }

    private boolean fallback() {
      return fallback;
    }

    private boolean defaultCatalogRouted() {
      return defaultCatalogRouted;
    }

    private void recordNamespace(CatalogRouter.ResolvedNamespace namespace) {
      catalog = namespace.catalogName();
      backend = namespace.backend().name();
      routed = true;
    }

    private void recordBackend(String backendName) {
      backend = backendName;
      if ("none".equals(catalog)) {
        catalog = backendName;
      }
      routed = true;
    }

    private void recordFanout() {
      catalog = "all";
      backend = "fanout";
      routed = true;
      fanout = true;
    }

    private void markFallback() {
      status = "fallback";
      fallback = true;
    }

    private void markError() {
      if (!"fallback".equals(status)) {
        status = "error";
      }
    }

    private void markDefaultCatalogRoute() {
      defaultCatalogRouted = true;
    }
  }
}
