package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.frontend.HortonworksFrontendExtension;
import io.github.mmalykhin.hmsproxy.security.FrontDoorSecurity;
import io.github.mmalykhin.hmsproxy.util.DebugLogUtil;
import io.github.mmalykhin.hmsproxy.util.WriteTraceUtil;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RoutingMetaStoreHandler implements InvocationHandler, HortonworksFrontendExtension {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingMetaStoreHandler.class);
  private static final AtomicLong REQUEST_SEQUENCE = new AtomicLong();
  private static final ThreadLocal<Long> REQUEST_ID = new ThreadLocal<>();
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
  private final ProxyConfig config;
  private final CatalogRouter router;
  private final FrontDoorSecurity frontDoorSecurity;
  private final long aliveSince;

  public RoutingMetaStoreHandler(ProxyConfig config, CatalogRouter router, FrontDoorSecurity frontDoorSecurity) {
    this.config = config;
    this.router = router;
    this.frontDoorSecurity = frontDoorSecurity;
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
    REQUEST_ID.set(requestId);
    if (LOG.isDebugEnabled()) {
      LOG.debug("requestId={} incoming method={} args={}",
          requestId, name, DebugLogUtil.formatArgs(args));
    }
    if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(name)) {
      LOG.info("requestId={} trace stage=client-request method={} summary={}",
          requestId, name, WriteTraceUtil.summarizeArgs(args));
    }

    try {
      Object result = switch (name) {
        case "getName" -> config.server().name();
        case "getVersion" -> config.compatibility().frontendProfile().metastoreVersion();
        case "aliveSince" -> aliveSince;
        case "reinitialize", "shutdown" -> null;
        case "set_ugi" -> handleSetUgi(method, args);
        case "get_delegation_token", "renew_delegation_token", "cancel_delegation_token" ->
            MetastoreCompatibility.handleLocally(name, args, frontDoorSecurity);
        case "getStatus" -> enumConstant(method.getReturnType(), "ALIVE");
        case "get_catalogs" -> new GetCatalogsResponse(config.catalogNames());
        case "get_catalog" -> handleGetCatalog(args);
        case "get_config_value" -> handleGetConfigValue(method, args);
        case "create_catalog", "alter_catalog", "drop_catalog" ->
            throw metaException("Catalog definitions are managed by proxy config, not via HMS API");
        case "get_all_databases" -> handleGetAllDatabases(method);
        case "get_databases" -> handleGetDatabases(method, args);
        case "get_table_meta" -> handleGetTableMeta(method, args);
        case "get_table_req" -> handleGetTableReq(method, args);
        case "get_table_objects_by_name_req" -> handleGetTablesReq(method, args);
        default -> routeByNamespaceOrFail(method, args);
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
      REQUEST_ID.remove();
    }
  }

  @Override
  public Object addWriteNotificationLog(Object request) throws Throwable {
    String dbName = (String) request.getClass().getMethod("getDb").invoke(request);
    CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
    CatalogBackend backend = namespace.backend();
    if (backend.runtimeProfile() != MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_0_78) {
      throw metaException(
          "Hortonworks add_write_notification_log requires a Hortonworks backend runtime for catalog '"
              + backend.name()
              + "'");
    }

    Object routedRequest = cloneWriteNotificationLogRequest(request);
    routedRequest.getClass().getMethod("setDb", String.class).invoke(routedRequest, namespace.backendDbName());
    return invokeBackendNamed(backend, "add_write_notification_log", routedRequest);
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
    List<String> databases = new ArrayList<>();
    for (CatalogBackend backend : router.backends()) {
      @SuppressWarnings("unchecked")
      List<String> backendDatabases = (List<String>) invokeBackend(backend, method, null);
      for (String database : backendDatabases) {
        databases.add(router.externalDatabaseName(backend.name(), database));
      }
    }
    return databases;
  }

  private Object handleGetDatabases(Method method, Object[] args) throws Throwable {
    String pattern = (String) args[0];
    CatalogRouter.ResolvedNamespace resolved = router.resolvePattern(pattern).orElse(null);
    if (resolved != null) {
      @SuppressWarnings("unchecked")
      List<String> backendDatabases =
          (List<String>) invokeBackend(resolved.backend(), method, new Object[] {resolved.backendDbName()});
      return backendDatabases.stream()
          .map(db -> router.externalDatabaseName(resolved.catalogName(), db))
          .toList();
    }

    List<String> databases = new ArrayList<>();
    for (CatalogBackend backend : router.backends()) {
      @SuppressWarnings("unchecked")
      List<String> backendDatabases =
          (List<String>) invokeBackend(backend, method, new Object[] {pattern});
      for (String database : backendDatabases) {
        databases.add(router.externalDatabaseName(backend.name(), database));
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
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults = (List<TableMeta>) invokeBackend(
          resolved.backend(), method, new Object[] {resolved.backendDbName(), tablePattern, tableTypes});
      return backendResults.stream()
          .map(result -> NamespaceTranslator.externalizeTableMeta(result, resolved, preserveBackendCatalogName()))
          .toList();
    }

    List<TableMeta> results = new ArrayList<>();
    for (CatalogBackend backend : router.backends()) {
      @SuppressWarnings("unchecked")
      List<TableMeta> backendResults =
          (List<TableMeta>) invokeBackend(backend, method, new Object[] {dbPattern, tablePattern, tableTypes});
      for (TableMeta result : backendResults) {
        results.add(NamespaceTranslator.externalizeTableMeta(
            result,
            router.resolveCatalog(backend.name(), result.getDbName()),
            preserveBackendCatalogName()));
      }
    }
    return results;
  }

  private Object handleGetTableReq(Method method, Object[] args) throws Throwable {
    GetTableRequest request = (GetTableRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = resolveRequestNamespace(request.getCatName(), request.getDbName());
    CatalogBackend backend = namespace.backend();
    GetTableRequest routedRequest =
        (GetTableRequest) NamespaceTranslator.internalizeArgument(request, namespace, preserveBackendCatalogName());
    Object result = invokeBackendRequest(backend, routedRequest, method.getName());
    return NamespaceTranslator.externalizeResult(result, namespace, preserveBackendCatalogName());
  }

  private Object handleGetTablesReq(Method method, Object[] args) throws Throwable {
    GetTablesRequest request = (GetTablesRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = resolveRequestNamespace(request.getCatName(), request.getDbName());
    CatalogBackend backend = namespace.backend();
    GetTablesRequest routedRequest =
        (GetTablesRequest) NamespaceTranslator.internalizeArgument(request, namespace, preserveBackendCatalogName());
    Object result = invokeBackendRequest(backend, routedRequest, method.getName());
    return NamespaceTranslator.externalizeResult(result, namespace, preserveBackendCatalogName());
  }

  private Object routeByNamespaceOrFail(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    if (args == null || args.length == 0) {
      return invokeGlobal(method, args);
    }

    if (DB_STRING_METHODS.contains(methodName) && args[0] instanceof String dbName) {
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      Object[] routedArgs = internalizeDbStringArguments(args, namespace);
      Object result = invokeBackend(namespace.backend(), method, routedArgs);
      return NamespaceTranslator.externalizeResult(result, namespace, preserveBackendCatalogName());
    }

    CatalogRouter.ResolvedNamespace extractedNamespace = findNamespaceInArgs(args);
    if (extractedNamespace != null) {
      Object[] routedArgs = internalizeObjectArguments(args, extractedNamespace);
      Object result = invokeBackend(extractedNamespace.backend(), method, routedArgs);
      return NamespaceTranslator.externalizeResult(result, extractedNamespace, preserveBackendCatalogName());
    }
    if (args.length > 1 && args[0] instanceof String dbName && args[1] instanceof String) {
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      Object[] routedArgs = internalizeDbStringArguments(args, namespace);
      Object result = invokeBackend(namespace.backend(), method, routedArgs);
      return NamespaceTranslator.externalizeResult(result, namespace, preserveBackendCatalogName());
    }

    return invokeGlobal(method, args);
  }

  private Object invokeGlobal(Method method, Object[] args) throws Throwable {
    if (MetastoreCompatibility.routesToDefaultBackend(method.getName())) {
      return invokeBackend(router.defaultBackend(), method, args);
    }
    if (!router.singleCatalog()) {
      throw metaException("Operation " + method.getName()
          + " has no catalog context; use explicit catalog.db naming or a catalog-aware request");
    }
    return invokeBackend(router.defaultBackend(), method, args);
  }

  private Object handleGetConfigValue(Method method, Object[] args) throws Throwable {
    String requestedName = args != null && args.length > 0 ? (String) args[0] : null;
    String defaultValue = args != null && args.length > 1 ? (String) args[1] : null;
    Optional<String> compatibilityValue = MetastoreCompatibility.compatibleConfigValue(
        requestedName,
        defaultValue,
        config.catalogs().get(config.defaultCatalog()).hiveConf());
    if (compatibilityValue.isPresent()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} returning compatibility config value for key '{}'",
            currentRequestId(), requestedName);
      }
      return compatibilityValue.get();
    }
    return invokeBackend(router.defaultBackend(), method, args);
  }

  private Object[] internalizeDbStringArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace) {
    Object[] routedArgs = Arrays.copyOf(args, args.length);
    routedArgs[0] = namespace.backendDbName();
    for (int index = 1; index < routedArgs.length; index++) {
      routedArgs[index] = NamespaceTranslator.internalizeArgument(
          routedArgs[index], namespace, preserveBackendCatalogName());
    }
    return routedArgs;
  }

  private Object[] internalizeObjectArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace) {
    Object[] routedArgs = Arrays.copyOf(args, args.length);
    for (int index = 0; index < routedArgs.length; index++) {
      routedArgs[index] = routedArgs[index] instanceof String dbName
          ? NamespaceTranslator.internalizeStringArgument(dbName, namespace)
          : NamespaceTranslator.internalizeArgument(
              routedArgs[index], namespace, preserveBackendCatalogName());
    }
    return routedArgs;
  }

  private CatalogRouter.ResolvedNamespace findNamespaceInArgs(Object[] args) throws MetaException {
    for (Object argument : args) {
      String extractedDbName = NamespaceTranslator.extractDbName(argument);
      if (extractedDbName != null) {
        return router.resolveDatabase(extractedDbName);
      }
    }
    for (Object argument : args) {
      if (!(argument instanceof String candidate)) {
        continue;
      }
      CatalogRouter.ResolvedNamespace explicitNamespace = router.resolvePattern(candidate).orElse(null);
      if (explicitNamespace != null) {
        return explicitNamespace;
      }
    }
    return null;
  }

  private CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException {
    if (catName != null && !catName.isBlank()) {
      Optional<CatalogRouter.ResolvedNamespace> explicitNamespace = router.resolveCatalogIfKnown(catName, dbName);
      if (explicitNamespace.isPresent()) {
        CatalogRouter.ResolvedNamespace resolvedByDb = router.resolvePattern(dbName).orElse(null);
        if (resolvedByDb != null) {
          if (!resolvedByDb.catalogName().equals(catName)) {
            throw metaException("Request has conflicting catalog and database namespace: catName='"
                + catName + "', dbName='" + dbName + "'");
          }
          return resolvedByDb;
        }
        return explicitNamespace.get();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} ignoring unknown request catalog '{}' and resolving by dbName='{}'",
            currentRequestId(), catName, dbName);
      }
    }
    return router.resolveDatabase(dbName);
  }

  private Object invokeBackend(CatalogBackend backend, Method method, Object[] args) throws Throwable {
    long startedAt = System.nanoTime();
    Long requestId = currentRequestId();
    ImpersonationContext impersonation = currentImpersonation().orElse(null);
    try {
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
      return result;
    } catch (Throwable cause) {
      Optional<Object> compatibilityFallback = MetastoreCompatibility.fallback(method.getName(), cause);
      if (compatibilityFallback.isPresent()) {
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
      throw cause;
    }
  }

  private Object invokeBackendRequest(CatalogBackend backend, Object request, String methodName) throws Throwable {
    long startedAt = System.nanoTime();
    Long requestId = currentRequestId();
    ImpersonationContext impersonation = currentImpersonation().orElse(null);
    try {
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
      return adapted;
    } catch (Throwable cause) {
      Optional<Object> compatibilityFallback = MetastoreCompatibility.fallback(methodName, cause);
      if (compatibilityFallback.isPresent()) {
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
    long startedAt = System.nanoTime();
    Long requestId = currentRequestId();
    ImpersonationContext impersonation = currentImpersonation().orElse(null);
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-call catalog={} method={} impersonationUser={} args={}",
            requestId,
            backend.name(),
            methodName,
            impersonation == null ? "-" : impersonation.userName(),
            DebugLogUtil.formatArgs(new Object[] {request}));
      }
      Object result = backend.invokeRawByName(
          methodName,
          new Class<?>[] {request.getClass()},
          new Object[] {request},
          impersonation);
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-response catalog={} method={} elapsedMs={} result={}",
            requestId, backend.name(), methodName, elapsedMillis(startedAt), DebugLogUtil.formatValue(result));
      }
      return result;
    } catch (Throwable cause) {
      LOG.debug("requestId={} proxy-error catalog={} method={} elapsedMs={} error={}",
          requestId, backend.name(), methodName, elapsedMillis(startedAt), cause.toString(), cause);
      throw cause;
    }
  }

  private static Object cloneWriteNotificationLogRequest(Object request) throws ReflectiveOperationException {
    return request.getClass().getConstructor(request.getClass()).newInstance(request);
  }

  private static long currentRequestId() {
    Long requestId = REQUEST_ID.get();
    return requestId == null ? -1L : requestId;
  }

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  private boolean preserveBackendCatalogName() {
    return config.compatibility().preserveBackendCatalogName();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object enumConstant(Class<?> enumType, String constantName) {
    return Enum.valueOf((Class<? extends Enum>) enumType.asSubclass(Enum.class), constantName);
  }

  private static MetaException metaException(String message) {
    return new MetaException(message);
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
    return MetastoreCompatibility.routesToDefaultBackend(methodName);
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
}
