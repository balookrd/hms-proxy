package io.github.mmalykhin.hmsproxy;

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
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RoutingMetaStoreHandler implements InvocationHandler {
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
  private static final List<String> DEFAULT_BACKEND_GLOBAL_METHODS = List.of(
      "set_ugi",
      "get_all_functions",
      "get_current_notificationEventId",
      "flushCache"
  );
  private static final List<String> DEFAULT_BACKEND_GLOBAL_PREFIXES = List.of(
      "get_",
      "show_",
      "list_"
  );

  private final ProxyConfig config;
  private final CatalogRouter router;
  private final long aliveSince;

  RoutingMetaStoreHandler(ProxyConfig config, CatalogRouter router) {
    this.config = config;
    this.router = router;
    this.aliveSince = System.currentTimeMillis() / 1000L;
  }

  @SuppressWarnings("unchecked")
  static <T> T newProxy(Class<T> interfaceClass, InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(
        interfaceClass.getClassLoader(),
        new Class<?>[] {interfaceClass},
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

    try {
      Object result = switch (name) {
        case "getName" -> config.server().name();
        case "getVersion" -> "hms-proxy/0.1.0";
        case "aliveSince" -> aliveSince;
        case "reinitialize", "shutdown" -> null;
        case "set_ugi" -> handleSetUgi(method, args);
        case "getStatus" -> enumConstant(method.getReturnType(), "ALIVE");
        case "get_catalogs" -> new GetCatalogsResponse(config.catalogNames());
        case "get_catalog" -> handleGetCatalog(args);
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
          .map(result -> NamespaceTranslator.externalizeTableMeta(result, resolved))
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
            router.resolveCatalog(backend.name(), result.getDbName())));
      }
    }
    return results;
  }

  private Object handleGetTableReq(Method method, Object[] args) throws Throwable {
    GetTableRequest request = (GetTableRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = resolveRequestNamespace(request.getCatName(), request.getDbName());
    GetTableRequest routedRequest = (GetTableRequest) NamespaceTranslator.internalizeArgument(request, namespace);
    Object result = invokeBackend(namespace.backend(), method, new Object[] {routedRequest});
    return NamespaceTranslator.externalizeResult(result, namespace);
  }

  private Object handleGetTablesReq(Method method, Object[] args) throws Throwable {
    GetTablesRequest request = (GetTablesRequest) args[0];
    CatalogRouter.ResolvedNamespace namespace = resolveRequestNamespace(request.getCatName(), request.getDbName());
    GetTablesRequest routedRequest = (GetTablesRequest) NamespaceTranslator.internalizeArgument(request, namespace);
    Object result = invokeBackend(namespace.backend(), method, new Object[] {routedRequest});
    return NamespaceTranslator.externalizeResult(result, namespace);
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
      return NamespaceTranslator.externalizeResult(result, namespace);
    }

    Object firstArgument = args[0];
    if (hasDbName(firstArgument)) {
      String dbName = readStringProperty(firstArgument, "getDbName");
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      Object[] routedArgs = internalizeObjectArguments(args, namespace);
      Object result = invokeBackend(namespace.backend(), method, routedArgs);
      return NamespaceTranslator.externalizeResult(result, namespace);
    }
    if (args.length > 1 && args[0] instanceof String dbName && args[1] instanceof String) {
      CatalogRouter.ResolvedNamespace namespace = router.resolveDatabase(dbName);
      Object[] routedArgs = internalizeDbStringArguments(args, namespace);
      Object result = invokeBackend(namespace.backend(), method, routedArgs);
      return NamespaceTranslator.externalizeResult(result, namespace);
    }

    return invokeGlobal(method, args);
  }

  private Object invokeGlobal(Method method, Object[] args) throws Throwable {
    if (isDefaultBackendGlobalMethod(method.getName())) {
      return invokeBackend(router.defaultBackend(), method, args);
    }
    if (!router.singleCatalog()) {
      throw metaException("Operation " + method.getName()
          + " has no catalog context; use explicit catalog.db naming or a catalog-aware request");
    }
    return invokeBackend(router.defaultBackend(), method, args);
  }

  private Object[] internalizeDbStringArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace) {
    Object[] routedArgs = Arrays.copyOf(args, args.length);
    routedArgs[0] = namespace.backendDbName();
    for (int index = 1; index < routedArgs.length; index++) {
      routedArgs[index] = NamespaceTranslator.internalizeArgument(routedArgs[index], namespace);
    }
    return routedArgs;
  }

  private Object[] internalizeObjectArguments(Object[] args, CatalogRouter.ResolvedNamespace namespace) {
    Object[] routedArgs = Arrays.copyOf(args, args.length);
    for (int index = 0; index < routedArgs.length; index++) {
      routedArgs[index] = NamespaceTranslator.internalizeArgument(routedArgs[index], namespace);
    }
    return routedArgs;
  }

  private CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws MetaException {
    if (catName != null && !catName.isBlank()) {
      return router.resolveCatalog(catName, dbName);
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
      Object result = backend.invoke(method, args, impersonation);
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} proxy-response catalog={} method={} elapsedMs={} result={}",
            requestId, backend.name(), method.getName(), elapsedMillis(startedAt),
            DebugLogUtil.formatValue(result));
      }
      return result;
    } catch (Throwable cause) {
      LOG.debug("requestId={} proxy-error catalog={} method={} elapsedMs={} error={}",
          requestId, backend.name(), method.getName(), elapsedMillis(startedAt), cause.toString(), cause);
      throw cause;
    }
  }

  private static long currentRequestId() {
    Long requestId = REQUEST_ID.get();
    return requestId == null ? -1L : requestId;
  }

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  private static boolean hasDbName(Object argument) {
    if (argument == null) {
      return false;
    }
    try {
      argument.getClass().getMethod("getDbName");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  private static String readStringProperty(Object argument, String getterName) {
    try {
      return (String) argument.getClass().getMethod(getterName).invoke(argument);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Unable to read property " + getterName + " from " + argument.getClass().getName(), e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object enumConstant(Class<?> enumType, String constantName) {
    return Enum.valueOf((Class<? extends Enum>) enumType.asSubclass(Enum.class), constantName);
  }

  private static MetaException metaException(String message) {
    return new MetaException(message);
  }

  private Object handleSetUgi(Method method, Object[] args) throws Throwable {
    if (!config.security().impersonationEnabled()) {
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
    if (!config.security().impersonationEnabled()) {
      return Optional.empty();
    }

    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      String userName = currentUser.getShortUserName();
      if (userName == null || userName.isBlank()) {
        return Optional.empty();
      }
      return Optional.of(new ImpersonationContext(userName, List.of(currentUser.getGroupNames())));
    } catch (Exception e) {
      throw metaException("Unable to resolve authenticated caller for impersonation: " + e.getMessage());
    }
  }

  record ImpersonationContext(String userName, List<String> groupNames) {
  }

  static boolean isDefaultBackendGlobalMethod(String methodName) {
    return DEFAULT_BACKEND_GLOBAL_METHODS.contains(methodName)
        || DEFAULT_BACKEND_GLOBAL_PREFIXES.stream().anyMatch(methodName::startsWith);
  }
}
