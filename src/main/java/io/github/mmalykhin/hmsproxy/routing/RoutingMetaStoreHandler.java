package io.github.mmalykhin.hmsproxy.routing;

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
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RoutingMetaStoreHandler implements InvocationHandler, HortonworksFrontendExtension, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(RoutingMetaStoreHandler.class);
  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("io.github.mmalykhin.hmsproxy.audit");
  private static final AtomicLong REQUEST_SEQUENCE = new AtomicLong();

  private final ProxyObservability observability;
  private final SyntheticReadLockManager syntheticReadLockManager;
  private final BackendRoutingController backendRoutingController;
  private final RoutingHandler routingHandler;
  private final InvocationHandler chain;

  public RoutingMetaStoreHandler(ProxyConfig config, CatalogRouter router, FrontDoorSecurity frontDoorSecurity) {
    this(config, router, frontDoorSecurity, new ProxyObservability(config));
  }

  public RoutingMetaStoreHandler(
      ProxyConfig config,
      CatalogRouter router,
      FrontDoorSecurity frontDoorSecurity,
      ProxyObservability observability
  ) {
    this.observability = observability;
    CompatibilityLayer compatibilityLayer = new CompatibilityLayer(config, frontDoorSecurity);
    FederationLayer federationLayer = new FederationLayer(config, router);
    TransactionalTableMutationGuard transactionalTableMutationGuard = new TransactionalTableMutationGuard(config);
    this.syntheticReadLockManager = new SyntheticReadLockManager(config, observability.metrics());
    RequestRateLimiter requestRateLimiter = new RequestRateLimiter(config, observability.metrics());
    this.backendRoutingController = new BackendRoutingController(config, router, observability);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(
        compatibilityLayer, backendRoutingController, observability, router, requestRateLimiter);
    long aliveSince = System.currentTimeMillis() / 1000L;

    ImpersonationResolver impersonationResolver = new ImpersonationResolver(config);
    this.routingHandler = new RoutingHandler(
        config, router, federationLayer, compatibilityLayer, observability, dispatcher, impersonationResolver);
    CompatibilityHandler compatibilityHandler = new CompatibilityHandler(
        config, compatibilityLayer, router, observability, dispatcher, impersonationResolver, aliveSince,
        this.routingHandler);
    LockHandler lockHandler = new LockHandler(
        syntheticReadLockManager, requestRateLimiter, router, federationLayer, observability, compatibilityHandler);
    this.chain = new RateLimitingHandler(requestRateLimiter, transactionalTableMutationGuard, lockHandler);
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
    RequestContext.REQUEST_ID.set(requestId);
    RequestContext.REQUEST_OBSERVATION.set(observation);
    if (LOG.isDebugEnabled()) {
      LOG.debug("requestId={} incoming method={} args={}",
          requestId, name, DebugLogUtil.formatArgs(args));
    }
    if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(name)) {
      LOG.info("requestId={} trace stage=client-request method={} summary={}",
          requestId, name, WriteTraceUtil.summarizeArgs(args));
    }

    try {
      Object result = chain.invoke(proxy, method, args);
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
      if (throwable instanceof RateLimitExceededException) {
        observation.markThrottled();
      } else {
        observation.markError();
      }
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
          new MetaException("Proxy internal error in " + name + ": " + throwable.getClass().getSimpleName()
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
      RequestContext.REQUEST_ID.remove();
      RequestContext.REQUEST_OBSERVATION.remove();
    }
  }

  @Override
  public void close() throws MetaException {
    syntheticReadLockManager.close();
    backendRoutingController.close();
  }

  @Override
  public Object addWriteNotificationLog(Object request) throws Throwable {
    Method method = HortonworksFrontendExtension.class.getMethod("addWriteNotificationLog", Object.class);
    return invoke(null, method, new Object[]{request});
  }

  @Override
  public Object getTablesExt(Object request) throws Throwable {
    Method method = HortonworksFrontendExtension.class.getMethod("getTablesExt", Object.class);
    return invoke(null, method, new Object[]{request});
  }

  @Override
  public Object getAllMaterializedViewObjectsForRewriting() throws Throwable {
    Method method = HortonworksFrontendExtension.class.getMethod("getAllMaterializedViewObjectsForRewriting");
    return invoke(null, method, new Object[0]);
  }

  private void emitAuditLog(long requestId, RequestObservation observation, long elapsedMs) {
    if (!AUDIT_LOG.isInfoEnabled()) {
      return;
    }
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("event", "hms_proxy_audit");
    fields.put("requestId", requestId);
    fields.put("method", observation.method());
    fields.put("operationClass", observation.operationClass());
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

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  private static double elapsedSeconds(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000_000.0d;
  }

  // --- Package-private helpers used by tests ---

  CatalogRouter.ResolvedNamespace resolveRequestNamespace(String catName, String dbName)
      throws org.apache.hadoop.hive.metastore.api.MetaException {
    return routingHandler.resolveRequestNamespace(catName, dbName);
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
}
