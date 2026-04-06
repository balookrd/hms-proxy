package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Third handler in the invocation chain. Answers locally-serviceable requests
 * (introspection, catalog enumeration, compatibility shims) without reaching backends.
 * Delegates genuine routing requests to the next handler.
 */
final class CompatibilityHandler implements InvocationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CompatibilityHandler.class);

  private final ProxyConfig config;
  private final CompatibilityLayer compatibilityLayer;
  private final CatalogRouter router;
  private final ProxyObservability observability;
  private final BackendCallDispatcher dispatcher;
  private final ImpersonationResolver impersonationResolver;
  private final long aliveSince;
  private final InvocationHandler next;

  CompatibilityHandler(
      ProxyConfig config,
      CompatibilityLayer compatibilityLayer,
      CatalogRouter router,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      long aliveSince,
      InvocationHandler next
  ) {
    this.config = config;
    this.compatibilityLayer = compatibilityLayer;
    this.router = router;
    this.observability = observability;
    this.dispatcher = dispatcher;
    this.impersonationResolver = impersonationResolver;
    this.aliveSince = aliveSince;
    this.next = next;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    String name = method.getName();
    return switch (name) {
      case "getName" -> config.server().name();
      case "getVersion" -> compatibilityLayer.frontendVersion();
      case "aliveSince" -> aliveSince;
      case "reinitialize", "shutdown" -> null;
      case "getStatus" -> enumConstant(method.getReturnType(), "ALIVE");
      case "get_catalogs" -> new GetCatalogsResponse(config.catalogNames());
      case "get_catalog" -> handleGetCatalog(args);
      case "create_catalog", "alter_catalog", "drop_catalog" ->
          throw new MetaException("Catalog definitions are policy-owned by proxy config; HMS API catalog mutations"
              + " are disabled to preserve explicit namespace ownership");
      case "get_config_value" -> handleGetConfigValue(method, args);
      default -> MetastoreCompatibility.handlesLocally(name)
          ? compatibilityLayer.handleLocalMethod(name, args)
          : next.invoke(proxy, method, args);
    };
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

  private Object handleGetConfigValue(Method method, Object[] args) throws Throwable {
    String requestedName = args != null && args.length > 0 ? (String) args[0] : null;
    String defaultValue = args != null && args.length > 1 ? (String) args[1] : null;
    java.util.Optional<String> compatibilityValue = compatibilityLayer.compatibleConfigValue(
        requestedName,
        defaultValue,
        config.catalogs().get(config.defaultCatalog()).hiveConf());
    if (compatibilityValue.isPresent()) {
      RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
      if (LOG.isDebugEnabled()) {
        LOG.debug("requestId={} returning compatibility config value for key '{}'",
            RequestContext.currentRequestId(), requestedName);
      }
      return compatibilityValue.get();
    }
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    observability.metrics().recordDefaultCatalogRoute(method.getName());
    return dispatcher.invokeBackend(
        router.defaultBackend(), method, args,
        impersonationResolver.resolve().orElse(null), RequestContext.currentRequestId(),
        true, true);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object enumConstant(Class<?> enumType, String constantName) {
    return Enum.valueOf((Class<? extends Enum>) enumType.asSubclass(Enum.class), constantName);
  }
}
