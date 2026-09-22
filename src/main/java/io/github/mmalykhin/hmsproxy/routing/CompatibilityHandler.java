package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.compatibility.MetastoreCompatibility;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
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
  private final ConfigValueCache configValueCache;
  private final AtomicReference<String> cachedDbUuid = new AtomicReference<>();

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
    this(config, compatibilityLayer, router, observability, dispatcher, impersonationResolver, aliveSince, next,
        new ConfigValueCache(config.latencyRouting().configValueCache()));
  }

  CompatibilityHandler(
      ProxyConfig config,
      CompatibilityLayer compatibilityLayer,
      CatalogRouter router,
      ProxyObservability observability,
      BackendCallDispatcher dispatcher,
      ImpersonationResolver impersonationResolver,
      long aliveSince,
      InvocationHandler next,
      ConfigValueCache configValueCache
  ) {
    this.config = config;
    this.compatibilityLayer = compatibilityLayer;
    this.router = router;
    this.observability = observability;
    this.dispatcher = dispatcher;
    this.impersonationResolver = impersonationResolver;
    this.aliveSince = aliveSince;
    this.next = next;
    this.configValueCache = configValueCache;
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
      case "partition_name_has_valid_characters" -> handlePartitionNameHasValidCharacters(args);
      case "getMetaConf" -> handleGetMetaConf(args);
      case "setMetaConf" -> handleSetMetaConf(args);
      case "get_metastore_db_uuid" -> handleGetMetastoreDbUuid(method, args);
      case "flushCache" -> handleFlushCache(method, args);
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
    return configValueCache.getOrFetch(requestedName, defaultValue, (name, sentinel) -> {
      Object[] backendArgs = new Object[] {name, sentinel};
      return (String) dispatcher.invokeDirect(
          router.defaultBackend(), method, backendArgs,
          null, RequestContext.currentRequestId(),
          true, true);
    });
  }

  private Object handlePartitionNameHasValidCharacters(Object[] args) throws MetaException {
    @SuppressWarnings("unchecked")
    List<String> partVals = args != null && args.length > 0 ? (List<String>) args[0] : Collections.emptyList();
    boolean throwException = args != null && args.length > 1 && Boolean.TRUE.equals(args[1]);
    String pattern = resolvePartitionValidationPattern();
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    return MetastoreCompatibility.partitionNameHasValidCharacters(partVals, throwException, pattern);
  }

  private String resolvePartitionValidationPattern() {
    Map<String, String> hiveConf = config.catalogs().get(config.defaultCatalog()).hiveConf();
    if (hiveConf != null) {
      String pattern = hiveConf.get(MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN.getVarname());
      if (pattern != null) {
        return pattern;
      }
      pattern = hiveConf.get(MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN.getHiveName());
      if (pattern != null) {
        return pattern;
      }
    }
    Object defaultVal = MetastoreConf.ConfVars.PARTITION_NAME_WHITELIST_PATTERN.getDefaultVal();
    return defaultVal != null ? defaultVal.toString() : null;
  }

  private Object handleGetMetaConf(Object[] args) throws MetaException {
    String key = args != null && args.length > 0 ? (String) args[0] : null;
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    Map<String, String> hiveConf = config.catalogs().get(config.defaultCatalog()).hiveConf();
    return MetastoreCompatibility.getMetaConf(key, hiveConf);
  }

  private Object handleSetMetaConf(Object[] args) throws MetaException {
    String key = args != null && args.length > 0 ? (String) args[0] : null;
    String value = args != null && args.length > 1 ? (String) args[1] : null;
    RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
    MetastoreCompatibility.setMetaConf(key, value);
    return null;
  }

  private Object handleGetMetastoreDbUuid(Method method, Object[] args) {
    String cached = cachedDbUuid.get();
    if (cached != null) {
      return cached;
    }
    String uuid = null;
    try {
      RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
      observability.metrics().recordDefaultCatalogRoute(method.getName());
      uuid = (String) dispatcher.invokeDirect(
          router.defaultBackend(), method, args,
          null, RequestContext.currentRequestId(),
          true, true);
    } catch (Throwable t) {
      LOG.warn("requestId={} failed to query metastore db uuid from default backend: {}",
          RequestContext.currentRequestId(), t.getMessage());
    }
    if (uuid == null || uuid.isEmpty()) {
      uuid = UUID.nameUUIDFromBytes(config.server().name().getBytes(StandardCharsets.UTF_8)).toString();
    }
    cachedDbUuid.set(uuid);
    return uuid;
  }

  private Object handleFlushCache(Method method, Object[] args) {
    configValueCache.invalidateAll();
    if (next instanceof RoutingHandler rh) {
      rh.flushCaches();
    }
    try {
      RequestContext.currentObservation().recordNamespace(router.resolveCatalog(config.defaultCatalog(), ""));
      observability.metrics().recordDefaultCatalogRoute(method.getName());
      dispatcher.invokeDirect(
          router.defaultBackend(), method, args,
          null, RequestContext.currentRequestId(),
          true, true);
    } catch (Throwable t) {
      LOG.warn("requestId={} backend flushCache invocation failed: {}",
          RequestContext.currentRequestId(), t.getMessage());
    }
    return null;
  }

  ConfigValueCache configValueCache() {
    return configValueCache;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object enumConstant(Class<?> enumType, String constantName) {
    return Enum.valueOf((Class<? extends Enum>) enumType.asSubclass(Enum.class), constantName);
  }
}
