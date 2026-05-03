package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Mutable per-request observation state accumulated while processing one HMS call.
 * Lives on the request thread via {@link RequestContext#REQUEST_OBSERVATION}.
 * Not thread-safe — all mutation must happen on the request thread. Fanout workers
 * receive their own throwaway {@link RequestObservation}; {@link FanoutExecutor}
 * harvests per-task signals (e.g. fallback) and folds them into the parent
 * observation on the request thread.
 */
final class RequestObservation {
  private final String method;
  private final String operationClass;
  private final Set<String> rateLimitedCatalogs = new LinkedHashSet<>();
  private String catalog = "none";
  private String backend = "none";
  private String status = "ok";
  private boolean routed;
  private boolean fanout;
  private boolean fallback;
  private boolean defaultCatalogRouted;

  RequestObservation(String method) {
    this.method = method;
    this.operationClass = HmsOperationPolicy.describe(method).operationClass().wireName();
  }

  String method() {
    return method;
  }

  String catalog() {
    return catalog;
  }

  String backend() {
    return backend;
  }

  String status() {
    return status;
  }

  String operationClass() {
    return operationClass;
  }

  boolean routed() {
    return routed;
  }

  boolean fanout() {
    return fanout;
  }

  boolean fallback() {
    return fallback;
  }

  boolean defaultCatalogRouted() {
    return defaultCatalogRouted;
  }

  void recordNamespace(CatalogRouter.ResolvedNamespace namespace) {
    catalog = namespace.catalogName();
    backend = namespace.backend().name();
    routed = true;
  }

  void recordBackend(String backendName) {
    backend = backendName;
    if ("none".equals(catalog)) {
      catalog = backendName;
    }
    routed = true;
  }

  void recordFanout() {
    catalog = "all";
    backend = "fanout";
    routed = true;
    fanout = true;
  }

  void markFallback() {
    status = "fallback";
    fallback = true;
  }

  void markDegraded() {
    if (!"fallback".equals(status) && !"throttled".equals(status)) {
      status = "degraded";
    }
  }

  void markError() {
    if (!"fallback".equals(status) && !"throttled".equals(status)) {
      status = "error";
    }
  }

  void markThrottled() {
    status = "throttled";
  }

  void markDefaultCatalogRoute() {
    defaultCatalogRouted = true;
  }

  boolean shouldRateLimitCatalog(String catalogName) {
    if (catalogName == null || catalogName.isBlank()) {
      return false;
    }
    return rateLimitedCatalogs.add(catalogName);
  }
}
