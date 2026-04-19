package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;

final class AdmissionGate {

  private final BackendRoutingController backendRoutingController;
  private final RequestRateLimiter requestRateLimiter;

  AdmissionGate(BackendRoutingController backendRoutingController, RequestRateLimiter requestRateLimiter) {
    this.backendRoutingController = backendRoutingController;
    this.requestRateLimiter = requestRateLimiter;
  }

  void enforceRateLimit(String methodName, String catalogName) throws RateLimitExceededException {
    if (!RequestContext.currentObservation().shouldRateLimitCatalog(catalogName)) {
      return;
    }
    requestRateLimiter.enforceCatalog(methodName, catalogName);
  }

  ProxyRuntimeState.BackendCallAdmission admit(CatalogBackend backend) throws MetaException {
    return backendRoutingController.admit(backend);
  }

  void recordSuccess(CatalogBackend backend, long elapsedMs) {
    backendRoutingController.recordSuccess(backend, elapsedMs);
  }

  void recordFailure(CatalogBackend backend, Throwable cause, long elapsedMs) {
    backendRoutingController.recordFailure(backend, cause, elapsedMs);
  }

  static MetaException unavailableException(
      CatalogBackend backend,
      String methodName,
      ProxyRuntimeState.BackendCallAdmission admission
  ) {
    String reason = admission.rejectionReason() == null ? "backend unavailable" : admission.rejectionReason();
    String message = "Backend catalog '" + backend.name() + "' rejected method '" + methodName + "' because " + reason;
    if (admission.retryAtEpochMs() > 0L) {
      long retryInMs = Math.max(0L, admission.retryAtEpochMs() - System.currentTimeMillis());
      message += "; next retry window in " + retryInMs + "ms";
    }
    return new MetaException(message);
  }
}
