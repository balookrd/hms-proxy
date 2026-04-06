package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.util.DebugLogUtil;
import io.github.mmalykhin.hmsproxy.util.WriteTraceUtil;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles all backend invocation concerns: circuit-breaker admission, rate limiting,
 * compatibility fallback, request/response logging, fanout reads (parallel and
 * sequential), and backend-error normalisation.
 *
 * <p>Single-backend calls are exposed through the {@code invokeBackend*} family.
 * Fanout calls are exposed through {@link #invokeFanoutRead}.
 */
final class BackendCallDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(BackendCallDispatcher.class);

  private final CompatibilityLayer compatibilityLayer;
  private final BackendRoutingController backendRoutingController;
  private final ProxyObservability observability;
  private final CatalogRouter router;
  private final RequestRateLimiter requestRateLimiter;

  BackendCallDispatcher(
      CompatibilityLayer compatibilityLayer,
      BackendRoutingController backendRoutingController,
      ProxyObservability observability,
      CatalogRouter router,
      RequestRateLimiter requestRateLimiter
  ) {
    this.compatibilityLayer = compatibilityLayer;
    this.backendRoutingController = backendRoutingController;
    this.observability = observability;
    this.router = router;
    this.requestRateLimiter = requestRateLimiter;
  }

  Object invokeBackend(
      CatalogBackend backend,
      Method method,
      Object[] args,
      ImpersonationContext impersonation,
      long requestId,
      boolean recordObservation,
      boolean enforceRateLimit
  ) throws Throwable {
    return performBackendCall(
        backend,
        method.getName(),
        args,
        impersonation,
        requestId,
        recordObservation,
        enforceRateLimit,
        true,
        () -> backend.invoke(method, args, impersonation),
        method);
  }

  Object invokeBackendRequest(
      CatalogBackend backend,
      Object request,
      String methodName,
      ImpersonationContext impersonation,
      long requestId
  ) throws Throwable {
    return performBackendCall(
        backend,
        methodName,
        new Object[]{request},
        impersonation,
        requestId,
        true,
        true,
        true,
        () -> backend.invokeRequest(methodName, request, impersonation),
        null);
  }

  Object invokeBackendByName(
      CatalogBackend backend,
      String methodName,
      Class<?>[] parameterTypes,
      Object[] args,
      ImpersonationContext impersonation,
      long requestId
  ) throws Throwable {
    return performBackendCall(
        backend,
        methodName,
        args,
        impersonation,
        requestId,
        true,
        true,
        false,
        () -> backend.invokeRawByName(methodName, parameterTypes, args, impersonation),
        null);
  }

  <T> List<FanoutBackendResult<T>> invokeFanoutRead(
      String methodName,
      FanoutBackendCall<T> call,
      ImpersonationContext impersonation,
      long requestId
  ) throws Throwable {
    List<CatalogBackend> backends = new ArrayList<>(router.backends());
    for (CatalogBackend backend : backends) {
      enforceCatalogRateLimit(methodName, backend.name());
    }
    return backendRoutingController.hedgedReadEnabled(methodName)
        ? invokeParallelFanoutRead(methodName, backends, impersonation, requestId, call)
        : invokeSequentialFanoutRead(methodName, backends, impersonation, requestId, call);
  }

  private Object performBackendCall(
      CatalogBackend backend,
      String methodName,
      Object[] args,
      ImpersonationContext impersonation,
      long requestId,
      boolean recordObservation,
      boolean enforceRateLimit,
      boolean allowCompatibilityFallback,
      BackendCall call,
      Method declaredMethod
  ) throws Throwable {
    long startedAt = System.nanoTime();
    if (enforceRateLimit) {
      enforceCatalogRateLimit(methodName, backend.name());
    }
    if (recordObservation) {
      RequestContext.currentObservation().recordBackend(backend.name());
    }

    ProxyRuntimeState.BackendCallAdmission admission = backendRoutingController.admit(backend);
    if (!admission.allowed()) {
      return maybeCompatibilityFallback(
          backend,
          methodName,
          requestId,
          allowCompatibilityFallback,
          backendUnavailableException(backend, methodName, admission),
          declaredMethod);
    }

    try {
      logBackendRequest(requestId, backend, methodName, impersonation, args);
      Object result = call.call();
      long elapsedMs = elapsedMillis(startedAt);
      logBackendResponse(requestId, backend, methodName, elapsedMs, result);
      backendRoutingController.recordSuccess(backend, elapsedMs);
      return result;
    } catch (Throwable cause) {
      long elapsedMs = elapsedMillis(startedAt);
      observability.metrics().recordBackendFailure(backend.name(), cause);
      backendRoutingController.recordFailure(backend, cause, elapsedMs);
      Optional<Object> compatibilityFallback =
          allowCompatibilityFallback ? compatibilityLayer.fallback(methodName, cause) : Optional.empty();
      if (compatibilityFallback.isPresent()) {
        RequestContext.currentObservation().markFallback();
        observability.metrics().recordBackendFallback(
            methodName,
            backend.runtimeProfile().name(),
            compatibilityLayer.frontendRuntimeProfile().name());
        LOG.warn("requestId={} backend catalog={} failed compatibility method {}, returning fallback",
            requestId, backend.name(), methodName, cause);
        return compatibilityFallback.get();
      }
      logBackendError(requestId, backend, methodName, elapsedMs, cause);
      if (declaredMethod != null) {
        throw normalizeBackendFailure(declaredMethod, backend.name(), cause);
      }
      throw cause;
    }
  }

  private Object maybeCompatibilityFallback(
      CatalogBackend backend,
      String methodName,
      long requestId,
      boolean allowCompatibilityFallback,
      Throwable cause,
      Method declaredMethod
  ) throws Throwable {
    Optional<Object> fallback =
        allowCompatibilityFallback ? compatibilityLayer.fallback(methodName, cause) : Optional.empty();
    if (fallback.isPresent()) {
      RequestContext.currentObservation().markFallback();
      observability.metrics().recordBackendFallback(
          methodName,
          backend.runtimeProfile().name(),
          compatibilityLayer.frontendRuntimeProfile().name());
      LOG.warn("requestId={} backend catalog={} served compatibility fallback after fast rejection in method {}",
          requestId, backend.name(), methodName, cause);
      return fallback.get();
    }
    if (declaredMethod != null) {
      throw normalizeBackendFailure(declaredMethod, backend.name(), cause);
    }
    throw cause;
  }

  private void enforceCatalogRateLimit(String methodName, String catalogName)
      throws RateLimitExceededException {
    if (!RequestContext.currentObservation().shouldRateLimitCatalog(catalogName)) {
      return;
    }
    requestRateLimiter.enforceCatalog(methodName, catalogName);
  }

  private <T> List<FanoutBackendResult<T>> invokeParallelFanoutRead(
      String methodName,
      List<CatalogBackend> backends,
      ImpersonationContext impersonation,
      long requestId,
      FanoutBackendCall<T> call
  ) throws Throwable {
    RequestObservation parentObservation = RequestContext.REQUEST_OBSERVATION.get();
    List<Future<FanoutTaskResult<T>>> futures = new ArrayList<>(backends.size());
    for (CatalogBackend backend : backends) {
      futures.add(backendRoutingController.fanoutExecutor().submit(() -> {
        if (parentObservation != null) {
          RequestContext.REQUEST_OBSERVATION.set(parentObservation);
        }
        try {
          return FanoutTaskResult.success(backend, call.call(backend, impersonation, requestId));
        } catch (Throwable error) {
          return FanoutTaskResult.failure(backend, error);
        } finally {
          RequestContext.REQUEST_OBSERVATION.remove();
        }
      }));
    }

    List<FanoutBackendResult<T>> results = new ArrayList<>(backends.size());
    for (Future<FanoutTaskResult<T>> future : futures) {
      FanoutTaskResult<T> taskResult;
      try {
        taskResult = future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetaException("Interrupted while waiting for fanout backend response");
      } catch (ExecutionException e) {
        Throwable cause = e.getCause() == null ? e : e.getCause();
        throw new MetaException("Fanout backend execution failed: " + cause.getMessage());
      }
      if (taskResult.error() != null) {
        handleFanoutFailure(methodName, taskResult.backend(), requestId, taskResult.error());
        continue;
      }
      results.add(new FanoutBackendResult<>(taskResult.backend(), taskResult.value()));
    }
    return results;
  }

  private <T> List<FanoutBackendResult<T>> invokeSequentialFanoutRead(
      String methodName,
      List<CatalogBackend> backends,
      ImpersonationContext impersonation,
      long requestId,
      FanoutBackendCall<T> call
  ) throws Throwable {
    List<FanoutBackendResult<T>> results = new ArrayList<>(backends.size());
    for (CatalogBackend backend : backends) {
      try {
        results.add(new FanoutBackendResult<>(backend, call.call(backend, impersonation, requestId)));
      } catch (Throwable error) {
        handleFanoutFailure(methodName, backend, requestId, error);
      }
    }
    return results;
  }

  private void handleFanoutFailure(
      String methodName,
      CatalogBackend backend,
      long requestId,
      Throwable error
  ) throws Throwable {
    if (!backendRoutingController.shouldDegradeSafeFanout(methodName, error)) {
      throw error;
    }
    RequestContext.currentObservation().markDegraded();
    LOG.warn("requestId={} omitting degraded backend catalog={} from safe fanout method={}",
        requestId, backend.name(), methodName, error);
  }

  private void logBackendRequest(
      long requestId,
      CatalogBackend backend,
      String methodName,
      ImpersonationContext impersonation,
      Object[] args
  ) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("requestId={} proxy-call catalog={} method={} impersonationUser={} args={}",
          requestId,
          backend.name(),
          methodName,
          impersonation == null ? "-" : impersonation.userName(),
          DebugLogUtil.formatArgs(args));
    }
    if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(methodName)) {
      LOG.info("requestId={} trace stage=backend-request catalog={} method={} impersonationUser={} summary={}",
          requestId,
          backend.name(),
          methodName,
          impersonation == null ? "-" : impersonation.userName(),
          WriteTraceUtil.summarizeArgs(args));
    }
  }

  private void logBackendResponse(
      long requestId,
      CatalogBackend backend,
      String methodName,
      long elapsedMs,
      Object result
  ) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("requestId={} proxy-response catalog={} method={} elapsedMs={} result={}",
          requestId, backend.name(), methodName, elapsedMs, DebugLogUtil.formatValue(result));
    }
    if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(methodName)) {
      LOG.info("requestId={} trace stage=backend-response catalog={} method={} elapsedMs={} summary={}",
          requestId,
          backend.name(),
          methodName,
          elapsedMs,
          WriteTraceUtil.summarizeResult(result));
    }
  }

  private void logBackendError(
      long requestId,
      CatalogBackend backend,
      String methodName,
      long elapsedMs,
      Throwable cause
  ) {
    if (LOG.isInfoEnabled() && WriteTraceUtil.shouldTrace(methodName)) {
      LOG.info("requestId={} trace stage=backend-error catalog={} method={} elapsedMs={} error={}",
          requestId,
          backend.name(),
          methodName,
          elapsedMs,
          cause.toString());
    }
    LOG.debug("requestId={} proxy-error catalog={} method={} elapsedMs={} error={}",
        requestId, backend.name(), methodName, elapsedMs, cause.toString(), cause);
  }

  private static MetaException backendUnavailableException(
      CatalogBackend backend,
      String methodName,
      ProxyRuntimeState.BackendCallAdmission admission
  ) {
    String reason = admission.rejectionReason() == null ? "backend unavailable" : admission.rejectionReason();
    String message = "Backend catalog '" + backend.name() + "' rejected method '" + methodName + "' because "
        + reason;
    if (admission.retryAtEpochMs() > 0L) {
      long retryInMs = Math.max(0L, admission.retryAtEpochMs() - System.currentTimeMillis());
      message += "; next retry window in " + retryInMs + "ms";
    }
    return new MetaException(message);
  }

  static Throwable normalizeBackendFailure(Method method, String backendName, Throwable cause) {
    if (!(cause instanceof TException) || isDeclaredMethodException(method, cause)) {
      return cause;
    }
    String message = "Backend catalog '" + backendName + "' failed in method '" + method.getName()
        + "' with " + cause.getClass().getSimpleName();
    if (cause.getMessage() != null && !cause.getMessage().isBlank()) {
      message += ": " + cause.getMessage();
    }
    MetaException metaException = new MetaException(message);
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

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  @FunctionalInterface
  interface BackendCall {
    Object call() throws Throwable;
  }

  @FunctionalInterface
  interface FanoutBackendCall<T> {
    T call(CatalogBackend backend, ImpersonationContext impersonation, long requestId) throws Throwable;
  }

  record FanoutBackendResult<T>(CatalogBackend backend, T value) {
  }

  private record FanoutTaskResult<T>(CatalogBackend backend, T value, Throwable error) {
    private static <T> FanoutTaskResult<T> success(CatalogBackend backend, T value) {
      return new FanoutTaskResult<>(backend, value, null);
    }

    private static <T> FanoutTaskResult<T> failure(CatalogBackend backend, Throwable error) {
      return new FanoutTaskResult<>(backend, null, error);
    }
  }
}
