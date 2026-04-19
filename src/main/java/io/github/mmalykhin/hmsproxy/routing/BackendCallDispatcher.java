package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import io.github.mmalykhin.hmsproxy.util.DebugLogUtil;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes invocations through admission control, compatibility fallback, and fanout.
 * All cross-cutting concerns (circuit breaking, rate limiting, error normalisation,
 * fanout scheduling) are delegated to dedicated collaborators.
 */
final class BackendCallDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(BackendCallDispatcher.class);

  private final CompatibilityLayer compatibilityLayer;
  private final AdmissionGate admissionGate;
  private final ProxyObservability observability;
  private final FanoutExecutor fanoutExecutor;

  BackendCallDispatcher(
      CompatibilityLayer compatibilityLayer,
      AdmissionGate admissionGate,
      ProxyObservability observability,
      FanoutExecutor fanoutExecutor
  ) {
    this.compatibilityLayer = compatibilityLayer;
    this.admissionGate = admissionGate;
    this.observability = observability;
    this.fanoutExecutor = fanoutExecutor;
  }

  Object invokeDirect(
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

  Object invokeViaRequest(
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

  Object invokeByReflection(
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

  <T> List<FanoutExecutor.FanoutBackendResult<T>> invokeFanoutRead(
      String methodName,
      FanoutExecutor.FanoutBackendCall<T> call,
      ImpersonationContext impersonation,
      long requestId
  ) throws Throwable {
    return fanoutExecutor.execute(methodName, call, impersonation, requestId);
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
      admissionGate.enforceRateLimit(methodName, backend.name());
    }
    if (recordObservation) {
      RequestContext.currentObservation().recordBackend(backend.name());
    }

    ProxyRuntimeState.BackendCallAdmission admission = admissionGate.admit(backend);
    if (!admission.allowed()) {
      return maybeCompatibilityFallback(
          backend,
          methodName,
          requestId,
          allowCompatibilityFallback,
          AdmissionGate.unavailableException(backend, methodName, admission),
          declaredMethod);
    }

    try {
      logBackendRequest(requestId, backend, methodName, impersonation, args);
      Object result = call.call();
      long elapsedMs = elapsedMillis(startedAt);
      logBackendResponse(requestId, backend, methodName, elapsedMs, result);
      admissionGate.recordSuccess(backend, elapsedMs);
      return result;
    } catch (Throwable cause) {
      long elapsedMs = elapsedMillis(startedAt);
      observability.metrics().recordBackendFailure(backend.name(), cause);
      admissionGate.recordFailure(backend, cause, elapsedMs);
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
        throw BackendErrorNormalizer.normalize(declaredMethod, backend.name(), cause);
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
      throw BackendErrorNormalizer.normalize(declaredMethod, backend.name(), cause);
    }
    throw cause;
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

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
  }

  @FunctionalInterface
  interface BackendCall {
    Object call() throws Throwable;
  }
}
