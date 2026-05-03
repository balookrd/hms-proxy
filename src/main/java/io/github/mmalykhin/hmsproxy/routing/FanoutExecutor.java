package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class FanoutExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(FanoutExecutor.class);

  private final BackendRoutingController backendRoutingController;
  private final CatalogRouter router;
  private final AdmissionGate admissionGate;

  FanoutExecutor(
      BackendRoutingController backendRoutingController,
      CatalogRouter router,
      AdmissionGate admissionGate
  ) {
    this.backendRoutingController = backendRoutingController;
    this.router = router;
    this.admissionGate = admissionGate;
  }

  <T> List<FanoutBackendResult<T>> execute(
      String methodName,
      FanoutBackendCall<T> call,
      ImpersonationContext impersonation,
      long requestId
  ) throws Throwable {
    List<CatalogBackend> backends = new ArrayList<>(router.backends());
    for (CatalogBackend backend : backends) {
      admissionGate.enforceRateLimit(methodName, backend.name());
    }
    return backendRoutingController.hedgedReadEnabled(methodName)
        ? executeParallel(methodName, backends, impersonation, requestId, call)
        : executeSequential(methodName, backends, impersonation, requestId, call);
  }

  private <T> List<FanoutBackendResult<T>> executeParallel(
      String methodName,
      List<CatalogBackend> backends,
      ImpersonationContext impersonation,
      long requestId,
      FanoutBackendCall<T> call
  ) throws Throwable {
    RequestObservation parentObservation = RequestContext.REQUEST_OBSERVATION.get();
    String observationMethod = parentObservation != null ? parentObservation.method() : methodName;
    List<CompletableFuture<FanoutTaskResult<T>>> futures = new ArrayList<>(backends.size());
    for (CatalogBackend backend : backends) {
      futures.add(CompletableFuture.supplyAsync(() -> {
        RequestObservation workerObservation = new RequestObservation(observationMethod);
        RequestContext.REQUEST_OBSERVATION.set(workerObservation);
        try {
          T value = call.call(backend, impersonation, requestId);
          return FanoutTaskResult.success(backend, value, workerObservation.fallback());
        } catch (Throwable error) {
          return FanoutTaskResult.failure(backend, error, workerObservation.fallback());
        } finally {
          RequestContext.REQUEST_OBSERVATION.remove();
        }
      }, backendRoutingController.fanoutExecutor()));
    }

    long timeoutMs = backendRoutingController.fanoutTimeoutMs();
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
          .get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException ignored) {
      // fall through and harvest whatever completed within the deadline
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new MetaException("Interrupted while waiting for fanout backend response");
    } catch (ExecutionException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      throw new MetaException("Fanout backend execution failed: " + cause.getMessage());
    }

    MetaException timeoutError = new MetaException("Fanout backend timed out after " + timeoutMs + " ms");
    List<FanoutBackendResult<T>> results = new ArrayList<>(backends.size());
    for (int i = 0; i < futures.size(); i++) {
      CompletableFuture<FanoutTaskResult<T>> future = futures.get(i);
      if (future.isDone()) {
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
        if (taskResult.fallback() && parentObservation != null) {
          parentObservation.markFallback();
        }
        if (taskResult.error() != null) {
          handleFanoutFailure(methodName, taskResult.backend(), requestId, taskResult.error());
        } else {
          results.add(new FanoutBackendResult<>(taskResult.backend(), taskResult.value()));
        }
      } else {
        future.cancel(true);
        handleFanoutFailure(methodName, backends.get(i), requestId, timeoutError);
      }
    }
    return results;
  }

  private <T> List<FanoutBackendResult<T>> executeSequential(
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

  @FunctionalInterface
  interface FanoutBackendCall<T> {
    T call(CatalogBackend backend, ImpersonationContext impersonation, long requestId) throws Throwable;
  }

  record FanoutBackendResult<T>(CatalogBackend backend, T value) {
  }

  private record FanoutTaskResult<T>(CatalogBackend backend, T value, Throwable error, boolean fallback) {
    private static <T> FanoutTaskResult<T> success(CatalogBackend backend, T value, boolean fallback) {
      return new FanoutTaskResult<>(backend, value, null, fallback);
    }

    private static <T> FanoutTaskResult<T> failure(CatalogBackend backend, Throwable error, boolean fallback) {
      return new FanoutTaskResult<>(backend, null, error, fallback);
    }
  }
}
