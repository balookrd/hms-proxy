package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import io.github.mmalykhin.hmsproxy.security.ClientRequestContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
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
    Long parentRequestId = RequestContext.REQUEST_ID.get();
    long effectiveWorkerRequestId = parentRequestId != null ? parentRequestId : requestId;
    ClientRequestContext.Snapshot parentClientContext = ClientRequestContext.capture();
    String observationMethod = parentObservation != null ? parentObservation.method() : methodName;
    long timeoutMs = backendRoutingController.fanoutTimeoutMs();
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    List<SubmittedTask<T>> submitted = new ArrayList<>(backends.size());
    MetaException timeoutError = new MetaException("Fanout backend timed out after " + timeoutMs + " ms");
    List<FanoutBackendResult<T>> results = new ArrayList<>(backends.size());
    try {
      for (CatalogBackend backend : backends) {
        try {
          Future<FanoutTaskResult<T>> future = backendRoutingController.fanoutExecutor().submit(() -> {
            RequestObservation previousObservation = RequestContext.REQUEST_OBSERVATION.get();
            Long previousRequestId = RequestContext.REQUEST_ID.get();
            ClientRequestContext.Snapshot previousClientContext = ClientRequestContext.install(parentClientContext);
            RequestObservation workerObservation = new RequestObservation(observationMethod);
            RequestContext.REQUEST_OBSERVATION.set(workerObservation);
            RequestContext.REQUEST_ID.set(effectiveWorkerRequestId);
            try {
              T value = call.call(backend, impersonation, requestId);
              return FanoutTaskResult.success(backend, value, workerObservation.fallback());
            } catch (Throwable error) {
              return FanoutTaskResult.failure(backend, error, workerObservation.fallback());
            } finally {
              ClientRequestContext.restore(previousClientContext);
              if (previousRequestId == null) {
                RequestContext.REQUEST_ID.remove();
              } else {
                RequestContext.REQUEST_ID.set(previousRequestId);
              }
              if (previousObservation == null) {
                RequestContext.REQUEST_OBSERVATION.remove();
              } else {
                RequestContext.REQUEST_OBSERVATION.set(previousObservation);
              }
            }
          });
          submitted.add(new SubmittedTask<>(backend, future));
        } catch (RejectedExecutionException rejected) {
          MetaException overloadError = new MetaException(
              "Fanout executor overloaded for method " + methodName + " and backend " + backend.name());
          handleFanoutFailure(methodName, backend, requestId, overloadError);
        }
      }

      for (SubmittedTask<T> task : submitted) {
        Future<FanoutTaskResult<T>> future = task.future();
        FanoutTaskResult<T> taskResult = null;
        try {
          long remainingNanos = deadlineNanos - System.nanoTime();
          if (remainingNanos > 0L) {
            taskResult = future.get(remainingNanos, TimeUnit.NANOSECONDS);
          } else if (future.isDone()) {
            // Past the deadline, but this backend already answered: keep its result.
            taskResult = future.get();
          }
        } catch (TimeoutException ignored) {
          // handled below as a degraded backend
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new MetaException("Interrupted while waiting for fanout backend response");
        } catch (ExecutionException e) {
          Throwable cause = e.getCause() == null ? e : e.getCause();
          throw new MetaException("Fanout backend execution failed: " + cause.getMessage());
        }
        if (taskResult == null) {
          future.cancel(true);
          handleFanoutFailure(methodName, task.backend(), requestId, timeoutError);
          continue;
        }
        if (taskResult.fallback() && parentObservation != null) {
          parentObservation.markFallback();
        }
        if (taskResult.error() != null) {
          handleFanoutFailure(methodName, taskResult.backend(), requestId, taskResult.error());
        } else {
          results.add(new FanoutBackendResult<>(taskResult.backend(), taskResult.value()));
        }
      }
    } finally {
      // Nothing may outlive this request: cancel whatever is still pending, including tasks left
      // behind when a strict-policy failure or queue rejection aborts the request.
      for (SubmittedTask<T> pending : submitted) {
        pending.future().cancel(true);
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

  private record SubmittedTask<T>(CatalogBackend backend, Future<FanoutTaskResult<T>> future) {
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
