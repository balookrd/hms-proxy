package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.backend.ImpersonationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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

    long timeoutMs = backendRoutingController.fanoutTimeoutMs();
    long deadlineNs = System.nanoTime() + timeoutMs * 1_000_000L;
    List<FanoutBackendResult<T>> results = new ArrayList<>(backends.size());
    for (int i = 0; i < futures.size(); i++) {
      Future<FanoutTaskResult<T>> future = futures.get(i);
      FanoutTaskResult<T> taskResult;
      try {
        long remainingNs = deadlineNs - System.nanoTime();
        taskResult = future.get(Math.max(0, remainingNs), TimeUnit.NANOSECONDS);
      } catch (TimeoutException e) {
        MetaException timeoutError = new MetaException("Fanout backend timed out after " + timeoutMs + " ms");
        for (int j = i; j < futures.size(); j++) {
          futures.get(j).cancel(true);
          handleFanoutFailure(methodName, backends.get(j), requestId, timeoutError);
        }
        break;
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

  private record FanoutTaskResult<T>(CatalogBackend backend, T value, Throwable error) {
    private static <T> FanoutTaskResult<T> success(CatalogBackend backend, T value) {
      return new FanoutTaskResult<>(backend, value, null);
    }

    private static <T> FanoutTaskResult<T> failure(CatalogBackend backend, Throwable error) {
      return new FanoutTaskResult<>(backend, null, error);
    }
  }
}
