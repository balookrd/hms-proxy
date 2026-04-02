package io.github.mmalykhin.hmsproxy.observability;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;

public final class ProxyRuntimeState {
  private final long startedAtEpochSecond;
  private final ConcurrentMap<String, BackendRuntimeStatus> backends = new ConcurrentHashMap<>();

  public ProxyRuntimeState(ProxyConfig config) {
    this.startedAtEpochSecond = Instant.now().getEpochSecond();
    long now = startedAtEpochSecond;
    for (String backendName : config.catalogNames()) {
      backends.put(backendName, new BackendRuntimeStatus(backendName, true, false, now, 0L, null, now));
    }
  }

  public long startedAtEpochSecond() {
    return startedAtEpochSecond;
  }

  public void recordBackendSuccess(String backend) {
    long now = Instant.now().getEpochSecond();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current).withProbe(true, false, now, null));
  }

  public void recordBackendProbeSuccess(String backend) {
    long now = Instant.now().getEpochSecond();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current).withProbe(true, false, now, null));
  }

  public void recordBackendFailure(String backend, Throwable error) {
    long now = Instant.now().getEpochSecond();
    backends.compute(backend, (ignored, current) -> {
      BackendRuntimeStatus status = statusOrDefault(backend, current).withFailure(now, summarize(error));
      if (isConnectivityFailure(error)) {
        return status.withProbe(false, true, now, summarize(error));
      }
      return status;
    });
  }

  public void recordBackendProbeFailure(String backend, Throwable error) {
    long now = Instant.now().getEpochSecond();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current).withFailure(now, summarize(error))
            .withProbe(false, true, now, summarize(error)));
  }

  public List<BackendRuntimeStatus> backendStatuses() {
    List<BackendRuntimeStatus> statuses = new ArrayList<>(backends.values());
    statuses.sort((left, right) -> left.backend().compareTo(right.backend()));
    return statuses;
  }

  private static BackendRuntimeStatus statusOrDefault(String backend, BackendRuntimeStatus current) {
    return current == null
        ? new BackendRuntimeStatus(backend, false, true, 0L, 0L, null, Instant.now().getEpochSecond())
        : current;
  }

  private static boolean isConnectivityFailure(Throwable error) {
    if (error == null) {
      return false;
    }
    return error instanceof TTransportException || error instanceof TApplicationException;
  }

  private static String summarize(Throwable error) {
    if (error == null) {
      return null;
    }
    String message = error.getMessage();
    return message == null || message.isBlank()
        ? error.getClass().getSimpleName()
        : error.getClass().getSimpleName() + ": " + message;
  }

  public record BackendRuntimeStatus(
      String backend,
      boolean connected,
      boolean degraded,
      long lastSuccessEpochSecond,
      long lastFailureEpochSecond,
      String lastError,
      long lastProbeEpochSecond
  ) {
    private BackendRuntimeStatus withFailure(long epochSecond, String error) {
      return new BackendRuntimeStatus(
          backend,
          connected,
          degraded,
          lastSuccessEpochSecond,
          epochSecond,
          error,
          lastProbeEpochSecond);
    }

    private BackendRuntimeStatus withProbe(boolean connected, boolean degraded, long epochSecond, String error) {
      return new BackendRuntimeStatus(
          backend,
          connected,
          degraded,
          connected ? epochSecond : lastSuccessEpochSecond,
          lastFailureEpochSecond,
          error == null ? lastError : error,
          epochSecond);
    }
  }
}
