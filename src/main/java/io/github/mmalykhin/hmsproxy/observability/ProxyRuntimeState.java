package io.github.mmalykhin.hmsproxy.observability;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.routing.TimeoutValueParser;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;

public final class ProxyRuntimeState {
  private static final String SOCKET_TIMEOUT_KEY = "hive.metastore.client.socket.timeout";

  private final long startedAtEpochSecond;
  private final ConcurrentMap<String, BackendRuntimeStatus> backends = new ConcurrentHashMap<>();

  public ProxyRuntimeState(ProxyConfig config) {
    this.startedAtEpochSecond = Instant.now().getEpochSecond();
    long now = System.currentTimeMillis();
    boolean pollingEnabled = config.latencyRouting().backendStatePolling().enabled();
    for (Map.Entry<String, ProxyConfig.CatalogConfig> entry : config.catalogs().entrySet()) {
      ProxyConfig.CatalogConfig catalog = entry.getValue();
      long baselineTimeoutMs = initialTimeoutMs(config, catalog);
      backends.put(entry.getKey(), new BackendRuntimeStatus(
          entry.getKey(),
          !pollingEnabled,
          pollingEnabled,
          pollingEnabled ? 0L : startedAtEpochSecond,
          0L,
          pollingEnabled ? "waiting_for_initial_probe" : null,
          0L,
          0L,
          0L,
          baselineTimeoutMs,
          baselineTimeoutMs,
          catalog.latencyBudgetMs(),
          CircuitState.CLOSED,
          0,
          0L,
          false));
    }
  }

  public long startedAtEpochSecond() {
    return startedAtEpochSecond;
  }

  public BackendCallAdmission admitBackendCall(String backend, ProxyConfig.LatencyRoutingConfig latencyRouting) {
    AtomicReference<BackendCallAdmission> admissionRef = new AtomicReference<>();
    long nowMs = System.currentTimeMillis();
    backends.compute(backend, (ignored, current) -> {
      BackendRuntimeStatus status = statusOrDefault(backend, current, latencyRouting);
      if (!latencyRouting.circuitBreaker().enabled()) {
        admissionRef.set(new BackendCallAdmission(true, false, status.adaptiveTimeoutMs(), null, 0L));
        return status;
      }

      if (status.circuitState() == CircuitState.OPEN && nowMs < status.circuitRetryAtEpochMs()) {
        admissionRef.set(new BackendCallAdmission(
            false,
            false,
            status.adaptiveTimeoutMs(),
            "circuit_open",
            status.circuitRetryAtEpochMs()));
        return status;
      }

      if (status.circuitState() == CircuitState.OPEN) {
        BackendRuntimeStatus halfOpen = status.toHalfOpen();
        admissionRef.set(new BackendCallAdmission(true, true, halfOpen.adaptiveTimeoutMs(), null, 0L));
        return halfOpen;
      }

      if (status.circuitState() == CircuitState.HALF_OPEN && status.halfOpenInFlight()) {
        admissionRef.set(new BackendCallAdmission(
            false,
            false,
            status.adaptiveTimeoutMs(),
            "half_open_probe_in_flight",
            status.circuitRetryAtEpochMs()));
        return status;
      }

      admissionRef.set(new BackendCallAdmission(true, false, status.adaptiveTimeoutMs(), null, 0L));
      return status;
    });
    return admissionRef.get();
  }

  public void recordBackendSuccess(
      String backend,
      long latencyMs,
      ProxyConfig.LatencyRoutingConfig latencyRouting
  ) {
    long nowEpochSecond = Instant.now().getEpochSecond();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current, latencyRouting).onSuccess(nowEpochSecond, latencyMs, latencyRouting, false));
  }

  public void recordBackendProbeSuccess(
      String backend,
      long latencyMs,
      ProxyConfig.LatencyRoutingConfig latencyRouting
  ) {
    long nowEpochSecond = Instant.now().getEpochSecond();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current, latencyRouting).onSuccess(nowEpochSecond, latencyMs, latencyRouting, true));
  }

  public void recordBackendFailure(
      String backend,
      Throwable error,
      long latencyMs,
      ProxyConfig.LatencyRoutingConfig latencyRouting
  ) {
    long nowEpochSecond = Instant.now().getEpochSecond();
    long nowMs = System.currentTimeMillis();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current, latencyRouting)
            .onFailure(nowEpochSecond, nowMs, latencyMs, summarize(error), error, latencyRouting, false));
  }

  public void recordBackendProbeFailure(
      String backend,
      Throwable error,
      ProxyConfig.LatencyRoutingConfig latencyRouting
  ) {
    long nowEpochSecond = Instant.now().getEpochSecond();
    long nowMs = System.currentTimeMillis();
    backends.compute(backend, (ignored, current) ->
        statusOrDefault(backend, current, latencyRouting)
            .onFailure(nowEpochSecond, nowMs, 0L, summarize(error), error, latencyRouting, true));
  }

  public BackendRuntimeStatus backendStatus(String backend) {
    return backends.get(backend);
  }

  public List<BackendRuntimeStatus> backendStatuses() {
    List<BackendRuntimeStatus> statuses = new ArrayList<>(backends.values());
    statuses.sort((left, right) -> left.backend().compareTo(right.backend()));
    return statuses;
  }

  private static BackendRuntimeStatus statusOrDefault(
      String backend,
      BackendRuntimeStatus current,
      ProxyConfig.LatencyRoutingConfig latencyRouting
  ) {
    return current == null
        ? new BackendRuntimeStatus(
            backend,
            false,
            true,
            0L,
            0L,
            "uninitialized",
            0L,
            0L,
            0L,
            latencyRouting.adaptiveTimeout().initialTimeoutMs(),
            latencyRouting.adaptiveTimeout().initialTimeoutMs(),
            0L,
            CircuitState.CLOSED,
            0,
            0L,
            false)
        : current;
  }

  private static long initialTimeoutMs(ProxyConfig config, ProxyConfig.CatalogConfig catalogConfig) {
    ProxyConfig.AdaptiveTimeoutConfig adaptiveTimeout = config.latencyRouting().adaptiveTimeout();
    long configuredTimeoutMs = TimeoutValueParser.parseDurationMs(
        catalogConfig.hiveConf().get(SOCKET_TIMEOUT_KEY),
        adaptiveTimeout.initialTimeoutMs());
    long baselineTimeoutMs = Math.max(configuredTimeoutMs, adaptiveTimeout.initialTimeoutMs());
    return clamp(
        baselineTimeoutMs,
        adaptiveTimeout.minTimeoutMs(),
        adaptiveTimeout.maxTimeoutMs());
  }

  private static long clamp(long value, long min, long max) {
    return Math.min(Math.max(value, min), max);
  }

  private static boolean isCircuitFailure(Throwable error) {
    return isConnectivityFailure(error) || isTimeoutFailure(error);
  }

  private static boolean isConnectivityFailure(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof TTransportException || current instanceof TApplicationException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean isTimeoutFailure(Throwable error) {
    Throwable current = error;
    while (current != null) {
      if (current instanceof java.net.SocketTimeoutException
          || current instanceof java.util.concurrent.TimeoutException) {
        return true;
      }
      String message = current.getMessage();
      if (message != null && message.toLowerCase().contains("timed out")) {
        return true;
      }
      current = current.getCause();
    }
    return false;
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

  public enum CircuitState {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  public record BackendCallAdmission(
      boolean allowed,
      boolean halfOpen,
      long timeoutMs,
      String rejectionReason,
      long retryAtEpochMs
  ) {
  }

  public record BackendRuntimeStatus(
      String backend,
      boolean connected,
      boolean degraded,
      long lastSuccessEpochSecond,
      long lastFailureEpochSecond,
      String lastError,
      long lastProbeEpochSecond,
      long lastLatencyMs,
      long latencyEwmaMs,
      long baselineTimeoutMs,
      long adaptiveTimeoutMs,
      long latencyBudgetMs,
      CircuitState circuitState,
      int consecutiveFailures,
      long circuitRetryAtEpochMs,
      boolean halfOpenInFlight
  ) {
    private BackendRuntimeStatus onSuccess(
        long nowEpochSecond,
        long latencyMs,
        ProxyConfig.LatencyRoutingConfig latencyRouting,
        boolean probe
    ) {
      long ewmaMs = latencyEwmaMs <= 0
          ? Math.max(latencyMs, 1L)
          : Math.max(1L, Math.round(
              latencyEwmaMs * (1.0d - latencyRouting.adaptiveTimeout().alpha())
                  + latencyMs * latencyRouting.adaptiveTimeout().alpha()));
      long nextAdaptiveTimeoutMs = latencyRouting.adaptiveTimeout().enabled()
          ? clamp(
              Math.max(
                  baselineTimeoutMs,
                  Math.round(ewmaMs * latencyRouting.adaptiveTimeout().multiplier()) + latencyBudgetMs),
              latencyRouting.adaptiveTimeout().minTimeoutMs(),
              latencyRouting.adaptiveTimeout().maxTimeoutMs())
          : adaptiveTimeoutMs;
      return new BackendRuntimeStatus(
          backend,
          true,
          false,
          nowEpochSecond,
          lastFailureEpochSecond,
          null,
          probe ? nowEpochSecond : lastProbeEpochSecond,
          latencyMs,
          ewmaMs,
          baselineTimeoutMs,
          nextAdaptiveTimeoutMs,
          latencyBudgetMs,
          CircuitState.CLOSED,
          0,
          0L,
          false);
    }

    private BackendRuntimeStatus onFailure(
        long nowEpochSecond,
        long nowMs,
        long latencyMs,
        String errorSummary,
        Throwable error,
        ProxyConfig.LatencyRoutingConfig latencyRouting,
        boolean probe
    ) {
      boolean connectivityFailure = isConnectivityFailure(error) || isTimeoutFailure(error);
      boolean circuitFailure = latencyRouting.circuitBreaker().enabled() && isCircuitFailure(error);
      int nextConsecutiveFailures =
          circuitFailure ? consecutiveFailures + 1 : consecutiveFailures;
      CircuitState nextCircuitState = circuitState;
      long nextRetryAtMs = circuitRetryAtEpochMs;
      boolean nextHalfOpenInFlight = false;
      if (circuitFailure
          && (circuitState == CircuitState.HALF_OPEN
              || nextConsecutiveFailures >= latencyRouting.circuitBreaker().failureThreshold())) {
        nextCircuitState = CircuitState.OPEN;
        nextRetryAtMs = nowMs + latencyRouting.circuitBreaker().openStateMs();
      }
      long nextAdaptiveTimeoutMs = adaptiveTimeoutMs;
      if (latencyRouting.adaptiveTimeout().enabled() && isTimeoutFailure(error)) {
        nextAdaptiveTimeoutMs = clamp(
            Math.max(adaptiveTimeoutMs * 2L, baselineTimeoutMs + latencyBudgetMs),
            latencyRouting.adaptiveTimeout().minTimeoutMs(),
            latencyRouting.adaptiveTimeout().maxTimeoutMs());
      }
      return new BackendRuntimeStatus(
          backend,
          !connectivityFailure,
          degraded || connectivityFailure || nextCircuitState != CircuitState.CLOSED,
          lastSuccessEpochSecond,
          nowEpochSecond,
          errorSummary,
          probe ? nowEpochSecond : lastProbeEpochSecond,
          latencyMs,
          latencyEwmaMs,
          baselineTimeoutMs,
          nextAdaptiveTimeoutMs,
          latencyBudgetMs,
          nextCircuitState,
          nextConsecutiveFailures,
          nextRetryAtMs,
          nextHalfOpenInFlight);
    }

    private BackendRuntimeStatus toHalfOpen() {
      return new BackendRuntimeStatus(
          backend,
          connected,
          true,
          lastSuccessEpochSecond,
          lastFailureEpochSecond,
          lastError,
          lastProbeEpochSecond,
          lastLatencyMs,
          latencyEwmaMs,
          baselineTimeoutMs,
          adaptiveTimeoutMs,
          latencyBudgetMs,
          CircuitState.HALF_OPEN,
          consecutiveFailures,
          circuitRetryAtEpochMs,
          true);
    }
  }
}
