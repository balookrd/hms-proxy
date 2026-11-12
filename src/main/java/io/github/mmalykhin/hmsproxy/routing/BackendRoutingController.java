package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.operation.HmsOperationPolicy;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy;

public final class BackendRoutingController implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BackendRoutingController.class);

  private final ProxyConfig config;
  private final CatalogRouter router;
  private final ProxyObservability observability;
  private final ScheduledExecutorService pollingExecutor;
  private final ExecutorService probeExecutor;
  private final BackendProbeRunner probeRunner;
  private final ExecutorService fanoutExecutor;

  public BackendRoutingController(ProxyConfig config, CatalogRouter router, ProxyObservability observability) {
    this(config, router, observability, CatalogBackend::probeConnectivity);
  }

  BackendRoutingController(
      ProxyConfig config,
      CatalogRouter router,
      ProxyObservability observability,
      BackendProbeRunner.BackendProbe backendProbe
  ) {
    this.config = config;
    this.router = router;
    this.observability = observability;
    boolean pollingEnabled = config.latencyRouting().backendStatePolling().enabled();
    this.pollingExecutor = pollingEnabled
        ? Executors.newSingleThreadScheduledExecutor(namedThreadFactory("hms-proxy-backend-poll"))
        : null;
    if (pollingEnabled) {
      int probePoolSize = Math.max(1, Math.min(
          config.latencyRouting().backendStatePolling().maxParallelism(),
          Math.max(1, router.backends().size())));
      this.probeExecutor = new ThreadPoolExecutor(
          probePoolSize,
          probePoolSize,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(Math.max(1, router.backends().size())),
          namedThreadFactory("hms-proxy-backend-probe"),
          new ThreadPoolExecutor.CallerRunsPolicy());
      this.probeRunner = new BackendProbeRunner(probeExecutor, backendProbe);
    } else {
      this.probeExecutor = null;
      this.probeRunner = null;
    }
    if (router.backends().size() > 1) {
      int maxParallelism = config.latencyRouting().hedgedRead().maxParallelism();
      int poolSize = Math.max(1, Math.min(maxParallelism > 0 ? maxParallelism : 8, router.backends().size()));
      // Bound the queue to the number of backends: a single request submits at most backends.size()
      // tasks. The pool is shared, so concurrent fanout requests can still exhaust it; CallerRunsPolicy
      // then applies back-pressure by running a task on the request thread, and FanoutExecutor counts
      // that time against the fanout deadline.
      this.fanoutExecutor = new ThreadPoolExecutor(
          poolSize,
          poolSize,
          0L,
          TimeUnit.MILLISECONDS,
          new ArrayBlockingQueue<>(router.backends().size()),
          namedThreadFactory("hms-proxy-fanout"),
          new ThreadPoolExecutor.CallerRunsPolicy());
    } else {
      this.fanoutExecutor = null;
    }
    startPollingIfEnabled();
  }

  public ProxyRuntimeState.BackendCallAdmission admit(CatalogBackend backend) throws MetaException {
    ProxyRuntimeState.BackendCallAdmission admission =
        observability.runtimeState().admitBackendCall(backend.name(), config.latencyRouting());
    if (admission.allowed() && config.latencyRouting().adaptiveTimeout().enabled()) {
      // An admitted call may already have claimed the half-open circuit probe. The adaptive
      // reconnect below talks to a backend that is likely still down, so any failure here must be
      // recorded as a backend failure - otherwise the probe stays in flight and the circuit never
      // leaves HALF_OPEN, permanently rejecting the catalog.
      CatalogBackend.AdaptiveTimeoutResult result;
      try {
        result = backend.ensureClientSocketTimeout(
            admission.timeoutMs(),
            config.latencyRouting().adaptiveTimeout().reconnectCooldownMs());
      } catch (Throwable cause) {
        observability.metrics().recordBackendFailure(backend.name(), cause);
        recordFailure(backend, cause, 0L);
        throw cause;
      }
      switch (result) {
        case APPLIED -> observability.metrics().recordAdaptiveTimeoutReconnect(backend.name());
        case SKIPPED_HYSTERESIS ->
            observability.metrics().recordAdaptiveTimeoutReconnectSkipped(backend.name(), "hysteresis");
        case SKIPPED_COOLDOWN ->
            observability.metrics().recordAdaptiveTimeoutReconnectSkipped(backend.name(), "cooldown");
        case UNCHANGED -> { }
      }
    }
    return admission;
  }

  public void recordSuccess(CatalogBackend backend, long latencyMs) {
    observability.runtimeState().recordBackendSuccess(backend.name(), latencyMs, config.latencyRouting());
  }

  public void recordFailure(CatalogBackend backend, Throwable error, long latencyMs) {
    observability.runtimeState().recordBackendFailure(backend.name(), error, latencyMs, config.latencyRouting());
  }

  public boolean hedgedReadEnabled(String methodName) {
    return fanoutExecutor != null && HmsOperationPolicy.describe(methodName).safeFanout();
  }

  public ExecutorService fanoutExecutor() {
    return fanoutExecutor;
  }

  public long fanoutTimeoutMs() {
    return config.latencyRouting().hedgedRead().fanoutTimeoutMs();
  }

  public boolean shouldDegradeSafeFanout(String methodName, Throwable error) {
    if (!HmsOperationPolicy.describe(methodName).safeFanout()
        || config.latencyRouting().degradedRoutingPolicy() != DegradedRoutingPolicy.SAFE_FANOUT_READS) {
      return false;
    }
    return error instanceof MetaException
        || error instanceof TTransportException
        || error instanceof TApplicationException;
  }

  @Override
  public void close() {
    if (pollingExecutor != null) {
      pollingExecutor.shutdownNow();
      awaitTerminationQuietly(pollingExecutor, "backend-poll");
    }
    if (probeExecutor != null) {
      probeExecutor.shutdownNow();
      awaitTerminationQuietly(probeExecutor, "backend-probe");
    }
    if (fanoutExecutor != null) {
      fanoutExecutor.shutdownNow();
      awaitTerminationQuietly(fanoutExecutor, "fanout");
    }
  }

  private static void awaitTerminationQuietly(ExecutorService executor, String name) {
    try {
      if (!executor.awaitTermination(5L, TimeUnit.SECONDS)) {
        LOG.warn("Executor '{}' did not terminate within 5s after shutdown", name);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void startPollingIfEnabled() {
    if (pollingExecutor == null) {
      return;
    }
    long intervalMs = config.latencyRouting().backendStatePolling().intervalMs();
    pollingExecutor.scheduleWithFixedDelay(this::runPollCycle, 0L, intervalMs, TimeUnit.MILLISECONDS);
  }

  private void runPollCycle() {
    probeRunner.runRound(
        List.copyOf(router.backends()),
        config.latencyRouting().backendStatePolling().probeTimeoutMs(),
        new BackendProbeRunner.ProbeListener() {
          @Override
          public void succeeded(CatalogBackend backend, long latencyMs) {
            observability.runtimeState().recordBackendProbeSuccess(
                backend.name(), latencyMs, config.latencyRouting());
          }

          @Override
          public void failed(CatalogBackend backend, Throwable error) {
            recordPollFailure(backend, error);
          }
        });
  }

  private void recordPollFailure(CatalogBackend backend, Throwable error) {
    LOG.warn("Backend catalog '{}' health poll failed", backend.name(), error);
    observability.runtimeState().recordBackendProbeFailure(backend.name(), error, config.latencyRouting());
  }

  private static ThreadFactory namedThreadFactory(String prefix) {
    return runnable -> {
      Thread thread = new Thread(runnable);
      thread.setName(prefix + "-" + thread.getId());
      thread.setDaemon(true);
      return thread;
    };
  }
}
