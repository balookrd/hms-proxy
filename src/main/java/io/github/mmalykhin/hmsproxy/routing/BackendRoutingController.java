package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.observability.ProxyObservability;
import io.github.mmalykhin.hmsproxy.observability.ProxyRuntimeState;
import java.util.Set;
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

public final class BackendRoutingController implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(BackendRoutingController.class);
  private static final Set<String> SAFE_FANOUT_METHODS = Set.of(
      "get_all_databases",
      "get_databases",
      "get_table_meta");

  private final ProxyConfig config;
  private final CatalogRouter router;
  private final ProxyObservability observability;
  private final ScheduledExecutorService pollingExecutor;
  private final ExecutorService fanoutExecutor;

  public BackendRoutingController(ProxyConfig config, CatalogRouter router, ProxyObservability observability) {
    this.config = config;
    this.router = router;
    this.observability = observability;
    this.pollingExecutor = config.latencyRouting().backendStatePolling().enabled()
        ? Executors.newSingleThreadScheduledExecutor(namedThreadFactory("hms-proxy-backend-poll"))
        : null;
    if (config.latencyRouting().hedgedRead().enabled() && router.backends().size() > 1) {
      int poolSize = Math.min(config.latencyRouting().hedgedRead().maxParallelism(), router.backends().size());
      // Bound the queue to the number of backends: each request submits at most backends.size() tasks.
      // CallerRunsPolicy provides back-pressure when all worker slots are occupied.
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
      backend.ensureClientSocketTimeout(admission.timeoutMs());
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
    return fanoutExecutor != null && SAFE_FANOUT_METHODS.contains(methodName);
  }

  public ExecutorService fanoutExecutor() {
    return fanoutExecutor;
  }

  public boolean shouldDegradeSafeFanout(String methodName, Throwable error) {
    if (!SAFE_FANOUT_METHODS.contains(methodName)
        || config.latencyRouting().degradedRoutingPolicy() != ProxyConfig.DegradedRoutingPolicy.SAFE_FANOUT_READS) {
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
    for (CatalogBackend backend : router.backends()) {
      try {
        ProxyRuntimeState.BackendRuntimeStatus status = observability.runtimeState().backendStatus(backend.name());
        if (status != null && config.latencyRouting().adaptiveTimeout().enabled()) {
          backend.ensureClientSocketTimeout(status.adaptiveTimeoutMs());
        }
        long startedAt = System.nanoTime();
        backend.checkConnectivity();
        observability.runtimeState().recordBackendProbeSuccess(
            backend.name(),
            elapsedMillis(startedAt),
            config.latencyRouting());
      } catch (Throwable error) {
        LOG.warn("Backend catalog '{}' health poll failed", backend.name(), error);
        observability.runtimeState().recordBackendProbeFailure(backend.name(), error, config.latencyRouting());
      }
    }
  }

  private static long elapsedMillis(long startedAt) {
    return (System.nanoTime() - startedAt) / 1_000_000L;
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
