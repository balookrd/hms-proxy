package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One round of backend liveness probes: every backend is probed on the given pool and each result
 * is reported to a listener. The single implementation behind both background state polling
 * ({@link BackendRoutingController}) and the on-demand {@code /readyz} probes, so a fix to how a
 * round treats a hung backend reaches both paths.
 */
public final class BackendProbeRunner {

  @FunctionalInterface
  public interface BackendProbe {
    void probe(CatalogBackend backend, long timeoutMs) throws Throwable;
  }

  public interface ProbeListener {
    void succeeded(CatalogBackend backend, long latencyMs);

    void failed(CatalogBackend backend, Throwable error);
  }

  private final ExecutorService executor;
  private final BackendProbe backendProbe;
  // Guarded by this: rounds of one runner never overlap.
  private final Map<String, RunningProbe> runningProbes = new HashMap<>();

  public BackendProbeRunner(ExecutorService executor, BackendProbe backendProbe) {
    this.executor = executor;
    this.backendProbe = backendProbe;
  }

  /**
   * Probes every backend and reports each result before returning. Returns early, with the
   * interrupt flag set, when the calling thread is interrupted.
   */
  public synchronized void runRound(List<CatalogBackend> backends, long timeoutMs, ProbeListener listener) {
    List<ProbeInFlight> inFlight = new ArrayList<>(backends.size());
    for (CatalogBackend backend : backends) {
      // A cancelled probe keeps its thread while the metastore client retries (it does not stop on
      // interrupt). Probing the same backend again would take a second thread, and once the pool is
      // full the probes of healthy backends wait in the queue and miss the deadline. A backend whose
      // previous probe has not returned by the next round is unresponsive anyway.
      RunningProbe previous = runningProbes.get(backend.name());
      if (previous != null && previous.occupiesThread()) {
        listener.failed(backend, new TimeoutException("previous probe still running"));
        continue;
      }
      long startedAt = System.nanoTime();
      AtomicBoolean started = new AtomicBoolean();
      AtomicBoolean finished = new AtomicBoolean();
      Future<?> probe = executor.submit((Callable<Void>) () -> {
        started.set(true);
        try {
          backendProbe.probe(backend, timeoutMs);
        } catch (Exception e) {
          throw e;
        } catch (Throwable t) {
          throw new RuntimeException(t);
        } finally {
          finished.set(true);
        }
        return null;
      });
      runningProbes.put(backend.name(), new RunningProbe(probe, started, finished));
      inFlight.add(new ProbeInFlight(backend, startedAt, probe));
    }
    long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    for (ProbeInFlight entry : inFlight) {
      try {
        // Probes share one deadline but are awaited in turn, so a probe that already returned while
        // an earlier one used up the deadline still counts by its own result.
        long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        if (remainingNanos == 0L && !entry.probe.isDone()) {
          entry.probe.cancel(true);
          throw new TimeoutException("probe deadline exceeded");
        }
        entry.probe.get(remainingNanos, TimeUnit.NANOSECONDS);
        listener.succeeded(entry.backend, (System.nanoTime() - entry.startedAt) / 1_000_000L);
      } catch (TimeoutException e) {
        entry.probe.cancel(true);
        listener.failed(entry.backend, e);
      } catch (ExecutionException e) {
        listener.failed(entry.backend, e.getCause() != null ? e.getCause() : e);
      } catch (InterruptedException e) {
        entry.probe.cancel(true);
        Thread.currentThread().interrupt();
        listener.failed(entry.backend, e);
        return;
      } catch (Throwable t) {
        listener.failed(entry.backend, t);
      }
    }
  }

  // Future.isDone() turns true as soon as the probe is cancelled, while its thread may still be
  // inside the metastore client; only the task itself knows when it has returned.
  private record RunningProbe(Future<?> future, AtomicBoolean started, AtomicBoolean finished) {
    boolean occupiesThread() {
      if (finished.get()) {
        return false;
      }
      // A probe cancelled while still queued never runs.
      return started.get() || !future.isCancelled();
    }
  }

  private record ProbeInFlight(CatalogBackend backend, long startedAt, Future<?> probe) {
  }
}
