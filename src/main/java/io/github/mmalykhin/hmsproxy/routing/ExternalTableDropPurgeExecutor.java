package io.github.mmalykhin.hmsproxy.routing;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs best-effort external-table purges off the Thrift worker thread. A recursive delete of a
 * large table location takes minutes on S3/HDFS, and doing it inline keeps a worker from the
 * bounded server pool busy for that whole time, which shows up as client timeouts under load.
 *
 * <p>The queue is bounded on purpose: a purge backlog means the storage layer cannot keep up, and
 * dropping the excess with a warning is preferable to growing an unbounded queue of deletes.
 */
final class ExternalTableDropPurgeExecutor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalTableDropPurgeExecutor.class);
  private static final int WORKER_THREADS = 2;
  private static final int QUEUE_CAPACITY = 512;
  private static final long IDLE_TIMEOUT_SECONDS = 60L;
  private static final long SHUTDOWN_WAIT_SECONDS = 60L;

  private final ThreadPoolExecutor executor;

  ExternalTableDropPurgeExecutor() {
    this.executor = new ThreadPoolExecutor(
        WORKER_THREADS,
        WORKER_THREADS,
        IDLE_TIMEOUT_SECONDS,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(QUEUE_CAPACITY),
        namedThreadFactory("hms-proxy-drop-purge-"));
    // No purge is in flight most of the time, so let idle workers go away instead of parking them.
    executor.allowCoreThreadTimeOut(true);
  }

  void submit(String catalogName, String location, Runnable purge) {
    try {
      executor.execute(purge);
    } catch (RejectedExecutionException e) {
      LOG.warn(
          "requestId={} skipping external-table purge for catalog '{}' at location '{}' because the"
              + " background purge executor rejected the task (queue full or proxy shutting down)",
          RequestContext.currentRequestId(), catalogName, location);
    }
  }

  @Override
  public void close() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
        int abandoned = executor.shutdownNow().size();
        LOG.warn("External-table purge executor did not drain within {}s, {} queued purges abandoned",
            SHUTDOWN_WAIT_SECONDS, abandoned);
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory namedThreadFactory(String prefix) {
    AtomicLong counter = new AtomicLong();
    return runnable -> {
      Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    };
  }
}
