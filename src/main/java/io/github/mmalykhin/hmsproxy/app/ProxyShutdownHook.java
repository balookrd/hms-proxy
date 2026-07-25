package io.github.mmalykhin.hmsproxy.app;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Body of the JVM shutdown hook.
 *
 * <p>Stopping the primary listener only unblocks {@code serve()} on the main thread. The
 * additional frontend listeners, the management listener, the router backends and the front-door
 * security are closed by the main thread <em>after</em> that, and the JVM halts as soon as the
 * last shutdown hook returns. A hook that only stops the primary listener therefore lets the JVM
 * halt while in-flight requests on the additional frontends are still running and backend
 * resources are still open, so the hook waits for the main thread to finish its ordered teardown.
 * The wait is bounded so a stuck teardown cannot hang the process forever.
 */
final class ProxyShutdownHook implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ProxyShutdownHook.class);

  private final Runnable stopPrimaryListener;
  private final CountDownLatch teardownComplete;
  private final long timeoutMillis;

  ProxyShutdownHook(Runnable stopPrimaryListener, CountDownLatch teardownComplete, long timeoutMillis) {
    this.stopPrimaryListener = stopPrimaryListener;
    this.teardownComplete = teardownComplete;
    this.timeoutMillis = timeoutMillis;
  }

  @Override
  public void run() {
    LOG.info("Shutdown requested, stopping HMS proxy");
    try {
      stopPrimaryListener.run();
    } catch (RuntimeException e) {
      // Still wait below: the main thread may be past serve() and mid-teardown already.
      LOG.warn("Failed to stop the primary listener cleanly", e);
    }
    try {
      if (teardownComplete.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
        LOG.info("HMS proxy stopped");
      } else {
        LOG.warn("HMS proxy teardown did not finish within {}ms, letting the JVM halt anyway",
            timeoutMillis);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting for HMS proxy teardown to finish");
    }
  }
}
