package io.github.mmalykhin.hmsproxy.app;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class ProxyShutdownHookTest {

  @Test
  public void stopsPrimaryListenerBeforeWaitingForTheOrderedTeardown() throws Exception {
    List<String> order = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch teardownComplete = new CountDownLatch(1);
    CountDownLatch listenerStopped = new CountDownLatch(1);
    Thread hook = runHook(new ProxyShutdownHook(
        () -> {
          order.add("stop-primary-listener");
          listenerStopped.countDown();
        },
        teardownComplete,
        TimeUnit.SECONDS.toMillis(10)), "ordered-teardown");

    Assert.assertTrue("hook must stop the primary listener to unblock serve()",
        listenerStopped.await(5, TimeUnit.SECONDS));
    Thread.sleep(200L);
    Assert.assertTrue("hook must still be waiting for the main thread to finish teardown",
        hook.isAlive());

    // Stands in for the main thread closing extras -> handler -> management -> router -> security.
    order.add("teardown-complete");
    teardownComplete.countDown();

    hook.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse("hook must return once teardown finished", hook.isAlive());
    Assert.assertEquals(List.of("stop-primary-listener", "teardown-complete"), order);
  }

  @Test
  public void returnsAfterTheTimeoutSoAStuckTeardownCannotHangTheJvm() throws Exception {
    Thread hook = runHook(new ProxyShutdownHook(() -> { }, new CountDownLatch(1), 200L), "timeout");

    hook.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse("hook must give up instead of blocking the JVM forever", hook.isAlive());
  }

  @Test
  public void stillWaitsForTeardownWhenStoppingTheListenerFails() throws Exception {
    CountDownLatch teardownComplete = new CountDownLatch(1);
    Thread hook = runHook(new ProxyShutdownHook(
        () -> {
          throw new IllegalStateException("listener stop failed");
        },
        teardownComplete,
        TimeUnit.SECONDS.toMillis(10)), "failed-stop");

    Thread.sleep(200L);
    Assert.assertTrue("a failed listener stop must not skip the teardown wait", hook.isAlive());

    teardownComplete.countDown();
    hook.join(TimeUnit.SECONDS.toMillis(5));
    Assert.assertFalse(hook.isAlive());
  }

  private static Thread runHook(ProxyShutdownHook hook, String name) {
    Thread thread = new Thread(hook, "shutdown-hook-test-" + name);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }
}
