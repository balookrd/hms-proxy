package io.github.mmalykhin.hmsproxy.util;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class ShutdownSequenceTest {
  @Test
  public void everyStepRunsAndTheFirstFailureCarriesTheRest() {
    List<String> closed = new ArrayList<>();
    IllegalStateException ranger = new IllegalStateException("ranger cleanup failed");
    Exception zk = new Exception("zookeeper close failed");

    ShutdownSequence shutdown = new ShutdownSequence(LoggerFactory.getLogger(ShutdownSequenceTest.class))
        .close("routing handler", () -> {
          closed.add("routing handler");
          throw ranger;
        })
        .close("cache invalidator", () -> {
          closed.add("cache invalidator");
          throw zk;
        })
        .close("user group resolver", () -> closed.add("user group resolver"));

    Assert.assertEquals("a failing step must not skip the ones after it",
        List.of("routing handler", "cache invalidator", "user group resolver"), closed);
    Assert.assertSame(ranger, shutdown.failure());
    Assert.assertArrayEquals(new Throwable[] {zk}, ranger.getSuppressed());
  }

  @Test
  public void aCleanShutdownReportsNoFailure() {
    Assert.assertNull(new ShutdownSequence(LoggerFactory.getLogger(ShutdownSequenceTest.class))
        .close("a", () -> { })
        .failure());
  }
}
