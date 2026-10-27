package io.github.mmalykhin.hmsproxy.util;

import org.slf4j.Logger;

/**
 * Closes components in order and keeps going past a failing one. Later steps stop scheduler
 * threads and flush on-disk state, so one component's cleanup error must not cost them; the first
 * failure is rethrown at the end with the rest attached as suppressed.
 */
public final class ShutdownSequence {
  @FunctionalInterface
  public interface Step {
    void close() throws Exception;
  }

  private final Logger log;
  private Exception failure;

  public ShutdownSequence(Logger log) {
    this.log = log;
  }

  public ShutdownSequence close(String component, Step step) {
    try {
      step.close();
    } catch (Exception e) {
      log.warn("Failed to close {}", component, e);
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    return this;
  }

  /** The first failure, or null when every step closed cleanly. */
  public Exception failure() {
    return failure;
  }
}
