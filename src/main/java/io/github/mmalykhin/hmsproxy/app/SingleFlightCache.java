package io.github.mmalykhin.hmsproxy.app;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * TTL cache whose refresh is single-flight: at most one caller ever runs the loader, and callers
 * that arrive while a refresh is in progress are served the previous value instead of queuing up
 * behind it. Used so that frequent management scrapes cannot multiply expensive readiness probes,
 * and so that a slow probe occupies one management thread rather than all of them.
 */
final class SingleFlightCache<T> {
  private final long ttlNanos;
  private final LongSupplier nanoClock;
  private final ReentrantLock refreshLock = new ReentrantLock();
  private volatile Entry<T> current;

  SingleFlightCache(long ttlMillis) {
    this(ttlMillis, System::nanoTime);
  }

  SingleFlightCache(long ttlMillis, LongSupplier nanoClock) {
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0L, ttlMillis));
    this.nanoClock = nanoClock;
  }

  T get(Supplier<T> loader) {
    Entry<T> snapshot = current;
    if (isFresh(snapshot)) {
      return snapshot.value();
    }
    if (snapshot == null) {
      refreshLock.lock();
    } else if (!refreshLock.tryLock()) {
      return snapshot.value();
    }
    try {
      Entry<T> latest = current;
      if (isFresh(latest)) {
        return latest.value();
      }
      T value = loader.get();
      current = new Entry<>(value, nanoClock.getAsLong());
      return value;
    } finally {
      refreshLock.unlock();
    }
  }

  private boolean isFresh(Entry<T> entry) {
    return entry != null && nanoClock.getAsLong() - entry.createdAtNanos() < ttlNanos;
  }

  private record Entry<V>(V value, long createdAtNanos) {
  }
}
