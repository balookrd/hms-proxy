package io.github.mmalykhin.hmsproxy.util;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses Hive-style duration values such as {@code 600s}, {@code 600sec}, {@code 5min} or
 * {@code 2d}. The accepted suffixes mirror {@code HiveConf.unitFor} so a value the backend
 * metastore accepts is not rejected here, and amounts stay integral because Hive parses them with
 * {@code Long.parseLong}. A missing unit means seconds, which is also Hive's convention.
 */
public final class TimeoutValueParser {
  private static final Logger LOG = LoggerFactory.getLogger(TimeoutValueParser.class);
  private static final Pattern DURATION_PATTERN =
      Pattern.compile("^\\s*(\\d+)\\s*([a-zA-Z]*)\\s*$");
  /** Unrecognized values are logged once each; this parser also runs on backend reconnect paths. */
  private static final Set<String> WARNED_VALUES = ConcurrentHashMap.newKeySet();
  private static final int MAX_WARNED_VALUES = 64;

  private TimeoutValueParser() {
  }

  public static long parseDurationMs(String value, long defaultValueMs) {
    if (value == null || value.isBlank()) {
      return defaultValueMs;
    }

    Matcher matcher = DURATION_PATTERN.matcher(value);
    if (!matcher.matches()) {
      return warnAndFallback(value, defaultValueMs);
    }

    long amount;
    try {
      amount = Long.parseLong(matcher.group(1));
    } catch (NumberFormatException e) {
      return warnAndFallback(value, defaultValueMs);
    }
    TimeUnit unit = unitFor(matcher.group(2));
    if (unit == null) {
      return warnAndFallback(value, defaultValueMs);
    }
    return toMillis(amount, unit);
  }

  public static String formatDurationMs(long durationMs) {
    return Math.max(durationMs, 1L) + "ms";
  }

  /** Returns null for an unknown suffix. Mirrors the suffixes accepted by {@code HiveConf.unitFor}. */
  private static TimeUnit unitFor(String rawUnit) {
    return switch (rawUnit.trim().toLowerCase(Locale.ROOT)) {
      case "" -> TimeUnit.SECONDS;
      case "d", "day", "days" -> TimeUnit.DAYS;
      case "h", "hour", "hours" -> TimeUnit.HOURS;
      case "m", "min", "mins", "minute", "minutes" -> TimeUnit.MINUTES;
      case "s", "sec", "secs", "second", "seconds" -> TimeUnit.SECONDS;
      case "ms", "msec", "msecs", "millisecond", "milliseconds" -> TimeUnit.MILLISECONDS;
      case "us", "usec", "usecs", "microsecond", "microseconds" -> TimeUnit.MICROSECONDS;
      case "ns", "nsec", "nsecs", "nanosecond", "nanoseconds" -> TimeUnit.NANOSECONDS;
      default -> null;
    };
  }

  /** Sub-millisecond units round down, but a non-zero duration never collapses into "no timeout". */
  private static long toMillis(long amount, TimeUnit unit) {
    long millis = unit.toMillis(amount);
    return millis == 0L && amount > 0L ? 1L : millis;
  }

  private static long warnAndFallback(String value, long defaultValueMs) {
    if (WARNED_VALUES.size() < MAX_WARNED_VALUES && WARNED_VALUES.add(value)) {
      LOG.warn(
          "Unrecognized duration value '{}'; falling back to {}ms. Expected an integer with an optional "
              + "Hive unit suffix (ns, us, ms, s, m, h, d); no suffix means seconds",
          value,
          defaultValueMs);
    }
    return defaultValueMs;
  }
}
