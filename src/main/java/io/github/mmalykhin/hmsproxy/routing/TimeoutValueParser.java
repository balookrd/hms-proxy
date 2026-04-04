package io.github.mmalykhin.hmsproxy.routing;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TimeoutValueParser {
  private static final Pattern DURATION_PATTERN =
      Pattern.compile("^\\s*(\\d+)\\s*(ms|s|m|h)?\\s*$", Pattern.CASE_INSENSITIVE);

  private TimeoutValueParser() {
  }

  public static long parseDurationMs(String value, long defaultValueMs) {
    if (value == null || value.isBlank()) {
      return defaultValueMs;
    }

    Matcher matcher = DURATION_PATTERN.matcher(value);
    if (!matcher.matches()) {
      return defaultValueMs;
    }

    long amount = Long.parseLong(matcher.group(1));
    String unit = matcher.group(2);
    if (unit == null || unit.isBlank()) {
      return amount * 1000L;
    }
    return switch (unit.toLowerCase(Locale.ROOT)) {
      case "ms" -> amount;
      case "s" -> amount * 1000L;
      case "m" -> amount * 60_000L;
      case "h" -> amount * 3_600_000L;
      default -> defaultValueMs;
    };
  }

  public static String formatDurationMs(long durationMs) {
    return Math.max(durationMs, 1L) + "ms";
  }
}
