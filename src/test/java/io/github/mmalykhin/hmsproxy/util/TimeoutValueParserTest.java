package io.github.mmalykhin.hmsproxy.util;

import org.junit.Assert;
import org.junit.Test;

public class TimeoutValueParserTest {
  private static final long DEFAULT_MS = 4321L;

  @Test
  public void treatsMissingUnitAsSeconds() {
    Assert.assertEquals(600_000L, TimeoutValueParser.parseDurationMs("600", DEFAULT_MS));
  }

  @Test
  public void parsesShortHiveUnits() {
    Assert.assertEquals(250L, TimeoutValueParser.parseDurationMs("250ms", DEFAULT_MS));
    Assert.assertEquals(600_000L, TimeoutValueParser.parseDurationMs("600s", DEFAULT_MS));
    Assert.assertEquals(300_000L, TimeoutValueParser.parseDurationMs("5m", DEFAULT_MS));
    Assert.assertEquals(7_200_000L, TimeoutValueParser.parseDurationMs("2h", DEFAULT_MS));
    Assert.assertEquals(172_800_000L, TimeoutValueParser.parseDurationMs("2d", DEFAULT_MS));
  }

  @Test
  public void parsesLongHiveUnits() {
    Assert.assertEquals(600_000L, TimeoutValueParser.parseDurationMs("600sec", DEFAULT_MS));
    Assert.assertEquals(300_000L, TimeoutValueParser.parseDurationMs("5min", DEFAULT_MS));
    Assert.assertEquals(3_600_000L, TimeoutValueParser.parseDurationMs("1hour", DEFAULT_MS));
    Assert.assertEquals(86_400_000L, TimeoutValueParser.parseDurationMs("1day", DEFAULT_MS));
    Assert.assertEquals(1_500L, TimeoutValueParser.parseDurationMs("1500msec", DEFAULT_MS));
  }

  @Test
  public void toleratesCaseAndSurroundingWhitespace() {
    Assert.assertEquals(600_000L, TimeoutValueParser.parseDurationMs("  600 SEC ", DEFAULT_MS));
  }

  @Test
  public void roundsSubMillisecondUnitsDownButNeverToZero() {
    Assert.assertEquals(1L, TimeoutValueParser.parseDurationMs("1500us", DEFAULT_MS));
    Assert.assertEquals(1L, TimeoutValueParser.parseDurationMs("500us", DEFAULT_MS));
    Assert.assertEquals(1L, TimeoutValueParser.parseDurationMs("10ns", DEFAULT_MS));
    Assert.assertEquals(0L, TimeoutValueParser.parseDurationMs("0us", DEFAULT_MS));
  }

  @Test
  public void fallsBackToDefaultForUnrecognizedValues() {
    // Hive parses durations with Long.parseLong, so a fractional amount is invalid there too.
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs("1.5s", DEFAULT_MS));
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs("600 seconds ish", DEFAULT_MS));
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs("forever", DEFAULT_MS));
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs("-5s", DEFAULT_MS));
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs("10weeks", DEFAULT_MS));
  }

  @Test
  public void fallsBackToDefaultForBlankOrMissingValues() {
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs(null, DEFAULT_MS));
    Assert.assertEquals(DEFAULT_MS, TimeoutValueParser.parseDurationMs("   ", DEFAULT_MS));
  }

  @Test
  public void formatsDurationsBackAsMilliseconds() {
    Assert.assertEquals("1500ms", TimeoutValueParser.formatDurationMs(1_500L));
    Assert.assertEquals("1ms", TimeoutValueParser.formatDurationMs(0L));
  }

  @Test
  public void roundTripsFormattedDurations() {
    Assert.assertEquals(
        1_500L,
        TimeoutValueParser.parseDurationMs(TimeoutValueParser.formatDurationMs(1_500L), DEFAULT_MS));
  }
}
