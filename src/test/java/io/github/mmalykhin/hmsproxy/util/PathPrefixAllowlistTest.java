package io.github.mmalykhin.hmsproxy.util;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class PathPrefixAllowlistTest {
  @Test
  public void parseTrimsAndDropsBlankEntries() {
    Assert.assertEquals(
        List.of("hdfs://ns/a", "hdfs://ns/b"),
        PathPrefixAllowlist.parse(" hdfs://ns/a , ,hdfs://ns/b "));
  }

  @Test
  public void parseOfNullOrBlankIsEmpty() {
    Assert.assertEquals(List.of(), PathPrefixAllowlist.parse(null));
    Assert.assertEquals(List.of(), PathPrefixAllowlist.parse("   "));
  }

  @Test
  public void matchesOnPathSeparatorBoundaryOnly() {
    List<String> prefixes = List.of("hdfs://ns/warehouse/db");
    Assert.assertTrue(PathPrefixAllowlist.matches("hdfs://ns/warehouse/db/t/data.parquet", prefixes));
    Assert.assertTrue("the prefix itself is inside its own tree",
        PathPrefixAllowlist.matches("hdfs://ns/warehouse/db", prefixes));
    Assert.assertFalse("a sibling directory sharing the name prefix must not match",
        PathPrefixAllowlist.matches("hdfs://ns/warehouse/dbx/t/data.parquet", prefixes));
  }

  @Test
  public void matchesHonoursATrailingSlashInTheConfiguredPrefix() {
    Assert.assertTrue(PathPrefixAllowlist.matches(
        "hdfs://ns/warehouse/db/t", List.of("hdfs://ns/warehouse/db/")));
    Assert.assertFalse(PathPrefixAllowlist.matches(
        "hdfs://ns/warehouse/dbx", List.of("hdfs://ns/warehouse/db/")));
  }

  @Test
  public void emptyAllowlistMatchesNothingAndNullLocationNeverMatches() {
    Assert.assertFalse(PathPrefixAllowlist.matches("hdfs://ns/a", List.of()));
    Assert.assertFalse(PathPrefixAllowlist.matches(null, List.of("hdfs://ns/a")));
  }
}
