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

  @Test
  public void containsUnsafePathSyntaxRejectsTraversalAndControlCharacters() {
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax(null));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("   "));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/../secret"));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/.."));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/./t"));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/%2e%2e/secret"));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/%2f/t"));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external\\t"));
    Assert.assertTrue(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/t\0"));
    Assert.assertFalse(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/v1.0/part.parquet"));
    Assert.assertFalse(PathPrefixAllowlist.containsUnsafePathSyntax("hdfs://ns/external/.staging/t"));
  }

  @Test
  public void matchesStrictDescendantRefusesAllowlistRootAndUnsafePaths() {
    List<String> prefixes = List.of("hdfs://ns/external/");
    Assert.assertFalse("exact root without slash must be refused",
        PathPrefixAllowlist.matchesStrictDescendant("hdfs://ns/external", prefixes));
    Assert.assertFalse("exact root with slash must be refused",
        PathPrefixAllowlist.matchesStrictDescendant("hdfs://ns/external/", prefixes));
    Assert.assertFalse("traversal under prefix must be refused",
        PathPrefixAllowlist.matchesStrictDescendant("hdfs://ns/external/../secret", prefixes));
    Assert.assertFalse("sibling directory must be refused",
        PathPrefixAllowlist.matchesStrictDescendant("hdfs://ns/external_other/t1", prefixes));
    Assert.assertTrue("strict descendant must be allowed",
        PathPrefixAllowlist.matchesStrictDescendant("hdfs://ns/external/t1", prefixes));
    Assert.assertTrue("strict descendant with trailing slash must be allowed",
        PathPrefixAllowlist.matchesStrictDescendant("hdfs://ns/external/db/t1/", prefixes));
    Assert.assertFalse("file root itself must be refused",
        PathPrefixAllowlist.matchesStrictDescendant("file:///", List.of("file:/")));
    Assert.assertTrue("descendant under file root must be allowed",
        PathPrefixAllowlist.matchesStrictDescendant("file:///tmp/t1", List.of("file:/")));
  }
}
