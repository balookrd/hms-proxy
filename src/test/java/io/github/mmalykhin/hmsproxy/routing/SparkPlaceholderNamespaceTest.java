package io.github.mmalykhin.hmsproxy.routing;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Assert;
import org.junit.Test;

public class SparkPlaceholderNamespaceTest {

  @Test
  public void isSparkReservedDbNameRecognizesGlobalTemp() {
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedDbName("global_temp"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedDbName("GLOBAL_TEMP"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedDbName("  global_temp  "));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedDbName("Global_Temp"));

    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedDbName(null));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedDbName(""));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedDbName("   "));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedDbName("global_temp_2"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedDbName("default"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedDbName("_dummy_database"));
  }

  @Test
  public void isSparkReservedTableRecognizesAnyTableInGlobalTemp() {
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("global_temp", "view1"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("GLOBAL_TEMP", "any_table"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("global_temp", null));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedTable("sales", "view1"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedTable(null, "view1"));
  }

  @Test
  public void isSparkFileFormatDbNameRecognizesStandardFormats() {
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("csv"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("CSV"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("parquet"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("orc"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("json"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("avro"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("text"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("delta"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkFileFormatDbName("iceberg"));

    Assert.assertFalse(SparkPlaceholderNamespace.isSparkFileFormatDbName("default"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkFileFormatDbName("sales"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkFileFormatDbName(null));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkFileFormatDbName(""));
  }

  @Test
  public void isSparkPathBasedTableRecognizesUriSchemesAndPaths() {
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkPathBasedTable("hdfs://titanium/warehouse/tablespace/external/hive/amdocs_arena_lz.db/csvlogs/tasks/"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkPathBasedTable("file:///tmp/data"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkPathBasedTable("s3a://bucket/path/to/table"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkPathBasedTable("/warehouse/tablespace/external/hive/db/table"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkPathBasedTable("sub/dir/path"));

    Assert.assertFalse(SparkPlaceholderNamespace.isSparkPathBasedTable("tasks"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkPathBasedTable("my_table_123"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkPathBasedTable(null));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkPathBasedTable(""));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkPathBasedTable("   "));
  }

  @Test
  public void isSparkReservedTableRecognizesPathBasedTablesForAnyDatabase() {
    String hdfsPath = "hdfs://titanium/warehouse/tablespace/external/hive/amdocs_arena_lz.db/csvlogs/tasks/";
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("csv", hdfsPath));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("parquet", "/tmp/parquet_data"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("default", "s3a://bucket/data"));
    Assert.assertTrue(SparkPlaceholderNamespace.isSparkReservedTable("amdocs_arena_lz", hdfsPath));

    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedTable("csv", "regular_table"));
    Assert.assertFalse(SparkPlaceholderNamespace.isSparkReservedTable("default", "users"));
  }

  @Test
  public void newNoSuchDatabaseExceptionFormatsCorrectly() {
    NoSuchObjectException exc = SparkPlaceholderNamespace.newNoSuchDatabaseException("global_temp");
    Assert.assertNotNull(exc);
    Assert.assertEquals("Database global_temp not found", exc.getMessage());

    NoSuchObjectException defaultExc = SparkPlaceholderNamespace.newNoSuchDatabaseException(null);
    Assert.assertEquals("Database global_temp not found", defaultExc.getMessage());
  }

  @Test
  public void newNoSuchTableExceptionFormatsCorrectly() {
    NoSuchObjectException exc = SparkPlaceholderNamespace.newNoSuchTableException("hdp", "global_temp", "my_view");
    Assert.assertNotNull(exc);
    Assert.assertEquals("hdp.global_temp.my_view table not found", exc.getMessage());

    NoSuchObjectException pathExc = SparkPlaceholderNamespace.newNoSuchTableException(
        "catalog1", "csv", "hdfs://titanium/warehouse/tablespace/external/hive/amdocs_arena_lz.db/csvlogs/tasks/");
    Assert.assertNotNull(pathExc);
    Assert.assertEquals(
        "catalog1.csv.hdfs://titanium/warehouse/tablespace/external/hive/amdocs_arena_lz.db/csvlogs/tasks/ table not found",
        pathExc.getMessage());

    NoSuchObjectException nullCatExc = SparkPlaceholderNamespace.newNoSuchTableException(null, null, null);
    Assert.assertEquals("hive.global_temp.table table not found", nullCatExc.getMessage());
  }
}
