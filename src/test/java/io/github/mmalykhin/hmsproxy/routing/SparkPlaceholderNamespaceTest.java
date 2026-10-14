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

    NoSuchObjectException nullCatExc = SparkPlaceholderNamespace.newNoSuchTableException(null, null, null);
    Assert.assertEquals("hive.global_temp.table table not found", nullCatExc.getMessage());
  }
}
