package io.github.mmalykhin.hmsproxy.routing;

import java.util.Locale;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

/**
 * Spark SQL uses an internal in-memory database for global temporary views
 * ({@code StaticSQLConf.GLOBAL_TEMP_DATABASE}, defaults to {@code global_temp}),
 * and also supports direct path-based relation queries (e.g. {@code SELECT ... FROM csv.`hdfs://...`}).
 * When creating a SparkSession or SharedState, Spark checks {@code externalCatalog.databaseExists("global_temp")}
 * via {@code get_database("global_temp")}. If the metastore returns a database or throws {@code MetaException}
 * (e.g. from Ranger access denial when {@code mask-unauthorized-as-not-found=false}), Spark fails with an
 * {@code AnalysisException}.
 *
 * Similarly, when resolving path-based queries, Spark or table format catalogs (such as Apache Iceberg's
 * {@code HiveTableOperations}) query the metastore via {@code get_table(format, path)} before falling back to
 * direct file reading. If the metastore throws {@code MetaException} (e.g. from Ranger denying access to the
 * format pseudo-database or URI table name), Iceberg and Spark fail with a fatal
 * {@code RuntimeException: Failed to get table info from metastore ...}.
 *
 * These placeholders and path-based tables belong to no backend metastore and must immediately return
 * {@link NoSuchObjectException} (or empty collections for list RPCs) without consulting Ranger authorization
 * or backend metastores.
 */
final class SparkPlaceholderNamespace {
  static final String GLOBAL_TEMP_DATABASE = "global_temp";

  private static final Set<String> SPARK_FILE_FORMAT_DATABASES = Set.of(
      "csv",
      "parquet",
      "orc",
      "json",
      "avro",
      "text",
      "binaryfile",
      "delta",
      "iceberg"
  );

  private SparkPlaceholderNamespace() {}

  static boolean isSparkReservedDbName(String dbName) {
    return dbName != null && GLOBAL_TEMP_DATABASE.equals(dbName.trim().toLowerCase(Locale.ROOT));
  }

  static boolean isSparkFileFormatDbName(String dbName) {
    return dbName != null && SPARK_FILE_FORMAT_DATABASES.contains(dbName.trim().toLowerCase(Locale.ROOT));
  }

  static boolean isSparkPathBasedTable(String tblName) {
    if (tblName == null || tblName.isBlank()) {
      return false;
    }
    String trimmed = tblName.trim();
    return trimmed.contains("://")
        || trimmed.startsWith("/")
        || trimmed.contains("/")
        || trimmed.contains("\\");
  }

  static boolean isSparkReservedTable(String dbName, String tblName) {
    if (isSparkReservedDbName(dbName)) {
      return true;
    }
    if (isSparkPathBasedTable(tblName)) {
      return true;
    }
    return false;
  }

  static NoSuchObjectException newNoSuchDatabaseException(String dbName) {
    String db = dbName != null && !dbName.isBlank() ? dbName : GLOBAL_TEMP_DATABASE;
    return new NoSuchObjectException("Database " + db + " not found");
  }

  static NoSuchObjectException newNoSuchTableException(
      String catName,
      String dbName,
      String tblName
  ) {
    String table = tblName != null && !tblName.isBlank() ? tblName : "table";
    String db = dbName != null && !dbName.isBlank() ? dbName : GLOBAL_TEMP_DATABASE;
    String cat = catName != null && !catName.isBlank() ? catName : "hive";
    return new NoSuchObjectException(cat + "." + db + "." + table + " table not found");
  }
}
