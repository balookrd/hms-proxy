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

  static String unquote(String s) {
    if (s == null) {
      return null;
    }
    String trimmed = s.trim();
    if (trimmed.length() >= 2) {
      char first = trimmed.charAt(0);
      char last = trimmed.charAt(trimmed.length() - 1);
      if ((first == '"' && last == '"')
          || (first == '\'' && last == '\'')
          || (first == '`' && last == '`')) {
        return trimmed.substring(1, trimmed.length() - 1).trim();
      }
    }
    return trimmed;
  }

  static boolean isSparkReservedDbName(String dbName) {
    if (dbName == null || dbName.isBlank()) {
      return false;
    }
    String normalized = unquote(dbName).toLowerCase(Locale.ROOT);
    if (GLOBAL_TEMP_DATABASE.equals(normalized) || isSparkFileFormatDbName(normalized)) {
      return true;
    }
    return isSparkPathBasedTable(dbName);
  }

  static boolean isSparkFileFormatDbName(String dbName) {
    if (dbName == null || dbName.isBlank()) {
      return false;
    }
    String unquoted = unquote(dbName).toLowerCase(Locale.ROOT);
    return SPARK_FILE_FORMAT_DATABASES.contains(unquoted);
  }

  static boolean startsWithSparkFileFormat(String name) {
    if (name == null || name.isBlank()) {
      return false;
    }
    String lower = unquote(name).toLowerCase(Locale.ROOT);
    for (String format : SPARK_FILE_FORMAT_DATABASES) {
      if (lower.startsWith(format + ".")
          || lower.startsWith(format + ":")
          || lower.startsWith(format + "/")) {
        return true;
      }
    }
    return false;
  }

  static boolean isSparkPathBasedTable(String tblName) {
    if (tblName == null || tblName.isBlank()) {
      return false;
    }
    String trimmed = unquote(tblName.trim());
    return trimmed.contains("://")
        || trimmed.startsWith("/")
        || trimmed.contains("/")
        || trimmed.contains("\\")
        || startsWithSparkFileFormat(trimmed);
  }

  static boolean isSparkReservedTable(String dbName, String tblName) {
    if (isSparkReservedDbName(dbName)) {
      return true;
    }
    if (isSparkFileFormatDbName(dbName)) {
      return true;
    }
    if (isSparkPathBasedTable(tblName)) {
      return true;
    }
    if (isSparkPathBasedTable(dbName)) {
      return true;
    }
    return false;
  }

  static boolean isSparkPlaceholderNoSuchObject(Throwable throwable) {
    if (!(throwable instanceof NoSuchObjectException)) {
      return false;
    }
    String message = throwable.getMessage();
    if (message == null) {
      return false;
    }
    if (message.contains(GLOBAL_TEMP_DATABASE)) {
      return true;
    }
    for (String format : SPARK_FILE_FORMAT_DATABASES) {
      if (message.contains("Database " + format + " not found")
          || message.contains("." + format + ".")
          || message.contains("." + format + " ")) {
        return true;
      }
    }
    return message.contains("://") || message.contains("/");
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
