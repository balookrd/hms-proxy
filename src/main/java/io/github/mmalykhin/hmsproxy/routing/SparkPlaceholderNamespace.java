package io.github.mmalykhin.hmsproxy.routing;

import java.util.Locale;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

/**
 * Spark SQL uses an internal in-memory database for global temporary views
 * ({@code StaticSQLConf.GLOBAL_TEMP_DATABASE}, defaults to {@code global_temp}).
 * When creating a SparkSession or SharedState, Spark checks {@code externalCatalog.databaseExists("global_temp")}
 * via {@code get_database("global_temp")}. If the metastore returns a database or throws {@code MetaException}
 * (e.g. from Ranger access denial when {@code mask-unauthorized-as-not-found=false}), Spark fails with an
 * {@code AnalysisException}.
 *
 * This placeholder belongs to no backend metastore and must immediately return {@link NoSuchObjectException}
 * (or empty collections for list RPCs) without consulting Ranger authorization or backend metastores.
 */
final class SparkPlaceholderNamespace {
  static final String GLOBAL_TEMP_DATABASE = "global_temp";

  private SparkPlaceholderNamespace() {}

  static boolean isSparkReservedDbName(String dbName) {
    return dbName != null && GLOBAL_TEMP_DATABASE.equals(dbName.trim().toLowerCase(Locale.ROOT));
  }

  static boolean isSparkReservedTable(String dbName, String tblName) {
    return isSparkReservedDbName(dbName);
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
