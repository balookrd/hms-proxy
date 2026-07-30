package io.github.mmalykhin.smokestand;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Minimal Iceberg REST client for the smoke stand: creates a table through the proxy's REST
 * front door, writes real Parquet data files into the stand's HDFS and commits them as
 * snapshots through REST - the half of the protocol curl cannot exercise. Runs inside the
 * stand network (data-file writes need the datanodes, which only resolve in-network).
 *
 * Commands:
 *   create --uri U --namespace NS --table T [--properties k=v,k=v]
 *                                                      create the table (id int, src string)
 *   append --uri U --namespace NS --table T --rows N --marker M
 *                                                      append N rows tagged src=M
 *   count  --uri U --namespace NS --table T [--where col=value]
 *                                                      print "rows=N" from a full scan
 *   files  --uri U --namespace NS --table T            print the scan's data- and delete-file counts
 *   drop   --uri U --namespace NS --table T [--purge]  drop the table through REST
 *
 * With --keytab/--principal set, logs into Kerberos before touching HDFS. With
 * --rest-auth spnego, every REST request carries a fresh SPNEGO token minted from the native
 * ticket cache (run kinit first) - see {@link SpnegoAuthManager}.
 */
public final class IcebergRestWriter {

  private IcebergRestWriter() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      fail("missing command: create | append | count | files | drop");
    }
    String command = args[0].toLowerCase(Locale.ROOT);
    Map<String, String> opts = parseOptions(args);

    String uri = required(opts, "uri");
    String namespace = required(opts, "namespace");
    String tableName = required(opts, "table");

    Configuration conf = new Configuration();
    // The stand's datanodes are reachable by container hostname, not by their internal IPs as
    // seen from other networks; harmless when resolution already works.
    conf.set("dfs.client.use.datanode.hostname", "true");

    String keytab = opts.get("keytab");
    String principal = opts.get("principal");
    if (keytab != null || principal != null) {
      if (keytab == null || principal == null) {
        fail("--keytab and --principal must be set together");
      }
      conf.set("hadoop.security.authentication", "kerberos");
      // The two clusters' service principals differ per host; accept what the cluster presents,
      // the same way the stand's other Kerberos clients are configured.
      conf.set("dfs.namenode.kerberos.principal.pattern", "*");
      conf.set("dfs.data.transfer.protection", "authentication");
      UserGroupInformation.setConfiguration(conf);
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      log("kerberos login as " + principal);
    }

    RESTCatalog catalog = new RESTCatalog();
    catalog.setConf(conf);
    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.URI, uri);
    if (opts.containsKey("warehouse")) {
      props.put(CatalogProperties.WAREHOUSE_LOCATION, opts.get("warehouse"));
    }
    if ("spnego".equalsIgnoreCase(opts.getOrDefault("rest-auth", ""))) {
      // Let JGSS mint SPNEGO tokens from the native ticket cache (a prior kinit) instead of
      // demanding credentials in the JAAS Subject.
      System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
      props.put("rest.auth.type", SpnegoAuthManager.class.getName());
    }
    catalog.initialize("smoke-rest-writer", props);

    TableIdentifier ident = TableIdentifier.of(Namespace.of(namespace), tableName);
    try {
      switch (command) {
        case "create" -> create(catalog, ident, opts.get("properties"));
        case "append" -> append(catalog, ident,
            Integer.parseInt(required(opts, "rows")), opts.getOrDefault("marker", "rest"));
        case "count" -> count(catalog, ident, opts.get("where"));
        case "files" -> files(catalog, ident);
        case "drop" -> drop(catalog, ident, opts.containsKey("purge"));
        default -> fail("unknown command: " + command);
      }
    } finally {
      catalog.close();
    }
  }

  private static void create(RESTCatalog catalog, TableIdentifier ident, String properties) {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "src", Types.StringType.get()));
    Map<String, String> tableProps = new HashMap<>();
    // Makes the server-side HiveTableOperations register the table in the metastore with the
    // Hive storage handler and SerDe, so a plain HiveServer2 (with iceberg-hive-runtime on its
    // classpath) can read and write it - the whole point of the interop scenario.
    tableProps.put("engine.hive.enabled", "true");
    // --properties wins over the defaults: the row-level scenario sets format-version and the
    // write.*.mode pair to pick merge-on-read or copy-on-write at create time.
    tableProps.putAll(parseProperties(properties));
    Table table = catalog.createTable(ident, schema, PartitionSpec.unpartitioned(), tableProps);
    log("created " + ident + " at " + table.location() + " with " + tableProps);
  }

  private static void append(RESTCatalog catalog, TableIdentifier ident, int rows, String marker)
      throws Exception {
    Table table = catalog.loadTable(ident);
    OutputFile out = table.io().newOutputFile(
        table.locationProvider().newDataLocation(
            FileFormat.PARQUET.addExtension(marker + "-" + UUID.randomUUID())));
    DataWriter<Record> writer = Parquet.writeData(out)
        .schema(table.schema())
        .createWriterFunc(GenericParquetWriter::buildWriter)
        .overwrite()
        .withSpec(PartitionSpec.unpartitioned())
        .build();
    try (writer) {
      GenericRecord template = GenericRecord.create(table.schema());
      for (int i = 1; i <= rows; i++) {
        Record record = template.copy();
        record.setField("id", i);
        record.setField("src", marker);
        writer.write(record);
      }
    }
    DataFile dataFile = writer.toDataFile();
    table.newAppend().appendFile(dataFile).commit();
    log("appended " + rows + " row(s) tagged src=" + marker + " to " + ident
        + "; snapshot=" + table.currentSnapshot().snapshotId()
        + " total-records=" + table.currentSnapshot().summary().get("total-records"));
  }

  private static void count(RESTCatalog catalog, TableIdentifier ident, String where)
      throws Exception {
    Table table = catalog.loadTable(ident);
    IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(table);
    if (where != null && !where.isEmpty()) {
      scan = scan.where(equality(table.schema(), where));
    }
    long count = 0;
    // A generic scan applies the table's delete files, so this count is the merge-on-read result,
    // not the raw data-file row count.
    try (CloseableIterable<Record> records = scan.build()) {
      for (Record ignored : records) {
        count++;
      }
    }
    // The stable line the smoke script greps for.
    System.out.println("rows=" + count);
  }

  /**
   * Reports what the current snapshot's scan is made of. The delete-file count is the one fact
   * that tells merge-on-read from copy-on-write: a copy-on-write engine rewrites the data files
   * and leaves none behind.
   */
  private static void files(RESTCatalog catalog, TableIdentifier ident) throws Exception {
    Table table = catalog.loadTable(ident);
    long dataFiles = 0;
    Set<String> deleteFiles = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        dataFiles++;
        for (DeleteFile delete : task.deletes()) {
          deleteFiles.add(delete.location());
        }
      }
    }
    // The stable lines the smoke script greps for.
    System.out.println("data-files=" + dataFiles);
    System.out.println("delete-files=" + deleteFiles.size());
  }

  private static void drop(RESTCatalog catalog, TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    if (!dropped) {
      fail("drop of " + ident + " returned false");
    }
    log("dropped " + ident + (purge ? " (purged)" : ""));
  }

  /** Builds a single-column equality predicate, typed from the table schema. */
  private static Expression equality(Schema schema, String where) {
    int separator = where.indexOf('=');
    if (separator <= 0) {
      fail("--where expects <column>=<value>, got: " + where);
    }
    String column = where.substring(0, separator);
    String value = where.substring(separator + 1);
    Types.NestedField field = schema.findField(column);
    if (field == null) {
      fail("--where names an unknown column: " + column);
    }
    Type.TypeID typeId = field.type().typeId();
    return switch (typeId) {
      case INTEGER -> Expressions.equal(column, Integer.parseInt(value));
      case LONG -> Expressions.equal(column, Long.parseLong(value));
      case STRING -> Expressions.equal(column, value);
      default -> {
        fail("--where does not support column type " + typeId);
        yield null;
      }
    };
  }

  /** Parses the {@code k=v,k=v} form of --properties; an empty or missing value means none. */
  private static Map<String, String> parseProperties(String properties) {
    Map<String, String> parsed = new HashMap<>();
    if (properties == null || properties.isEmpty()) {
      return parsed;
    }
    for (String pair : properties.split(",")) {
      int separator = pair.indexOf('=');
      if (separator <= 0) {
        fail("--properties expects k=v[,k=v], got: " + pair);
      }
      parsed.put(pair.substring(0, separator), pair.substring(separator + 1));
    }
    return parsed;
  }

  private static Map<String, String> parseOptions(String[] args) {
    Map<String, String> opts = new HashMap<>();
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if (!arg.startsWith("--")) {
        fail("unexpected argument: " + arg);
      }
      String key = arg.substring(2);
      // Flags without a value (--purge) map to "".
      if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
        opts.put(key, args[++i]);
      } else {
        opts.put(key, "");
      }
    }
    return opts;
  }

  private static String required(Map<String, String> opts, String key) {
    String value = opts.get(key);
    if (value == null || value.isEmpty()) {
      fail("missing required option --" + key);
    }
    return value;
  }

  private static void log(String message) {
    System.out.println("[iceberg-rest-writer] " + message);
  }

  private static void fail(String message) {
    System.err.println("[iceberg-rest-writer] error: " + message);
    System.exit(2);
  }
}
