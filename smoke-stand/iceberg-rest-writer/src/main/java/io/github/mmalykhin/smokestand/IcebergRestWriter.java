package io.github.mmalykhin.smokestand;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;

/**
 * Minimal Iceberg REST client for the smoke stand: creates a table through the proxy's REST
 * front door, writes real Parquet data files into the stand's HDFS and commits them as
 * snapshots through REST - the half of the protocol curl cannot exercise. Runs inside the
 * stand network (data-file writes need the datanodes, which only resolve in-network).
 *
 * Commands:
 *   create --uri U --namespace NS --table T            create the table (id int, src string)
 *   append --uri U --namespace NS --table T --rows N --marker M
 *                                                      append N rows tagged src=M
 *   count  --uri U --namespace NS --table T            print "rows=N" from a full scan
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
      fail("missing command: create | append | count | drop");
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
        case "create" -> create(catalog, ident);
        case "append" -> append(catalog, ident,
            Integer.parseInt(required(opts, "rows")), opts.getOrDefault("marker", "rest"));
        case "count" -> count(catalog, ident);
        case "drop" -> drop(catalog, ident, opts.containsKey("purge"));
        default -> fail("unknown command: " + command);
      }
    } finally {
      catalog.close();
    }
  }

  private static void create(RESTCatalog catalog, TableIdentifier ident) {
    Schema schema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "src", Types.StringType.get()));
    Map<String, String> tableProps = new HashMap<>();
    // Makes the server-side HiveTableOperations register the table in the metastore with the
    // Hive storage handler and SerDe, so a plain HiveServer2 (with iceberg-hive-runtime on its
    // classpath) can read and write it - the whole point of the interop scenario.
    tableProps.put("engine.hive.enabled", "true");
    Table table = catalog.createTable(ident, schema, PartitionSpec.unpartitioned(), tableProps);
    log("created " + ident + " at " + table.location());
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

  private static void count(RESTCatalog catalog, TableIdentifier ident) throws Exception {
    Table table = catalog.loadTable(ident);
    long count = 0;
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record ignored : records) {
        count++;
      }
    }
    // The stable line the smoke script greps for.
    System.out.println("rows=" + count);
  }

  private static void drop(RESTCatalog catalog, TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    if (!dropped) {
      fail("drop of " + ident + " returned false");
    }
    log("dropped " + ident + (purge ? " (purged)" : ""));
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
