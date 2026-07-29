package io.github.mmalykhin.hmsproxy.restcatalog;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;

/** In-memory fake of ThriftHiveMetastore.Iface for unit tests in the restcatalog package. */
final class RecordingThriftIface {
  static final long LOCK_ID = 42L;

  // Magic database name that makes get_database throw an Error instead of a checked
  // Thrift exception. Proxy.newProxyInstance() passes Error/RuntimeException through the
  // invocation handler unwrapped (only checked exceptions get boxed in
  // UndeclaredThrowableException), so this reaches IcebergHttpHandler's catch-all exactly
  // like the real NoSuchMethodError this proxy phase needed to stop from hanging the client.
  static final String THROWS_ERROR_PROBE_DB = "throws_error_probe";

  final List<String> calls = new ArrayList<>();
  final Map<String, Database> databases = new HashMap<>();
  final Map<String, Table> tables = new HashMap<>();
  /**
   * Table names (unqualified) whose {@code alter_table} must fail, so a test can make a commit
   * break at the metastore rather than at Iceberg's requirement check - the two failure points
   * behave differently in a multi-table transaction.
   */
  final List<String> alterTableFailures = new ArrayList<>();
  final Map<String, List<String>> tablesByDatabase = new HashMap<>();
  List<String> allDatabases = Collections.emptyList();
  ShowLocksResponse lastShowLocksResponse;
  final ThriftHiveMetastore.Iface iface;

  RecordingThriftIface() {
    this.iface = (ThriftHiveMetastore.Iface) Proxy.newProxyInstance(
        ThriftHiveMetastore.Iface.class.getClassLoader(),
        new Class<?>[]{ThriftHiveMetastore.Iface.class},
        this::handle);
  }

  static Database database(String name) {
    Database db = new Database();
    db.setName(name);
    db.setDescription("");
    db.setLocationUri("file:///tmp/" + name);
    db.setParameters(new HashMap<>());
    return db;
  }

  static Table table(String db, String name) {
    Table t = new Table();
    t.setDbName(db);
    t.setTableName(name);
    t.setOwner("test");
    t.setTableType("EXTERNAL_TABLE");
    Map<String, String> params = new HashMap<>();
    // HiveCatalog filters listTables() by this marker; without it, Iceberg-aware
    // clients see an empty namespace even though the underlying HMS row exists.
    params.put("table_type", "ICEBERG");
    t.setParameters(params);
    return t;
  }

  private Object handle(Object proxy, Method method, Object[] args) throws Throwable {
    String name = method.getName();
    switch (name) {
      case "get_database": {
        String db = (String) args[0];
        calls.add("get_database:" + db);
        if (THROWS_ERROR_PROBE_DB.equals(db)) {
          throw new Error("simulated classpath-mismatch failure (e.g. NoSuchMethodError)");
        }
        Database value = databases.get(db);
        if (value == null) {
          throw new NoSuchObjectException("no database " + db);
        }
        return value;
      }
      case "get_all_databases":
        calls.add("get_all_databases");
        return allDatabases;
      case "get_databases": {
        String pattern = (String) args[0];
        calls.add("get_databases:" + pattern);
        return allDatabases;
      }
      case "get_all_tables": {
        String db = (String) args[0];
        calls.add("get_all_tables:" + db);
        return tablesByDatabase.getOrDefault(db, Collections.emptyList());
      }
      case "get_tables": {
        String db = (String) args[0];
        String pattern = (String) args[1];
        calls.add("get_tables:" + db + ":" + pattern);
        return tablesByDatabase.getOrDefault(db, Collections.emptyList());
      }
      case "get_table": {
        String db = (String) args[0];
        String tbl = (String) args[1];
        calls.add("get_table:" + db + ":" + tbl);
        Table value = tables.get(db + "." + tbl);
        if (value == null) {
          throw new NoSuchObjectException("no table " + db + "." + tbl);
        }
        // A copy, never the stored object: a real metastore is on the other side of a Thrift
        // wire, so every reply is freshly deserialized. Iceberg's HiveTableOperations mutates
        // the Table it read (new metadata_location) BEFORE calling alter_table, so handing out
        // the stored object would apply that write here even when alter_table is refused - and
        // the commit-status check Iceberg runs after a refusal would read its own uncommitted
        // write back and report the failed commit as a success.
        return new Table(value);
      }
      case "get_table_objects_by_name": {
        String db = (String) args[0];
        @SuppressWarnings("unchecked")
        List<String> tableNames = (List<String>) args[1];
        calls.add("get_table_objects_by_name:" + db + ":" + tableNames);
        List<Table> result = new java.util.ArrayList<>();
        for (String t : tableNames) {
          Table value = tables.get(db + "." + t);
          if (value != null) {
            result.add(new Table(value));
          }
        }
        return result;
      }
      case "create_database": {
        Database database = (Database) args[0];
        calls.add("create_database:" + database.getName());
        databases.put(database.getName(), database);
        return null;
      }
      case "drop_database": {
        String db = (String) args[0];
        boolean deleteData = (Boolean) args[1];
        boolean cascade = (Boolean) args[2];
        calls.add("drop_database:" + db + ":" + deleteData + ":" + cascade);
        if (!databases.containsKey(db)) {
          throw new NoSuchObjectException("no database " + db);
        }
        databases.remove(db);
        return null;
      }
      case "alter_database": {
        String db = (String) args[0];
        Database database = (Database) args[1];
        calls.add("alter_database:" + db + ":" + database.getName());
        databases.put(db, database);
        return null;
      }
      case "create_table": {
        Table table = (Table) args[0];
        calls.add("create_table:" + table.getDbName() + "." + table.getTableName());
        tables.put(table.getDbName() + "." + table.getTableName(), table);
        return null;
      }
      case "drop_table": {
        String db = (String) args[0];
        String tbl = (String) args[1];
        calls.add("drop_table:" + db + "." + tbl);
        tables.remove(db + "." + tbl);
        return null;
      }
      case "alter_table_with_environment_context": {
        String db = (String) args[0];
        String tbl = (String) args[1];
        Table table = (Table) args[2];
        calls.add("alter_table:" + db + "." + tbl + ":table=" + table.getDbName());
        if (alterTableFailures.contains(tbl)) {
          calls.add("alter_table_injected_failure:" + db + "." + tbl);
          throw new org.apache.hadoop.hive.metastore.api.MetaException(
              "injected alter_table failure for " + db + "." + tbl);
        }
        tables.put(db + "." + tbl, table);
        return null;
      }
      case "lock": {
        calls.add("lock");
        return new LockResponse(LOCK_ID, LockState.ACQUIRED);
      }
      case "check_lock": {
        CheckLockRequest request = (CheckLockRequest) args[0];
        calls.add("check_lock:" + request.getLockid());
        return new LockResponse(LOCK_ID, LockState.ACQUIRED);
      }
      case "unlock": {
        UnlockRequest request = (UnlockRequest) args[0];
        calls.add("unlock:" + request.getLockid());
        return null;
      }
      case "show_locks": {
        calls.add("show_locks");
        lastShowLocksResponse = new ShowLocksResponse();
        return lastShowLocksResponse;
      }
      case "heartbeat": {
        HeartbeatRequest request = (HeartbeatRequest) args[0];
        calls.add("heartbeat:" + request.getLockid() + ":" + request.getTxnid());
        return null;
      }
      default:
        throw new UnsupportedOperationException("unexpected Iface call: " + name
            + " args=" + Arrays.toString(args));
    }
  }
}
