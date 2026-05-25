package io.github.mmalykhin.hmsproxy.restcatalog;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

/** In-memory fake of ThriftHiveMetastore.Iface for unit tests in the restcatalog package. */
final class RecordingThriftIface {
  final List<String> calls = new ArrayList<>();
  final Map<String, Database> databases = new HashMap<>();
  final Map<String, Table> tables = new HashMap<>();
  final Map<String, List<String>> tablesByDatabase = new HashMap<>();
  List<String> allDatabases = Collections.emptyList();
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
        return value;
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
            result.add(value);
          }
        }
        return result;
      }
      default:
        throw new UnsupportedOperationException("unexpected Iface call: " + name
            + " args=" + Arrays.toString(args));
    }
  }
}
