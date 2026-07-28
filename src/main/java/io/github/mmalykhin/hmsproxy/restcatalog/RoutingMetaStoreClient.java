package io.github.mmalykhin.hmsproxy.restcatalog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;

/**
 * Bridges Iceberg's HiveCatalog (which requires IMetaStoreClient) to the proxy's
 * ThriftHiveMetastore.Iface. Read paths, table writes (create/drop/alter), namespace DDL
 * (create/drop/alter database) and the commit-lock RPCs Iceberg's write path needs are
 * implemented; everything else throws UnsupportedOperationException. This class only wires
 * the client through: it does not restrict writes to the default catalog, that gate is a
 * separate concern.
 */
public final class RoutingMetaStoreClient {
  private RoutingMetaStoreClient() {
  }

  public static IMetaStoreClient create(ThriftHiveMetastore.Iface delegate) {
    return create(delegate, null);
  }

  public static IMetaStoreClient create(
      ThriftHiveMetastore.Iface delegate, CatalogNameTranslation translation) {
    Objects.requireNonNull(delegate, "delegate");
    return (IMetaStoreClient) Proxy.newProxyInstance(
        IMetaStoreClient.class.getClassLoader(),
        new Class<?>[]{IMetaStoreClient.class},
        new RoutingInvocationHandler(delegate, translation));
  }

  private static final class RoutingInvocationHandler implements InvocationHandler {
    private final ThriftHiveMetastore.Iface delegate;
    private final CatalogNameTranslation translation;

    RoutingInvocationHandler(
        ThriftHiveMetastore.Iface delegate, CatalogNameTranslation translation) {
      this.delegate = delegate;
      this.translation = translation;
    }

    private String db(String internal) {
      return translation == null ? internal : translation.toExternal(internal);
    }

    private Database rewriteDatabase(Database result) {
      if (translation == null || result == null) {
        return result;
      }
      String internalName = translation.fromExternalOrNull(result.getName());
      if (internalName == null) {
        return result;
      }
      Database copy = new Database(result);
      copy.setName(internalName);
      return copy;
    }

    private Table rewriteTable(Table result) {
      if (translation == null || result == null) {
        return result;
      }
      String internalName = translation.fromExternalOrNull(result.getDbName());
      if (internalName == null) {
        return result;
      }
      Table copy = new Table(result);
      copy.setDbName(internalName);
      return copy;
    }

    /** Translates a caller-supplied Table's dbName to the external name, on a copy. */
    private Table translateDbName(Table table) {
      if (translation == null || table == null) {
        return table;
      }
      Table copy = new Table(table);
      copy.setDbName(db(table.getDbName()));
      return copy;
    }

    /** Translates a caller-supplied Database's own name to the external name, on a copy. */
    private Database translateDatabaseName(Database database) {
      if (translation == null || database == null) {
        return database;
      }
      Database copy = new Database(database);
      copy.setName(db(database.getName()));
      return copy;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      Class<?>[] paramTypes = method.getParameterTypes();
      switch (name) {
        case "close":
        case "reconnect":
        case "flushCache":
          return null;
        case "isCompatibleWith":
        case "isLocalMetaStore":
          return false;
        case "equals":
          return proxy == args[0];
        case "hashCode":
          return System.identityHashCode(proxy);
        case "toString":
          return "RoutingMetaStoreClient[delegate=" + delegate + "]";
        case "getDatabase":
          return rewriteDatabase(delegate.get_database(db((String) args[0])));
        case "getAllDatabases":
          return translation == null
              ? delegate.get_all_databases()
              : translation.internalNames(delegate.get_all_databases());
        case "getDatabases":
          return translation == null
              ? delegate.get_databases((String) args[0])
              : translation.internalNames(delegate.get_databases(db((String) args[0])));
        case "getAllTables":
          return delegate.get_all_tables(db((String) args[0]));
        case "getTables":
          if (paramTypes.length == 2) {
            return delegate.get_tables(db((String) args[0]), (String) args[1]);
          }
          if (paramTypes.length == 3) {
            return delegate.get_tables_by_type(
                db((String) args[0]), (String) args[1], String.valueOf(args[2]));
          }
          break;
        case "getTable":
          if (paramTypes.length == 2
              && paramTypes[0] == String.class && paramTypes[1] == String.class) {
            return rewriteTable(delegate.get_table(db((String) args[0]), (String) args[1]));
          }
          break;
        case "getTableObjectsByName":
          if (paramTypes.length == 2
              && paramTypes[0] == String.class && paramTypes[1] == List.class) {
            @SuppressWarnings("unchecked")
            List<String> tableNames = (List<String>) args[1];
            List<Table> results = delegate.get_table_objects_by_name(
                db((String) args[0]), tableNames);
            if (translation == null) {
              return results;
            }
            List<Table> rewritten = new ArrayList<>(results.size());
            for (Table result : results) {
              rewritten.add(rewriteTable(result));
            }
            return rewritten;
          }
          break;
        case "tableExists":
          if (paramTypes.length == 2) {
            try {
              delegate.get_table(db((String) args[0]), (String) args[1]);
              return true;
            } catch (NoSuchObjectException e) {
              return false;
            }
          }
          break;
        case "createTable":
          if (paramTypes.length == 1) {
            delegate.create_table(translateDbName((Table) args[0]));
            return null;
          }
          break;
        case "createDatabase":
          if (paramTypes.length == 1) {
            delegate.create_database(translateDatabaseName((Database) args[0]));
            return null;
          }
          break;
        case "dropDatabase":
          if (paramTypes.length == 4) {
            delegate.drop_database(
                db((String) args[0]), (Boolean) args[1], (Boolean) args[3]);
            return null;
          }
          break;
        case "alterDatabase":
          if (paramTypes.length == 2) {
            delegate.alter_database(
                db((String) args[0]), translateDatabaseName((Database) args[1]));
            return null;
          }
          break;
        case "dropTable":
          if (paramTypes.length == 4
              && paramTypes[0] == String.class && paramTypes[1] == String.class
              && paramTypes[2] == boolean.class && paramTypes[3] == boolean.class) {
            boolean deleteData = (Boolean) args[2];
            boolean ignoreUnknownTable = (Boolean) args[3];
            try {
              delegate.drop_table(db((String) args[0]), (String) args[1], deleteData);
            } catch (NoSuchObjectException e) {
              if (!ignoreUnknownTable) {
                throw e;
              }
            }
            return null;
          }
          break;
        case "alter_table_with_environmentContext":
          if (paramTypes.length == 4) {
            delegate.alter_table_with_environment_context(
                db((String) args[0]), (String) args[1],
                translateDbName((Table) args[2]), (EnvironmentContext) args[3]);
            return null;
          }
          break;
        case "lock":
          if (paramTypes.length == 1) {
            // The LockRequest's own database names are resolved downstream by the
            // proxy's LockHandler; translating them here would double-translate.
            return delegate.lock((LockRequest) args[0]);
          }
          break;
        case "checkLock":
          if (paramTypes.length == 1) {
            return delegate.check_lock(new CheckLockRequest((Long) args[0]));
          }
          break;
        case "unlock":
          if (paramTypes.length == 1) {
            delegate.unlock(new UnlockRequest((Long) args[0]));
            return null;
          }
          break;
        case "showLocks":
          if (paramTypes.length == 1) {
            return delegate.show_locks((ShowLocksRequest) args[0]);
          }
          break;
        case "heartbeat":
          if (paramTypes.length == 2) {
            HeartbeatRequest request = new HeartbeatRequest();
            request.setLockid((Long) args[0]);
            request.setTxnid((Long) args[1]);
            delegate.heartbeat(request);
            return null;
          }
          break;
        default:
          break;
      }
      throw new UnsupportedOperationException(
          "HMS proxy REST gateway does not support IMetaStoreClient." + name
              + "(" + parameterSignature(paramTypes) + ")");
    }

    private static String parameterSignature(Class<?>[] paramTypes) {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < paramTypes.length; i++) {
        if (i > 0) {
          builder.append(',');
        }
        builder.append(paramTypes[i].getSimpleName());
      }
      return builder.toString();
    }
  }
}
