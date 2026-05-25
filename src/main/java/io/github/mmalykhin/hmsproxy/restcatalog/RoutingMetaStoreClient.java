package io.github.mmalykhin.hmsproxy.restcatalog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.List;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

/**
 * Bridges Iceberg's HiveCatalog (which requires IMetaStoreClient) to the proxy's
 * ThriftHiveMetastore.Iface. Only methods used by HiveCatalog's read paths are
 * implemented; write paths and unsupported methods throw UnsupportedOperationException
 * until Phase 5 brings write support online.
 */
public final class RoutingMetaStoreClient {
  private RoutingMetaStoreClient() {
  }

  public static IMetaStoreClient create(ThriftHiveMetastore.Iface delegate) {
    Objects.requireNonNull(delegate, "delegate");
    return (IMetaStoreClient) Proxy.newProxyInstance(
        IMetaStoreClient.class.getClassLoader(),
        new Class<?>[]{IMetaStoreClient.class},
        new RoutingInvocationHandler(delegate));
  }

  private static final class RoutingInvocationHandler implements InvocationHandler {
    private final ThriftHiveMetastore.Iface delegate;

    RoutingInvocationHandler(ThriftHiveMetastore.Iface delegate) {
      this.delegate = delegate;
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
          return delegate.get_database((String) args[0]);
        case "getAllDatabases":
          return delegate.get_all_databases();
        case "getDatabases":
          return delegate.get_databases((String) args[0]);
        case "getAllTables":
          return delegate.get_all_tables((String) args[0]);
        case "getTables":
          if (paramTypes.length == 2) {
            return delegate.get_tables((String) args[0], (String) args[1]);
          }
          if (paramTypes.length == 3) {
            return delegate.get_tables_by_type(
                (String) args[0], (String) args[1], String.valueOf(args[2]));
          }
          break;
        case "getTable":
          if (paramTypes.length == 2
              && paramTypes[0] == String.class && paramTypes[1] == String.class) {
            return delegate.get_table((String) args[0], (String) args[1]);
          }
          break;
        case "getTableObjectsByName":
          if (paramTypes.length == 2
              && paramTypes[0] == String.class && paramTypes[1] == List.class) {
            @SuppressWarnings("unchecked")
            List<String> tableNames = (List<String>) args[1];
            return delegate.get_table_objects_by_name((String) args[0], tableNames);
          }
          break;
        case "tableExists":
          if (paramTypes.length == 2) {
            try {
              delegate.get_table((String) args[0], (String) args[1]);
              return true;
            } catch (NoSuchObjectException e) {
              return false;
            }
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
