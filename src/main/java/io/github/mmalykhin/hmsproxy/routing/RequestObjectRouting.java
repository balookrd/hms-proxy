package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import java.lang.reflect.Method;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;

/**
 * Shared routing, access-validation, and Hive 4 unsupported-method fallback helpers for Thrift
 * {@code *_req} wrapper handlers.
 */
final class RequestObjectRouting {
  private RequestObjectRouting() {
  }

  record ResolvedRequest(
      Object request,
      String catName,
      String dbName,
      String tableName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    CatalogBackend backend() {
      return namespace.backend();
    }
  }

  @FunctionalInterface
  interface FallbackCall {
    Object run() throws Throwable;
  }

  static String readCatalogName(Object request) {
    return ThriftReflectionCache.readString(
        request, "catName", "getCatName", "getCatalogName", "getCat_name", "getCatalog");
  }

  static String readDbName(Object request) {
    String dbName = ThriftReflectionCache.readString(request, "dbName", "getDbName", "getDb_name", "getDb");
    return dbName != null ? dbName : NamespaceTranslator.extractDbName(request);
  }

  static String readTableName(Object request) {
    String tblName = ThriftReflectionCache.readString(request, "tblName", "getTblName");
    return tblName != null ? tblName : RoutingSupport.extractTableName(request);
  }

  static ResolvedRequest resolve(
      RoutingSupport support,
      String methodName,
      Object request,
      String catName,
      String dbName,
      String tableName
  ) throws MetaException {
    if (dbName == null) {
      throw new MetaException(methodName + " requires a target database");
    }
    CatalogRouter.ResolvedNamespace namespace = support.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(methodName, catName, dbName, namespace);
    return new ResolvedRequest(request, catName, dbName, tableName, namespace);
  }

  static ResolvedRequest resolveReadByMethodName(
      RoutingSupport support,
      String methodName,
      Object request
  ) throws TException {
    String catName = readCatalogName(request);
    String dbName = readDbName(request);
    String tableName = readTableName(request);
    ResolvedRequest resolved = resolve(support, methodName, request, catName, dbName, tableName);
    support.validateExposedDatabaseAccess(methodName, resolved.namespace());
    support.validateExposedTableAccess(methodName, resolved.namespace(), tableName);
    return resolved;
  }

  static ResolvedRequest resolveRead(
      RoutingSupport support,
      Method method,
      Object request
  ) throws TException {
    String catName = readCatalogName(request);
    String dbName = readDbName(request);
    String tableName = readTableName(request);
    ResolvedRequest resolved = resolve(support, method.getName(), request, catName, dbName, tableName);
    support.validateExposedDatabaseAccess(method, resolved.namespace());
    if (tableName != null && !tableName.isBlank()) {
      support.validateExposedTableAccess(method, resolved.namespace(), tableName);
    }
    return resolved;
  }

  static ResolvedRequest resolveWrite(
      RoutingSupport support,
      String methodName,
      Object request
  ) throws MetaException {
    String catName = readCatalogName(request);
    String dbName = readDbName(request);
    String tableName = readTableName(request);
    ResolvedRequest resolved = resolve(support, methodName, request, catName, dbName, tableName);
    support.validateCatalogAccess(resolved.backend(), methodName, resolved.namespace().backendDbName());
    return resolved;
  }

  static Object invokeWithUnsupportedFallback(
      RoutingSupport support,
      CatalogBackend backend,
      String methodName,
      Object routedRequest,
      FallbackCall fallback
  ) throws Throwable {
    try {
      return support.invokeBackendNamed(backend, methodName, routedRequest);
    } catch (Throwable cause) {
      if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
        return fallback.run();
      }
      throw cause;
    }
  }

  static Method findIfaceMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing expected method: " + name, e);
    }
  }
}
