package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;

final class TableConstraintsHandler implements SpecialCaseHandler {
  private static final Method GET_PRIMARY_KEYS = findMethod("get_primary_keys", PrimaryKeysRequest.class);
  private static final Method GET_FOREIGN_KEYS = findMethod("get_foreign_keys", ForeignKeysRequest.class);
  private static final Method GET_UNIQUE_CONSTRAINTS = findMethod("get_unique_constraints", UniqueConstraintsRequest.class);
  private static final Method GET_NOT_NULL_CONSTRAINTS = findMethod("get_not_null_constraints", NotNullConstraintsRequest.class);
  private static final Method GET_DEFAULT_CONSTRAINTS = findMethod("get_default_constraints", DefaultConstraintsRequest.class);
  private static final Method GET_CHECK_CONSTRAINTS = findMethod("get_check_constraints", CheckConstraintsRequest.class);

  private final RoutingSupport support;

  TableConstraintsHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String dbName = extractDbName(request);
    if (dbName == null || dbName.isBlank()) {
      throw new MetaException(method.getName() + " requires a target database");
    }

    String catName = extractCatName(request);
    CatalogRouter.ResolvedNamespace namespace = support.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(method.getName(), catName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, method.getName(), namespace.backendDbName());

    internalizeRequest(request, namespace);

    Object response;
    if ("get_all_table_constraints".equals(method.getName())) {
      try {
        response = support.invokeBackendNamed(backend, "get_all_table_constraints", request);
      } catch (Throwable cause) {
        if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
          response = fallbackToLegacy(backend, request, namespace.backendDbName());
        } else {
          throw cause;
        }
      }
    } else {
      response = support.invokeDirect(backend, method, args);
    }

    externalizeResponse(method.getName(), response, namespace);
    return response;
  }

  private String extractDbName(Object request) {
    String dbName = ThriftReflectionCache.readString(request, "getDbName", "getDb_name", "getDbname");
    if (dbName != null && !dbName.isBlank()) {
      return dbName;
    }
    String foreignDb = ThriftReflectionCache.readString(request, "getForeign_db_name", "getForeignDbName");
    if (foreignDb != null && !foreignDb.isBlank()) {
      return foreignDb;
    }
    return ThriftReflectionCache.readString(request, "getParent_db_name", "getParentDbName");
  }

  private String extractCatName(Object request) {
    String catName = ThriftReflectionCache.readString(request, "getCatName", "getCatalogName");
    if (catName != null && !catName.isBlank()) {
      return catName;
    }
    String foreignCat = ThriftReflectionCache.readString(request, "getForeign_catName");
    if (foreignCat != null && !foreignCat.isBlank()) {
      return foreignCat;
    }
    return ThriftReflectionCache.readString(request, "getParent_catName");
  }

  private void internalizeRequest(Object request, CatalogRouter.ResolvedNamespace namespace) {
    ThriftReflectionCache.invokeStringSetter(request, "setDbName", namespace.backendDbName());
    ThriftReflectionCache.invokeStringSetter(request, "setDb_name", namespace.backendDbName());
    ThriftReflectionCache.invokeStringSetter(request, "setDbname", namespace.backendDbName());
    String foreignDb = ThriftReflectionCache.readString(request, "getForeign_db_name");
    if (foreignDb != null && matchesDb(foreignDb, namespace)) {
      ThriftReflectionCache.invokeStringSetter(request, "setForeign_db_name", namespace.backendDbName());
    }
    String parentDb = ThriftReflectionCache.readString(request, "getParent_db_name");
    if (parentDb != null && matchesDb(parentDb, namespace)) {
      ThriftReflectionCache.invokeStringSetter(request, "setParent_db_name", namespace.backendDbName());
    }
  }

  private static boolean matchesDb(String requestDb, CatalogRouter.ResolvedNamespace namespace) {
    return requestDb.equalsIgnoreCase(namespace.externalDbName())
        || requestDb.equalsIgnoreCase(namespace.backendDbName());
  }

  private void externalizeResponse(String methodName, Object response, CatalogRouter.ResolvedNamespace namespace) {
    if (response == null) {
      return;
    }
    String clientDbName = namespace.externalDbName();
    String clientCatName = namespace.catalogName();
    switch (methodName) {
      case "get_all_table_constraints" -> externalizeAllConstraintsResponse(response, namespace);
      case "get_primary_keys" -> {
        Object pks = ThriftReflectionCache.invokeGetter(response, "getPrimaryKeys");
        externalizeList(pks, "setTable_db", clientDbName);
        externalizeList(pks, "setCatName", clientCatName);
      }
      case "get_foreign_keys" -> {
        Object fks = ThriftReflectionCache.invokeGetter(response, "getForeignKeys");
        externalizeForeignKeys(fks, namespace);
      }
      case "get_unique_constraints" -> {
        Object uqs = ThriftReflectionCache.invokeGetter(response, "getUniqueConstraints");
        externalizeList(uqs, "setTable_db", clientDbName);
        externalizeList(uqs, "setCatName", clientCatName);
      }
      case "get_not_null_constraints" -> {
        Object nns = ThriftReflectionCache.invokeGetter(response, "getNotNullConstraints");
        externalizeList(nns, "setTable_db", clientDbName);
        externalizeList(nns, "setCatName", clientCatName);
      }
      case "get_default_constraints" -> {
        Object dfs = ThriftReflectionCache.invokeGetter(response, "getDefaultConstraints");
        externalizeList(dfs, "setTable_db", clientDbName);
        externalizeList(dfs, "setCatName", clientCatName);
      }
      case "get_check_constraints" -> {
        Object cks = ThriftReflectionCache.invokeGetter(response, "getCheckConstraints");
        externalizeList(cks, "setTable_db", clientDbName);
        externalizeList(cks, "setCatName", clientCatName);
      }
      default -> {
      }
    }
  }

  private Object fallbackToLegacy(CatalogBackend backend, Object request, String backendDb) throws Throwable {
    String tblName = (String) ThriftReflectionCache.invokeGetter(request, "getTblName");

    PrimaryKeysResponse pkResp = (PrimaryKeysResponse) support.invokeDirect(backend, GET_PRIMARY_KEYS,
        new Object[]{new PrimaryKeysRequest(backendDb, tblName)});
    List<SQLPrimaryKey> pks = pkResp != null && pkResp.getPrimaryKeys() != null ? pkResp.getPrimaryKeys() : List.of();

    ForeignKeysResponse fkResp = (ForeignKeysResponse) support.invokeDirect(backend, GET_FOREIGN_KEYS,
        new Object[]{new ForeignKeysRequest(null, null, backendDb, tblName)});
    List<SQLForeignKey> fks = fkResp != null && fkResp.getForeignKeys() != null ? fkResp.getForeignKeys() : List.of();

    UniqueConstraintsResponse uqResp = (UniqueConstraintsResponse) support.invokeDirect(backend, GET_UNIQUE_CONSTRAINTS,
        new Object[]{new UniqueConstraintsRequest(null, backendDb, tblName)});
    List<SQLUniqueConstraint> uqs = uqResp != null && uqResp.getUniqueConstraints() != null ? uqResp.getUniqueConstraints() : List.of();

    NotNullConstraintsResponse nnResp = (NotNullConstraintsResponse) support.invokeDirect(backend, GET_NOT_NULL_CONSTRAINTS,
        new Object[]{new NotNullConstraintsRequest(null, backendDb, tblName)});
    List<SQLNotNullConstraint> nns = nnResp != null && nnResp.getNotNullConstraints() != null ? nnResp.getNotNullConstraints() : List.of();

    DefaultConstraintsResponse dfResp = (DefaultConstraintsResponse) support.invokeDirect(backend, GET_DEFAULT_CONSTRAINTS,
        new Object[]{new DefaultConstraintsRequest(null, backendDb, tblName)});
    List<SQLDefaultConstraint> dfs = dfResp != null && dfResp.getDefaultConstraints() != null ? dfResp.getDefaultConstraints() : List.of();

    CheckConstraintsResponse ckResp = (CheckConstraintsResponse) support.invokeDirect(backend, GET_CHECK_CONSTRAINTS,
        new Object[]{new CheckConstraintsRequest(null, backendDb, tblName)});
    List<SQLCheckConstraint> cks = ckResp != null && ckResp.getCheckConstraints() != null ? ckResp.getCheckConstraints() : List.of();

    ClassLoader cl = request.getClass().getClassLoader();
    Class<?> allConstraintsClass = Class.forName("org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints", true, cl);
    Object allConstraints = allConstraintsClass.getConstructor().newInstance();

    setListField(allConstraints, "setPrimaryKeys", pks, cl);
    setListField(allConstraints, "setForeignKeys", fks, cl);
    setListField(allConstraints, "setUniqueConstraints", uqs, cl);
    setListField(allConstraints, "setNotNullConstraints", nns, cl);
    setListField(allConstraints, "setDefaultConstraints", dfs, cl);
    setListField(allConstraints, "setCheckConstraints", cks, cl);

    Class<?> responseClass = Class.forName("org.apache.hadoop.hive.metastore.api.AllTableConstraintsResponse", true, cl);
    Constructor<?> respCtor = responseClass.getConstructor(allConstraintsClass);
    return respCtor.newInstance(allConstraints);
  }

  private void setListField(Object target, String setterName, List<?> values, ClassLoader targetCl) {
    if (values == null) {
      return;
    }
    try {
      List<?> converted = (List<?>) ThriftValueConverter.convertDynamicValue(values, targetCl);
      target.getClass().getMethod(setterName, List.class).invoke(target, converted);
    } catch (Throwable ignored) {
    }
  }

  private void externalizeAllConstraintsResponse(Object response, CatalogRouter.ResolvedNamespace namespace) {
    if (response == null) {
      return;
    }
    try {
      Object allConstraints = ThriftReflectionCache.invokeGetter(response, "getAllTableConstraints");
      if (allConstraints == null) {
        return;
      }
      String clientDbName = namespace.externalDbName();
      externalizeList(ThriftReflectionCache.invokeGetter(allConstraints, "getPrimaryKeys"), "setTable_db", clientDbName);
      externalizeForeignKeys(ThriftReflectionCache.invokeGetter(allConstraints, "getForeignKeys"), namespace);
      externalizeList(ThriftReflectionCache.invokeGetter(allConstraints, "getUniqueConstraints"), "setTable_db", clientDbName);
      externalizeList(ThriftReflectionCache.invokeGetter(allConstraints, "getNotNullConstraints"), "setTable_db", clientDbName);
      externalizeList(ThriftReflectionCache.invokeGetter(allConstraints, "getDefaultConstraints"), "setTable_db", clientDbName);
      externalizeList(ThriftReflectionCache.invokeGetter(allConstraints, "getCheckConstraints"), "setTable_db", clientDbName);
    } catch (Throwable ignored) {
    }
  }

  private void externalizeForeignKeys(Object listObj, CatalogRouter.ResolvedNamespace namespace) {
    if (listObj instanceof List<?> list) {
      for (Object item : list) {
        if (item != null) {
          ThriftReflectionCache.invokeStringSetter(item, "setFktable_db", namespace.externalDbName());
          String pkDb = ThriftReflectionCache.readString(item, "getPktable_db");
          if (pkDb != null && !pkDb.isBlank()) {
            String externalPkDb = support.router.externalDatabaseName(namespace.catalogName(), pkDb);
            ThriftReflectionCache.invokeStringSetter(item, "setPktable_db", externalPkDb);
          }
        }
      }
    }
  }

  private void externalizeList(Object listObj, String setterName, String clientValue) {
    if (listObj instanceof List<?> list) {
      for (Object item : list) {
        if (item != null) {
          ThriftReflectionCache.invokeStringSetter(item, setterName, clientValue);
        }
      }
    }
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
