package io.github.mmalykhin.hmsproxy.routing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

final class NamespaceExternalizer {
  private NamespaceExternalizer() {}

  static Object externalize(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    if (value == null) {
      return null;
    }
    if (NamespaceTranslator.isScalar(value)) {
      return value;
    }
    if (value instanceof List<?> list) {
      List<Object> transformed = new ArrayList<>(list.size());
      for (Object element : list) {
        transformed.add(externalize(element, namespace, preserveBackendCatalogName));
      }
      return transformed;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> transformed = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        transformed.put(entry.getKey(), externalize(entry.getValue(), namespace, preserveBackendCatalogName));
      }
      return transformed;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      // One defensive copy per subtree root: the backend result must not be mutated in place,
      // but everything below the copy is private to it and is rewritten in place.
      TBase<?, ?> copy = thriftValue.deepCopy();
      return rewriteInPlace(copy, namespace, preserveBackendCatalogName);
    }
    return value;
  }

  private static Object rewriteInPlace(
      TBase<?, ?> thriftValue,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    rewriteFields(thriftValue, namespace, preserveBackendCatalogName);
    return applyNamespace(thriftValue, namespace, preserveBackendCatalogName);
  }

  /** Rewrites an already-copied nested value in place and returns it unchanged (identity preserved). */
  private static Object rewriteNested(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    if (value == null || NamespaceTranslator.isScalar(value)) {
      return value;
    }
    if (value instanceof List<?> list) {
      for (Object element : list) {
        rewriteNested(element, namespace, preserveBackendCatalogName);
      }
      return value;
    }
    if (value instanceof Map<?, ?> map) {
      for (Object element : map.values()) {
        rewriteNested(element, namespace, preserveBackendCatalogName);
      }
      return value;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      rewriteInPlace(thriftValue, namespace, preserveBackendCatalogName);
    }
    return value;
  }

  private static void rewriteFields(
      TBase<?, ?> thriftValue,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    for (TFieldIdEnum fieldId : ThriftReflectionCache.fieldIds(thriftValue)) {
      Object fieldValue = ThriftReflectionCache.getField(thriftValue, fieldId);
      Object transformed = transformField(fieldId, fieldValue, namespace, preserveBackendCatalogName);
      if (transformed != fieldValue) {
        ThriftReflectionCache.setField(thriftValue, fieldId, transformed);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Object transformField(
      TFieldIdEnum fieldId,
      Object fieldValue,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    String normalizedFieldName = NamespaceTranslator.normalizeFieldName(fieldId.getFieldName());
    if (fieldValue instanceof String stringValue) {
      if (NamespaceTranslator.looksLikeDbNameField(fieldId.getFieldName())
          && !normalizedFieldName.equals("dbname")) {
        return transformDbName(stringValue, namespace);
      }
      if (NamespaceTranslator.looksLikeFullTableNameField(fieldId.getFieldName())
          && !normalizedFieldName.equals("fulltablename")) {
        return transformFullTableName(stringValue, namespace);
      }
    }
    if (fieldValue instanceof List<?> listValue
        && NamespaceTranslator.looksLikeFullTableNamesField(fieldId.getFieldName())
        && !normalizedFieldName.equals("fulltablenames")) {
      return transformFullTableNames((List<String>) listValue, namespace);
    }
    return rewriteNested(fieldValue, namespace, preserveBackendCatalogName);
  }

  private static Object applyNamespace(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    if (value instanceof Database database) {
      database.setName(namespace.externalDbName());
      if (!preserveBackendCatalogName) {
        database.setCatalogName(namespace.catalogName());
      }
      return database;
    }
    String originalFullTableName = NamespaceTranslator.readFullTableNameProperty(value);
    List<String> originalFullTableNames = NamespaceTranslator.readFullTableNamesProperty(value);
    ThriftReflectionCache.invokeStringSetter(value, "setDbName", namespace.externalDbName());
    ThriftReflectionCache.invokeStringSetter(value, "setDbname", namespace.externalDbName());
    ThriftReflectionCache.invokeStringSetter(value, "setDb_name", namespace.externalDbName());
    ThriftReflectionCache.invokeStringSetter(value, "setFullTableName",
        transformFullTableName(originalFullTableName, namespace));
    rewriteFullTableNames(value, transformFullTableNames(originalFullTableNames, namespace));
    if (!preserveBackendCatalogName) {
      ThriftReflectionCache.invokeStringSetter(value, "setCatName", namespace.catalogName());
      ThriftReflectionCache.invokeStringSetter(value, "setCatalogName", namespace.catalogName());
    }
    return value;
  }

  static String transformDbName(String dbName, CatalogRouter.ResolvedNamespace namespace) {
    if (dbName == null) {
      return null;
    }
    return NamespaceTranslator.matchesExternalDatabaseAlias(dbName, namespace.backendDbName())
        ? namespace.externalDbName()
        : dbName;
  }

  static String transformFullTableName(String fullTableName, CatalogRouter.ResolvedNamespace namespace) {
    if (fullTableName == null || fullTableName.isBlank()) {
      return fullTableName;
    }
    int separator = fullTableName.lastIndexOf('.');
    if (separator <= 0 || separator + 1 >= fullTableName.length()) {
      return fullTableName;
    }
    String dbName = fullTableName.substring(0, separator);
    String tableName = fullTableName.substring(separator + 1);
    String rewrittenDbName = NamespaceTranslator.matchesExternalDatabaseAlias(dbName, namespace.backendDbName())
        ? namespace.externalDbName()
        : dbName;
    return rewrittenDbName + "." + tableName;
  }

  private static List<String> transformFullTableNames(
      List<String> fullTableNames,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    if (fullTableNames == null) {
      return null;
    }
    if (fullTableNames.isEmpty()) {
      return fullTableNames;
    }
    List<String> transformed = new ArrayList<>(fullTableNames.size());
    for (String fullTableName : fullTableNames) {
      transformed.add(transformFullTableName(fullTableName, namespace));
    }
    return transformed;
  }

  private static void rewriteFullTableNames(Object target, List<String> values) {
    if (values == null) {
      return;
    }
    ThriftReflectionCache.invokeStringListSetter(target, "setFullTableNames", values);
  }
}
