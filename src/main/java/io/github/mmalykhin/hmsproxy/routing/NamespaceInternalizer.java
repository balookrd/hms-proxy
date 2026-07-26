package io.github.mmalykhin.hmsproxy.routing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

final class NamespaceInternalizer {
  private NamespaceInternalizer() {}

  static Object internalize(
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
        transformed.add(internalize(element, namespace, preserveBackendCatalogName));
      }
      return transformed;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> transformed = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        transformed.put(entry.getKey(), internalize(entry.getValue(), namespace, preserveBackendCatalogName));
      }
      return transformed;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      // One defensive copy per subtree root: callers (thrift processor arguments) must never observe
      // a mutated input, but everything below the copy is private to it and is rewritten in place.
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
      String originalName = database.getName();
      database.setName(namespace.backendDbName());
      database.setCatalogName(
          internalCatalogName(database.getCatalogName(), originalName, namespace, preserveBackendCatalogName));
      return database;
    }
    String originalDbName = NamespaceTranslator.readDbNameProperty(value);
    if (HivePlaceholderNamespace.isPlaceholderDbName(originalDbName)) {
      // Hive's INSERT ... VALUES placeholder belongs to no database. Rewriting it to the resolved
      // backend database would lock a fictitious table of a real database instead of the harmless
      // placeholder name every unproxied Hive sends.
      return value;
    }
    String originalFullTableName = NamespaceTranslator.readFullTableNameProperty(value);
    List<String> originalFullTableNames = NamespaceTranslator.readFullTableNamesProperty(value);
    ThriftReflectionCache.invokeStringSetter(value, "setCatName",
        internalCatalogNameForField(value, "catName",
            ThriftReflectionCache.readString(value, "getCatName"),
            originalDbName, namespace, preserveBackendCatalogName));
    ThriftReflectionCache.invokeStringSetter(value, "setCatalogName",
        internalCatalogNameForField(value, "catalogName",
            ThriftReflectionCache.readString(value, "getCatalogName"),
            originalDbName, namespace, preserveBackendCatalogName));
    ThriftReflectionCache.invokeStringSetter(value, "setDbName", namespace.backendDbName());
    ThriftReflectionCache.invokeStringSetter(value, "setDbname", namespace.backendDbName());
    ThriftReflectionCache.invokeStringSetter(value, "setDb_name", namespace.backendDbName());
    ThriftReflectionCache.invokeStringSetter(value, "setFullTableName",
        transformFullTableName(originalFullTableName, namespace));
    rewriteFullTableNames(value, transformFullTableNames(originalFullTableNames, namespace));
    return value;
  }

  static String transformDbName(String dbName, CatalogRouter.ResolvedNamespace namespace) {
    if (dbName == null) {
      return null;
    }
    return NamespaceTranslator.matchesExternalDatabaseAlias(dbName, namespace.externalDbName())
        ? namespace.backendDbName()
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
    String rewrittenDbName = NamespaceTranslator.matchesExternalDatabaseAlias(dbName, namespace.externalDbName())
        ? namespace.backendDbName()
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

  static String internalCatalogName(
      String requestCatalogName,
      String originalDbName,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return internalCatalogNameCore(
        requestCatalogName,
        originalDbName,
        namespace.catalogName(),
        namespace.externalDbName(),
        namespace.backendDbName(),
        preserveBackendCatalogName);
  }

  static String internalCatalogName(String requestCatalogName, String proxyCatalogName) {
    return internalCatalogNameCore(requestCatalogName, null, proxyCatalogName, null, null, false);
  }

  private static String internalCatalogNameCore(
      String requestCatalogName,
      String originalDbName,
      String proxyCatalogName,
      String externalDbName,
      String backendDbName,
      boolean preserveBackendCatalogName
  ) {
    if (requestCatalogName == null || requestCatalogName.isBlank()) {
      return null;
    }
    if (requestCatalogName.equals(proxyCatalogName)) {
      return null;
    }
    if (externalDbName != null && externalDbName.equals(backendDbName)) {
      return requestCatalogName;
    }
    if (NamespaceTranslator.matchesExternalDatabaseAlias(originalDbName, externalDbName)) {
      return preserveBackendCatalogName ? requestCatalogName : null;
    }
    return requestCatalogName;
  }

  private static String internalCatalogNameForField(
      Object target,
      String fieldName,
      String requestCatalogName,
      String originalDbName,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    String translated = internalCatalogName(requestCatalogName, originalDbName, namespace, preserveBackendCatalogName);
    if (translated == null && target instanceof TBase<?, ?> thriftValue
        && ThriftReflectionCache.hasRequiredField(thriftValue, fieldName)) {
      return requestCatalogName;
    }
    return translated;
  }

  private static void rewriteFullTableNames(Object target, List<String> values) {
    if (values == null) {
      return;
    }
    ThriftReflectionCache.invokeStringListSetter(target, "setFullTableNames", values);
  }
}
