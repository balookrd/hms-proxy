package io.github.mmalykhin.hmsproxy;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

final class NamespaceTranslator {
  private enum Direction {
    EXTERNALIZE,
    INTERNALIZE
  }

  private NamespaceTranslator() {
  }

  static Object externalizeResult(Object value, CatalogRouter.ResolvedNamespace namespace) {
    return externalizeResult(value, namespace, false);
  }

  static Object externalizeResult(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return transform(value, namespace, Direction.EXTERNALIZE, preserveBackendCatalogName);
  }

  static Object internalizeArgument(Object value, CatalogRouter.ResolvedNamespace namespace) {
    return transform(value, namespace, Direction.INTERNALIZE, false);
  }

  static String internalizeStringArgument(String value, CatalogRouter.ResolvedNamespace namespace) {
    if (value == null) {
      return null;
    }
    return matchesExternalDatabaseAlias(value, namespace.externalDbName()) ? namespace.backendDbName() : value;
  }

  static Table externalizeTable(Table table, CatalogRouter.ResolvedNamespace namespace) {
    return externalizeTable(table, namespace, false);
  }

  static Table externalizeTable(
      Table table,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return (Table) externalizeResult(table, namespace, preserveBackendCatalogName);
  }

  static TableMeta externalizeTableMeta(TableMeta tableMeta, CatalogRouter.ResolvedNamespace namespace) {
    return externalizeTableMeta(tableMeta, namespace, false);
  }

  static TableMeta externalizeTableMeta(
      TableMeta tableMeta,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return (TableMeta) externalizeResult(tableMeta, namespace, preserveBackendCatalogName);
  }

  static String extractDbName(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Database database) {
      return blankToNull(database.getName());
    }
    String directDbName = readDbNameProperty(value);
    if (directDbName != null) {
      return directDbName;
    }
    String fullTableName = readFullTableNameProperty(value);
    if (fullTableName != null) {
      return extractDbNameFromFullTableName(fullTableName);
    }
    List<String> fullTableNames = readFullTableNamesProperty(value);
    if (fullTableNames != null) {
      for (String candidate : fullTableNames) {
        String extractedDbName = extractDbNameFromFullTableName(candidate);
        if (extractedDbName != null) {
          return extractedDbName;
        }
      }
    }
    if (value instanceof List<?> list) {
      for (Object element : list) {
        String nestedDbName = extractDbName(element);
        if (nestedDbName != null) {
          return nestedDbName;
        }
      }
      return null;
    }
    if (value instanceof Map<?, ?> map) {
      for (Object element : map.values()) {
        String nestedDbName = extractDbName(element);
        if (nestedDbName != null) {
          return nestedDbName;
        }
      }
      return null;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      for (TFieldIdEnum fieldId : thriftFieldIds(thriftValue)) {
        Object fieldValue = getThriftFieldValue(thriftValue, fieldId);
        String nestedDbName = extractDbNameFromField(fieldId, fieldValue);
        if (nestedDbName == null) {
          nestedDbName = extractDbName(fieldValue);
        }
        if (nestedDbName != null) {
          return nestedDbName;
        }
      }
    }
    return null;
  }

  private static Object transform(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction,
      boolean preserveBackendCatalogName
  ) {
    if (value == null) {
      return null;
    }
    if (isScalar(value)) {
      return value;
    }
    if (value instanceof List<?> list) {
      List<Object> transformed = new ArrayList<>(list.size());
      for (Object element : list) {
        transformed.add(transform(element, namespace, direction, preserveBackendCatalogName));
      }
      return transformed;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> transformed = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        transformed.put(entry.getKey(), transform(entry.getValue(), namespace, direction, preserveBackendCatalogName));
      }
      return transformed;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      TBase<?, ?> copy = thriftValue.deepCopy();
      rewriteNestedFields(copy, namespace, direction, preserveBackendCatalogName);
      return applyNamespace(copy, namespace, direction, preserveBackendCatalogName);
    }
    return value;
  }

  private static void rewriteNestedFields(
      TBase<?, ?> thriftValue,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction,
      boolean preserveBackendCatalogName
  ) {
    for (TFieldIdEnum fieldId : thriftFieldIds(thriftValue)) {
      Object fieldValue = getThriftFieldValue(thriftValue, fieldId);
      Object transformed = transformThriftField(fieldId, fieldValue, namespace, direction, preserveBackendCatalogName);
      if (transformed != fieldValue) {
        setThriftFieldValue(thriftValue, fieldId, transformed);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static Object transformThriftField(
      TFieldIdEnum fieldId,
      Object fieldValue,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction,
      boolean preserveBackendCatalogName
  ) {
    String normalizedFieldName = normalizeFieldName(fieldId.getFieldName());
    if (fieldValue instanceof String stringValue) {
      if (looksLikeDbNameField(fieldId.getFieldName()) && !normalizedFieldName.equals("dbname")) {
        return transformDbNameString(stringValue, namespace, direction);
      }
      if (looksLikeFullTableNameField(fieldId.getFieldName()) && !normalizedFieldName.equals("fulltablename")) {
        return transformFullTableName(stringValue, namespace, direction);
      }
    }
    if (fieldValue instanceof List<?> listValue
        && looksLikeFullTableNamesField(fieldId.getFieldName())
        && !normalizedFieldName.equals("fulltablenames")) {
      return transformFullTableNames((List<String>) listValue, namespace, direction);
    }
    return transform(fieldValue, namespace, direction, preserveBackendCatalogName);
  }

  private static Object applyNamespace(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction,
      boolean preserveBackendCatalogName
  ) {
    if (value instanceof Database database) {
      if (direction == Direction.EXTERNALIZE) {
        database.setName(namespace.externalDbName());
        if (!preserveBackendCatalogName) {
          database.setCatalogName(namespace.catalogName());
        }
      } else {
        String originalName = database.getName();
        database.setName(namespace.backendDbName());
        database.setCatalogName(internalCatalogName(database.getCatalogName(), originalName, namespace));
      }
      return database;
    }
    String originalDbName = readDbNameProperty(value);
    String originalFullTableName = readFullTableNameProperty(value);
    List<String> originalFullTableNames = readFullTableNamesProperty(value);
    if (direction == Direction.EXTERNALIZE) {
      maybeInvoke(value, "setDbName", namespace.externalDbName());
      maybeInvoke(value, "setDbname", namespace.externalDbName());
      maybeInvoke(value, "setDb_name", namespace.externalDbName());
      rewriteFullTableName(value, transformFullTableName(originalFullTableName, namespace, direction));
      rewriteFullTableNames(value, transformFullTableNames(originalFullTableNames, namespace, direction));
      if (!preserveBackendCatalogName) {
        maybeInvoke(value, "setCatName", namespace.catalogName());
        maybeInvoke(value, "setCatalogName", namespace.catalogName());
      }
    } else {
      maybeInvoke(value, "setCatName",
          internalCatalogName(readStringProperty(value, "getCatName"), originalDbName, namespace));
      maybeInvoke(value, "setCatalogName",
          internalCatalogName(readStringProperty(value, "getCatalogName"), originalDbName, namespace));
      maybeInvoke(value, "setDbName", namespace.backendDbName());
      maybeInvoke(value, "setDbname", namespace.backendDbName());
      maybeInvoke(value, "setDb_name", namespace.backendDbName());
      rewriteFullTableName(value, transformFullTableName(originalFullTableName, namespace, direction));
      rewriteFullTableNames(value, transformFullTableNames(originalFullTableNames, namespace, direction));
    }
    return value;
  }

  private static boolean isScalar(Object value) {
    return value instanceof CharSequence
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof Enum<?>;
  }

  @SuppressWarnings("unchecked")
  private static List<TFieldIdEnum> thriftFieldIds(TBase<?, ?> thriftValue) {
    try {
      Field metadataField = thriftValue.getClass().getField("metaDataMap");
      Map<?, ?> metadata = (Map<?, ?>) metadataField.get(null);
      List<TFieldIdEnum> fieldIds = new ArrayList<>();
      for (Object fieldId : metadata.keySet()) {
        TFieldIdEnum typedFieldId = (TFieldIdEnum) fieldId;
        if (isThriftFieldSet(thriftValue, typedFieldId)) {
          fieldIds.add(typedFieldId);
        }
      }
      return fieldIds;
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to inspect thrift metadata for " + thriftValue.getClass().getName(), e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object getThriftFieldValue(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).getFieldValue(fieldId);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void setThriftFieldValue(TBase<?, ?> thriftValue, TFieldIdEnum fieldId, Object value) {
    ((TBase) thriftValue).setFieldValue(fieldId, value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static boolean isThriftFieldSet(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).isSet(fieldId);
  }

  static String internalCatalogName(String requestCatalogName, CatalogRouter.ResolvedNamespace namespace) {
    return internalCatalogName(requestCatalogName, null, namespace);
  }

  static String internalCatalogName(
      String requestCatalogName,
      String originalDbName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    return internalCatalogName(requestCatalogName, originalDbName, namespace.catalogName(), namespace.externalDbName());
  }

  static String internalCatalogName(String requestCatalogName, String proxyCatalogName) {
    return internalCatalogName(requestCatalogName, null, proxyCatalogName, null);
  }

  private static String internalCatalogName(
      String requestCatalogName,
      String originalDbName,
      String proxyCatalogName,
      String externalDbName
  ) {
    if (requestCatalogName == null || requestCatalogName.isBlank()) {
      return null;
    }
    if (requestCatalogName.equals(proxyCatalogName)) {
      return null;
    }
    if (matchesExternalDatabaseAlias(originalDbName, externalDbName)) {
      return null;
    }
    return requestCatalogName;
  }

  private static boolean matchesExternalDatabaseAlias(String originalDbName, String externalDbName) {
    if (originalDbName == null || originalDbName.isBlank() || externalDbName == null || externalDbName.isBlank()) {
      return false;
    }
    String normalizedDbName = normalizeCompatibilityDbName(originalDbName);
    return normalizedDbName.equals(externalDbName) || normalizedDbName.endsWith("." + externalDbName);
  }

  private static String normalizeCompatibilityDbName(String dbName) {
    if (dbName == null || dbName.isBlank()) {
      return dbName;
    }
    int hash = dbName.indexOf('#');
    if (dbName.startsWith("@") && hash > 1 && hash + 1 < dbName.length()) {
      return normalizeCompatibilityDbName(dbName.substring(hash + 1));
    }
    return dbName;
  }

  private static String readDbNameProperty(Object value) {
    return blankToNull(readStringProperty(value, "getDbName", "getDbname", "getDb_name"));
  }

  private static String readFullTableNameProperty(Object value) {
    return blankToNull(readStringProperty(value, "getFullTableName", "getFull_table_name"));
  }

  private static List<String> readFullTableNamesProperty(Object value) {
    List<String> fullTableNames = readStringListProperty(value, "getFullTableNames");
    return fullTableNames != null ? fullTableNames : readStringListProperty(value, "getFull_table_names");
  }

  private static String extractDbNameFromFullTableName(String fullTableName) {
    if (fullTableName == null || fullTableName.isBlank()) {
      return null;
    }
    int separator = fullTableName.lastIndexOf('.');
    if (separator <= 0) {
      return null;
    }
    return blankToNull(fullTableName.substring(0, separator));
  }

  private static String transformFullTableName(
      String fullTableName,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction
  ) {
    if (fullTableName == null || fullTableName.isBlank()) {
      return fullTableName;
    }
    int separator = fullTableName.lastIndexOf('.');
    if (separator <= 0 || separator + 1 >= fullTableName.length()) {
      return fullTableName;
    }

    String dbName = fullTableName.substring(0, separator);
    String tableName = fullTableName.substring(separator + 1);
    String rewrittenDbName = switch (direction) {
      case EXTERNALIZE -> matchesExternalDatabaseAlias(dbName, namespace.backendDbName())
          ? namespace.externalDbName()
          : dbName;
      case INTERNALIZE -> matchesExternalDatabaseAlias(dbName, namespace.externalDbName())
          ? namespace.backendDbName()
          : dbName;
    };
    return rewrittenDbName + "." + tableName;
  }

  private static String transformDbNameString(
      String dbName,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction
  ) {
    if (dbName == null) {
      return null;
    }
    return switch (direction) {
      case EXTERNALIZE -> matchesExternalDatabaseAlias(dbName, namespace.backendDbName())
          ? namespace.externalDbName()
          : dbName;
      case INTERNALIZE -> matchesExternalDatabaseAlias(dbName, namespace.externalDbName())
          ? namespace.backendDbName()
          : dbName;
    };
  }

  private static List<String> transformFullTableNames(
      List<String> fullTableNames,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction
  ) {
    if (fullTableNames == null) {
      return null;
    }
    if (fullTableNames.isEmpty()) {
      return fullTableNames;
    }
    List<String> transformed = new ArrayList<>(fullTableNames.size());
    for (String fullTableName : fullTableNames) {
      transformed.add(transformFullTableName(fullTableName, namespace, direction));
    }
    return transformed;
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private static String extractDbNameFromField(TFieldIdEnum fieldId, Object fieldValue) {
    if (fieldValue == null) {
      return null;
    }
    String fieldName = fieldId.getFieldName();
    if (fieldValue instanceof String stringValue) {
      if (looksLikeDbNameField(fieldName)) {
        return blankToNull(stringValue);
      }
      if (looksLikeFullTableNameField(fieldName)) {
        return extractDbNameFromFullTableName(stringValue);
      }
    }
    if (fieldValue instanceof List<?> listValue && looksLikeFullTableNamesField(fieldName)) {
      for (Object element : listValue) {
        if (element instanceof String stringValue) {
          String extractedDbName = extractDbNameFromFullTableName(stringValue);
          if (extractedDbName != null) {
            return extractedDbName;
          }
        }
      }
    }
    return null;
  }

  private static boolean looksLikeDbNameField(String fieldName) {
    String normalizedFieldName = normalizeFieldName(fieldName);
    return normalizedFieldName.equals("dbname") || normalizedFieldName.endsWith("dbname");
  }

  private static boolean looksLikeFullTableNameField(String fieldName) {
    return normalizeFieldName(fieldName).endsWith("fulltablename");
  }

  private static boolean looksLikeFullTableNamesField(String fieldName) {
    return normalizeFieldName(fieldName).endsWith("fulltablenames");
  }

  private static String normalizeFieldName(String fieldName) {
    if (fieldName == null) {
      return "";
    }
    return fieldName.replace("_", "").toLowerCase();
  }

  private static String readStringProperty(Object target, String... getterNames) {
    for (String getterName : getterNames) {
      try {
        Method method = target.getClass().getMethod(getterName);
        return (String) method.invoke(target);
      } catch (NoSuchMethodException ignored) {
        // Try the next compatible getter.
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(
            "Unable to invoke " + getterName + " on " + target.getClass().getName(), e);
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static List<String> readStringListProperty(Object target, String getterName) {
    try {
      Method method = target.getClass().getMethod(getterName);
      Object value = method.invoke(target);
      if (value == null) {
        return null;
      }
      return (List<String>) value;
    } catch (NoSuchMethodException ignored) {
      return null;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + getterName + " on " + target.getClass().getName(), e);
    }
  }

  private static void maybeInvoke(Object target, String methodName, String argument) {
    try {
      Method method = target.getClass().getMethod(methodName, String.class);
      method.invoke(target, argument);
    } catch (NoSuchMethodException ignored) {
      // Namespace propagation is best-effort for thrift DTOs with compatible setters.
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + methodName + " on " + target.getClass().getName(), e);
    }
  }

  private static void rewriteFullTableName(Object target, String value) {
    maybeInvoke(target, "setFullTableName", value);
  }

  private static void rewriteFullTableNames(Object target, List<String> values) {
    if (values == null) {
      return;
    }
    try {
      Method method = target.getClass().getMethod("setFullTableNames", List.class);
      method.invoke(target, values);
    } catch (NoSuchMethodException ignored) {
      // Namespace propagation is best-effort for thrift DTOs with compatible setters.
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke setFullTableNames on " + target.getClass().getName(), e);
    }
  }
}
