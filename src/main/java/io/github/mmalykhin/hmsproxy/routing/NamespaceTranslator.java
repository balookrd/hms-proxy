package io.github.mmalykhin.hmsproxy.routing;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

public final class NamespaceTranslator {
  /**
   * Hive's built-in pseudo source for {@code INSERT ... VALUES} and FROM-less queries. It exists in
   * no metastore, so a reference to it carries no namespace to route by.
   */
  public static final String DUMMY_DATABASE = "_dummy_database";

  private NamespaceTranslator() {}

  public static boolean isDummySourceDbName(String dbName) {
    return DUMMY_DATABASE.equals(dbName);
  }

  public static Object externalizeResult(Object value, CatalogRouter.ResolvedNamespace namespace) {
    return externalizeResult(value, namespace, false);
  }

  public static Object externalizeResult(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return NamespaceExternalizer.externalize(value, namespace, preserveBackendCatalogName);
  }

  public static Object internalizeArgument(Object value, CatalogRouter.ResolvedNamespace namespace) {
    return internalizeArgument(value, namespace, false);
  }

  public static Object internalizeArgument(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return NamespaceInternalizer.internalize(value, namespace, preserveBackendCatalogName);
  }

  public static String internalizeStringArgument(String value, CatalogRouter.ResolvedNamespace namespace) {
    if (value == null) {
      return null;
    }
    return matchesExternalDatabaseAlias(value, namespace.externalDbName()) ? namespace.backendDbName() : value;
  }

  public static Table externalizeTable(Table table, CatalogRouter.ResolvedNamespace namespace) {
    return externalizeTable(table, namespace, false);
  }

  public static Table externalizeTable(
      Table table,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return (Table) externalizeResult(table, namespace, preserveBackendCatalogName);
  }

  public static TableMeta externalizeTableMeta(TableMeta tableMeta, CatalogRouter.ResolvedNamespace namespace) {
    return externalizeTableMeta(tableMeta, namespace, false);
  }

  public static TableMeta externalizeTableMeta(
      TableMeta tableMeta,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return (TableMeta) externalizeResult(tableMeta, namespace, preserveBackendCatalogName);
  }

  public static String internalCatalogName(String requestCatalogName, CatalogRouter.ResolvedNamespace namespace) {
    return internalCatalogName(requestCatalogName, null, namespace);
  }

  public static String internalCatalogName(
      String requestCatalogName,
      String originalDbName,
      CatalogRouter.ResolvedNamespace namespace
  ) {
    return internalCatalogName(requestCatalogName, originalDbName, namespace, false);
  }

  public static String internalCatalogName(
      String requestCatalogName,
      String originalDbName,
      CatalogRouter.ResolvedNamespace namespace,
      boolean preserveBackendCatalogName
  ) {
    return NamespaceInternalizer.internalCatalogName(
        requestCatalogName, originalDbName, namespace, preserveBackendCatalogName);
  }

  public static String internalCatalogName(String requestCatalogName, String proxyCatalogName) {
    return NamespaceInternalizer.internalCatalogName(requestCatalogName, proxyCatalogName);
  }

  public static String extractDbName(Object value) {
    return extractDbName(value, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static String extractDbName(Object value, Set<Object> seen) {
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
        String nestedDbName = extractDbName(element, seen);
        if (nestedDbName != null) {
          return nestedDbName;
        }
      }
      return null;
    }
    if (value instanceof Map<?, ?> map) {
      for (Object element : map.values()) {
        String nestedDbName = extractDbName(element, seen);
        if (nestedDbName != null) {
          return nestedDbName;
        }
      }
      return null;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      if (!seen.add(thriftValue)) {
        return null;
      }
      for (TFieldIdEnum fieldId : ThriftReflectionCache.fieldIds(thriftValue)) {
        Object fieldValue = ThriftReflectionCache.getField(thriftValue, fieldId);
        String nestedDbName = extractDbNameFromField(fieldId, fieldValue);
        if (nestedDbName == null) {
          nestedDbName = extractDbName(fieldValue, seen);
        }
        if (nestedDbName != null) {
          return nestedDbName;
        }
      }
    }
    return null;
  }

  // --- package-private helpers used by NamespaceExternalizer / NamespaceInternalizer ---

  static boolean isScalar(Object value) {
    return value instanceof CharSequence
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof Enum<?>;
  }

  static String readDbNameProperty(Object value) {
    return blankToNull(ThriftReflectionCache.readString(value, "getDbName", "getDbname", "getDb_name"));
  }

  static String readFullTableNameProperty(Object value) {
    return blankToNull(ThriftReflectionCache.readString(value, "getFullTableName", "getFull_table_name"));
  }

  static List<String> readFullTableNamesProperty(Object value) {
    List<String> result = ThriftReflectionCache.readStringList(value, "getFullTableNames");
    return result != null ? result : ThriftReflectionCache.readStringList(value, "getFull_table_names");
  }

  static String extractDbNameFromFullTableName(String fullTableName) {
    if (fullTableName == null || fullTableName.isBlank()) {
      return null;
    }
    int separator = fullTableName.lastIndexOf('.');
    if (separator <= 0) {
      return null;
    }
    return blankToNull(fullTableName.substring(0, separator));
  }

  static String extractDbNameFromField(TFieldIdEnum fieldId, Object fieldValue) {
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

  static boolean matchesExternalDatabaseAlias(String originalDbName, String externalDbName) {
    if (originalDbName == null || originalDbName.isBlank()
        || externalDbName == null || externalDbName.isBlank()) {
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

  static boolean looksLikeDbNameField(String fieldName) {
    String normalized = normalizeFieldName(fieldName);
    return normalized.equals("dbname") || normalized.endsWith("dbname");
  }

  static boolean looksLikeFullTableNameField(String fieldName) {
    return normalizeFieldName(fieldName).endsWith("fulltablename");
  }

  static boolean looksLikeFullTableNamesField(String fieldName) {
    return normalizeFieldName(fieldName).endsWith("fulltablenames");
  }

  static String normalizeFieldName(String fieldName) {
    if (fieldName == null) {
      return "";
    }
    return fieldName.replace("_", "").toLowerCase();
  }

  static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
