package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.lang.reflect.Field;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

final class ViewDefinitionCompatibility {
  private static final Pattern DB_TABLE_REFERENCE = Pattern.compile(
      "(?<db>`[^`]+`|[A-Za-z0-9_@#]+(?:\\.[A-Za-z0-9_@#]+)?)\\s*\\.\\s*(?<table>`[^`]+`|[A-Za-z0-9_]+)");

  private final ProxyConfig config;
  private final CatalogRouter router;

  ViewDefinitionCompatibility(ProxyConfig config, CatalogRouter router) {
    this.config = config;
    this.router = router;
  }

  Object externalizeResult(Object value, CatalogRouter.ResolvedNamespace namespace) {
    if (!config.federation().viewTextRewriteEnabled()) {
      return value;
    }
    rewriteViewTexts(value, namespace, Direction.EXTERNALIZE, new IdentityHashMap<>());
    return value;
  }

  Object internalizeArgument(Object value, CatalogRouter.ResolvedNamespace namespace) {
    if (!config.federation().viewTextRewriteEnabled()) {
      return value;
    }
    rewriteViewTexts(value, namespace, Direction.INTERNALIZE, new IdentityHashMap<>());
    return value;
  }

  private void rewriteViewTexts(
      Object value,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction,
      IdentityHashMap<Object, Boolean> visited
  ) {
    if (value == null || isScalar(value) || visited.put(value, Boolean.TRUE) != null) {
      return;
    }
    if (value instanceof Table table) {
      rewriteViewText(table, namespace, direction);
    }
    if (value instanceof List<?> list) {
      for (Object element : list) {
        rewriteViewTexts(element, namespace, direction, visited);
      }
      return;
    }
    if (value instanceof Map<?, ?> map) {
      for (Object element : map.values()) {
        rewriteViewTexts(element, namespace, direction, visited);
      }
      return;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      for (TFieldIdEnum fieldId : thriftFieldIds(thriftValue)) {
        rewriteViewTexts(getThriftFieldValue(thriftValue, fieldId), namespace, direction, visited);
      }
    }
  }

  private void rewriteViewText(Table table, CatalogRouter.ResolvedNamespace namespace, Direction direction) {
    if (!isViewLike(table)) {
      return;
    }
    table.setViewExpandedText(rewriteSql(table.getViewExpandedText(), namespace, direction));
    if (!config.federation().preserveOriginalViewText()) {
      table.setViewOriginalText(rewriteSql(table.getViewOriginalText(), namespace, direction));
    }
  }

  private String rewriteSql(String sql, CatalogRouter.ResolvedNamespace namespace, Direction direction) {
    if (sql == null || sql.isBlank()) {
      return sql;
    }
    Matcher matcher = DB_TABLE_REFERENCE.matcher(sql);
    StringBuffer rewritten = new StringBuffer();
    while (matcher.find()) {
      String dbToken = matcher.group("db");
      String rewrittenDbToken = rewriteDbToken(dbToken, namespace, direction);
      if (rewrittenDbToken.equals(dbToken)) {
        matcher.appendReplacement(rewritten, Matcher.quoteReplacement(matcher.group()));
        continue;
      }
      String replacement = matcher.group().replaceFirst(
          Pattern.quote(dbToken),
          Matcher.quoteReplacement(rewrittenDbToken));
      matcher.appendReplacement(rewritten, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(rewritten);
    return rewritten.toString();
  }

  private String rewriteDbToken(String dbToken, CatalogRouter.ResolvedNamespace namespace, Direction direction) {
    String normalized = stripBackticks(dbToken);
    String rewritten = switch (direction) {
      case EXTERNALIZE -> externalizeDbToken(normalized, namespace);
      case INTERNALIZE -> internalizeDbToken(normalized, namespace);
    };
    if (rewritten.equals(normalized)) {
      return dbToken;
    }
    return isBackticked(dbToken) ? '`' + rewritten + '`' : rewritten;
  }

  private String externalizeDbToken(String dbToken, CatalogRouter.ResolvedNamespace namespace) {
    if (matchesBackendDb(dbToken, namespace.backendDbName())) {
      return namespace.externalDbName();
    }
    return dbToken;
  }

  private String internalizeDbToken(String dbToken, CatalogRouter.ResolvedNamespace namespace) {
    if (dbToken.equals(namespace.externalDbName())) {
      return namespace.backendDbName();
    }
    CatalogRouter.ResolvedNamespace explicit = router.resolvePattern(dbToken).orElse(null);
    if (explicit != null) {
      return explicit.backendDbName();
    }
    return dbToken;
  }

  private static boolean matchesBackendDb(String dbToken, String backendDbName) {
    if (dbToken == null || dbToken.isBlank() || backendDbName == null || backendDbName.isBlank()) {
      return false;
    }
    return dbToken.equals(backendDbName) || dbToken.endsWith("." + backendDbName);
  }

  private static boolean isViewLike(Table table) {
    String tableType = table.getTableType();
    if (tableType == null || tableType.isBlank()) {
      return false;
    }
    String normalizedType = tableType.trim().toUpperCase();
    return "VIRTUAL_VIEW".equals(normalizedType) || "MATERIALIZED_VIEW".equals(normalizedType);
  }

  private static boolean isScalar(Object value) {
    return value instanceof CharSequence
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof Enum<?>;
  }

  private static boolean isBackticked(String value) {
    return value.length() >= 2 && value.startsWith("`") && value.endsWith("`");
  }

  private static String stripBackticks(String value) {
    return isBackticked(value) ? value.substring(1, value.length() - 1) : value;
  }

  @SuppressWarnings("unchecked")
  private static List<TFieldIdEnum> thriftFieldIds(TBase<?, ?> thriftValue) {
    try {
      Field metadataField = thriftValue.getClass().getField("metaDataMap");
      Map<?, ?> metadata = (Map<?, ?>) metadataField.get(null);
      return List.copyOf((Set<TFieldIdEnum>) metadata.keySet());
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to inspect thrift metadata for " + thriftValue.getClass().getName(), e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object getThriftFieldValue(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).getFieldValue(fieldId);
  }

  private enum Direction {
    EXTERNALIZE,
    INTERNALIZE
  }
}
