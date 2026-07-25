package io.github.mmalykhin.hmsproxy.federation;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import io.github.mmalykhin.hmsproxy.federation.SqlReferenceScanner.Part;
import io.github.mmalykhin.hmsproxy.federation.SqlReferenceScanner.TableReference;
import io.github.mmalykhin.hmsproxy.routing.CatalogRouter;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ViewDefinitionCompatibility {
  private static final Logger LOG = LoggerFactory.getLogger(ViewDefinitionCompatibility.class);
  private static final String DEFAULT_BACKEND_CATALOG = "hive";
  private static final List<String> BACKEND_CATALOG_CONF_KEYS =
      List.of("metastore.catalog.default", "hive.metastore.catalog.default");

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
    if (value == null || isScalar(value)) {
      return;
    }
    if (value instanceof Table table) {
      if (visited.put(value, Boolean.TRUE) == null) {
        rewriteViewText(table, namespace, direction);
      }
      return;
    }
    if (value instanceof List<?> list) {
      if (visited.put(value, Boolean.TRUE) != null) {
        return;
      }
      for (Object element : list) {
        rewriteViewTexts(element, namespace, direction, visited);
      }
      return;
    }
    if (value instanceof Map<?, ?> map) {
      if (visited.put(value, Boolean.TRUE) != null) {
        return;
      }
      for (Object element : map.values()) {
        rewriteViewTexts(element, namespace, direction, visited);
      }
      return;
    }
    if (value instanceof TBase<?, ?> thriftValue) {
      // Subtrees that cannot reach a Table (partitions, storage descriptors, column stats) are
      // skipped without touching a single field.
      List<TFieldIdEnum> fields = ThriftViewTextFields.fieldsReachingViewText(thriftValue);
      if (fields.isEmpty() || visited.put(value, Boolean.TRUE) != null) {
        return;
      }
      for (TFieldIdEnum fieldId : fields) {
        // TUnion-backed thrift payloads throw when callers read inactive fields.
        // Only traverse fields that are actually set.
        if (isThriftFieldSet(thriftValue, fieldId)) {
          rewriteViewTexts(getThriftFieldValue(thriftValue, fieldId), namespace, direction, visited);
        }
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
    List<TableReference> references = SqlReferenceScanner.scan(sql);
    StringBuilder rewritten = null;
    int copiedUpTo = 0;
    for (TableReference reference : references) {
      Replacement replacement = rewriteQualifier(reference, namespace, direction);
      if (replacement == null || replacement.start() < copiedUpTo) {
        continue;
      }
      if (rewritten == null) {
        rewritten = new StringBuilder(sql.length() + 16);
      }
      rewritten.append(sql, copiedUpTo, replacement.start()).append(replacement.text());
      copiedUpTo = replacement.end();
    }
    if (rewritten == null) {
      return sql;
    }
    return rewritten.append(sql, copiedUpTo, sql.length()).toString();
  }

  /**
   * Returns the text edit for the qualifier of one table reference, or {@code null} when the
   * reference must stay untouched.
   */
  private Replacement rewriteQualifier(
      TableReference reference,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction
  ) {
    List<Part> qualifier = reference.qualifier();
    if (qualifier.size() == 1) {
      Part dbPart = qualifier.get(0);
      String rewrittenDb = rewriteDbName(dbPart.unquoted(), namespace, direction);
      if (rewrittenDb == null) {
        return null;
      }
      return replacementFor(dbPart, dbPart, rewrittenDb, dbPart.quoted());
    }
    if (qualifier.size() == 2) {
      return rewriteCatalogQualifiedReference(reference, qualifier, namespace, direction);
    }
    LOG.debug(
        "Leaving view SQL reference '{}' untouched: unsupported qualifier depth for namespace '{}'",
        joinParts(reference.parts()),
        namespace.externalDbName());
    return null;
  }

  private Replacement rewriteCatalogQualifiedReference(
      TableReference reference,
      List<Part> qualifier,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction
  ) {
    Part catalogPart = qualifier.get(0);
    Part dbPart = qualifier.get(1);
    String catalogToken = catalogPart.unquoted();
    String dbToken = dbPart.unquoted();
    if (direction == Direction.EXTERNALIZE) {
      if (!dbToken.equals(namespace.backendDbName())) {
        return null;
      }
      if (catalogToken.equalsIgnoreCase(backendCatalogName(namespace))) {
        // '<backend catalog>.<backend db>.<table>' collapses into the external database name.
        return replacementFor(catalogPart, dbPart, namespace.externalDbName(), dbPart.quoted());
      }
      LOG.debug(
          "Leaving view SQL reference '{}' untouched: catalog qualifier '{}' is not the backend"
              + " catalog of namespace '{}'",
          joinParts(reference.parts()),
          catalogToken,
          namespace.externalDbName());
      return null;
    }
    // INTERNALIZE: an external database name may itself be spelled 'catalog.db' when the
    // catalog-db separator contains a dot.
    if (config.catalogDbSeparator().contains(".")) {
      String joined = catalogToken + "." + dbToken;
      String rewrittenJoined = rewriteDbName(joined, namespace, direction);
      if (rewrittenJoined != null) {
        return replacementFor(catalogPart, dbPart, rewrittenJoined, dbPart.quoted());
      }
    }
    String rewrittenDb = rewriteDbName(dbToken, namespace, direction);
    if (rewrittenDb != null && !config.catalogs().containsKey(catalogToken)) {
      // Keep the catalog qualifier: dropping it would silently retarget the reference.
      return replacementFor(dbPart, dbPart, rewrittenDb, dbPart.quoted());
    }
    LOG.debug(
        "Leaving view SQL reference '{}' untouched: catalog qualifier '{}' cannot be resolved for"
            + " namespace '{}'",
        joinParts(reference.parts()),
        catalogToken,
        namespace.externalDbName());
    return null;
  }

  /** Returns the rewritten database name, or {@code null} when the name is not ours to rewrite. */
  private String rewriteDbName(
      String dbToken,
      CatalogRouter.ResolvedNamespace namespace,
      Direction direction
  ) {
    if (dbToken.isEmpty()) {
      return null;
    }
    return switch (direction) {
      case EXTERNALIZE -> dbToken.equals(namespace.backendDbName()) ? namespace.externalDbName() : null;
      case INTERNALIZE -> internalizeDbName(dbToken, namespace);
    };
  }

  private String internalizeDbName(String dbToken, CatalogRouter.ResolvedNamespace namespace) {
    if (dbToken.equals(namespace.externalDbName())) {
      return namespace.backendDbName();
    }
    CatalogRouter.ResolvedNamespace explicit = router.resolvePattern(dbToken).orElse(null);
    return explicit == null ? null : explicit.backendDbName();
  }

  private static Replacement replacementFor(Part from, Part to, String replacement, boolean quoted) {
    String text = quoted ? '`' + replacement.replace("`", "``") + '`' : replacement;
    return new Replacement(from.start(), to.end(), text);
  }

  private String backendCatalogName(CatalogRouter.ResolvedNamespace namespace) {
    CatalogConfig catalogConfig = config.catalogs().get(namespace.catalogName());
    if (catalogConfig != null) {
      for (String key : BACKEND_CATALOG_CONF_KEYS) {
        String configured = catalogConfig.hiveConf().get(key);
        if (configured != null && !configured.isBlank()) {
          return configured.trim();
        }
      }
    }
    return DEFAULT_BACKEND_CATALOG;
  }

  private static String joinParts(List<Part> parts) {
    StringBuilder joined = new StringBuilder();
    for (Part part : parts) {
      if (joined.length() > 0) {
        joined.append('.');
      }
      joined.append(part.text());
    }
    return joined.toString();
  }

  private static boolean isViewLike(Table table) {
    String tableType = table.getTableType();
    if (tableType == null || tableType.isBlank()) {
      return false;
    }
    String normalizedType = tableType.trim().toUpperCase(Locale.ROOT);
    return "VIRTUAL_VIEW".equals(normalizedType) || "MATERIALIZED_VIEW".equals(normalizedType);
  }

  private static boolean isScalar(Object value) {
    return value instanceof CharSequence
        || value instanceof Number
        || value instanceof Boolean
        || value instanceof Enum<?>;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Object getThriftFieldValue(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).getFieldValue(fieldId);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static boolean isThriftFieldSet(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).isSet(fieldId);
  }

  private record Replacement(int start, int end, String text) {
  }

  private enum Direction {
    EXTERNALIZE,
    INTERNALIZE
  }
}
