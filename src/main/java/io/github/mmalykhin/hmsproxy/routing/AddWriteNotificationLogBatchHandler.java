package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class AddWriteNotificationLogBatchHandler implements SpecialCaseHandler {
  private final RoutingSupport support;

  private record ItemResolution(CatalogRouter.ResolvedNamespace namespace, String itemCat, String itemDb) {
  }

  AddWriteNotificationLogBatchHandler(RoutingSupport support) {
    this.support = support;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    Object request = args[0];
    String catName = ThriftReflectionCache.readString(request, "getCatName", "getCatalogName", "getCatalog");
    String dbName = ThriftReflectionCache.readString(request, "getDb", "getDbName");
    if (dbName == null) {
      Object rawItems = ThriftReflectionCache.invokeGetter(request, "getRequestList");
      if (rawItems instanceof Iterable<?> items) {
        for (Object item : items) {
          if (item != null) {
            dbName = ThriftReflectionCache.readString(item, "getDb", "getDbName");
            if (catName == null) {
              catName = ThriftReflectionCache.readString(item, "getCatName", "getCatalogName", "getCatalog");
            }
            if (dbName != null) {
              break;
            }
          }
        }
      }
    }
    if (dbName == null) {
      throw new MetaException("add_write_notification_log_in_batch requires a target database");
    }
    CatalogRouter.ResolvedNamespace namespace = support.resolveRequestNamespace(catName, dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit("add_write_notification_log_in_batch", catName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, "add_write_notification_log_in_batch", namespace.backendDbName());
    if (!backend.runtimeProfile().isHive4()) {
      throw new MetaException(
          "add_write_notification_log_in_batch requires a Hive 4 backend runtime for catalog '"
              + backend.name()
              + "'");
    }

    List<ItemResolution> resolvedItems = new ArrayList<>();
    Object rawItems = ThriftReflectionCache.invokeGetter(request, "getRequestList");
    if (rawItems instanceof Iterable<?> items) {
      for (Object item : items) {
        if (item == null) {
          resolvedItems.add(null);
          continue;
        }
        String itemCat = ThriftReflectionCache.readString(item, "getCatName", "getCatalogName", "getCatalog");
        String itemDb = ThriftReflectionCache.readString(item, "getDb", "getDbName");
        if (itemDb != null) {
          CatalogRouter.ResolvedNamespace itemNs =
              support.resolveRequestNamespace(itemCat != null ? itemCat : catName, itemDb);
          if (!itemNs.catalogName().equals(namespace.catalogName())) {
            throw new MetaException(
                "Cannot add write notification log batch across different catalogs: '"
                    + namespace.catalogName() + "' and '" + itemNs.catalogName() + "'");
          }
          support.validateCatalogAccess(backend, "add_write_notification_log_in_batch", itemNs.backendDbName());
          resolvedItems.add(new ItemResolution(itemNs, itemCat, itemDb));
        } else {
          resolvedItems.add(new ItemResolution(null, itemCat, null));
        }
      }
    }

    boolean preserveBackendCatalogName = support.federationLayer.preserveBackendCatalogName();
    String internalCat = catName == null ? null : NamespaceTranslator.internalCatalogName(
        catName, dbName, namespace, preserveBackendCatalogName);
    String backendCatName = internalCat;
    if (backendCatName == null || backendCatName.isBlank()) {
      CatalogConfig catalogConfig = support.config.catalogs().get(namespace.catalogName());
      backendCatName = catalogConfig == null
          ? CatalogConfig.DEFAULT_BACKEND_CATALOG_NAME
          : catalogConfig.backendDefaultCatalogName();
    }
    Object routedRequest = ThriftReflectionCache.deepCopy(request);
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDb", namespace.backendDbName());
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setDbName", namespace.backendDbName());
    if (catName != null) {
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatName", internalCat);
    }
    ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatalog", backendCatName);
    Object rawList = ThriftReflectionCache.invokeGetter(routedRequest, "getRequestList");
    if (rawList instanceof Iterable<?> items) {
      int idx = 0;
      for (Object item : items) {
        ItemResolution resolution = idx < resolvedItems.size() ? resolvedItems.get(idx) : null;
        idx++;
        if (item == null || resolution == null) {
          continue;
        }
        if (resolution.namespace() != null) {
          ThriftReflectionCache.invokeStringSetter(item, "setDb", resolution.namespace().backendDbName());
          ThriftReflectionCache.invokeStringSetter(item, "setDbName", resolution.namespace().backendDbName());
          if (resolution.itemCat() != null) {
            String internalItemCat = NamespaceTranslator.internalCatalogName(
                resolution.itemCat(), resolution.itemDb(), resolution.namespace(), preserveBackendCatalogName);
            ThriftReflectionCache.invokeStringSetter(item, "setCatName", internalItemCat);
            ThriftReflectionCache.invokeStringSetter(
                item,
                "setCatalog",
                (internalItemCat == null || internalItemCat.isBlank()) ? backendCatName : internalItemCat);
          }
        } else if (resolution.itemCat() != null) {
          String internalItemCat = NamespaceTranslator.internalCatalogName(
              resolution.itemCat(), dbName, namespace, preserveBackendCatalogName);
          ThriftReflectionCache.invokeStringSetter(item, "setCatName", internalItemCat);
          ThriftReflectionCache.invokeStringSetter(
              item,
              "setCatalog",
              (internalItemCat == null || internalItemCat.isBlank()) ? backendCatName : internalItemCat);
        }
      }
    }
    return support.invokeBackendNamed(backend, "add_write_notification_log_in_batch", routedRequest);
  }
}
