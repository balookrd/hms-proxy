package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier;
import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

final class RenamePartitionHandler implements SpecialCaseHandler {
  private static final Method RENAME_PARTITION = findMethod("rename_partition",
      String.class, String.class, List.class, Partition.class);

  private final RoutingSupport support;
  private final IcebergTablePointerGuard icebergTablePointerGuard;
  private final ExternalTableLocationRewriter externalTableLocationRewriter;

  RenamePartitionHandler(RoutingSupport support, IcebergTablePointerGuard icebergTablePointerGuard) {
    this(support, icebergTablePointerGuard, null);
  }

  RenamePartitionHandler(RoutingSupport support, IcebergTablePointerGuard icebergTablePointerGuard,
      ExternalTableLocationRewriter externalTableLocationRewriter) {
    this.support = support;
    this.icebergTablePointerGuard = icebergTablePointerGuard;
    this.externalTableLocationRewriter = externalTableLocationRewriter;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    boolean isReq = "rename_partition_req".equals(methodName);

    Object request = isReq ? (args != null && args.length > 0 ? args[0] : null) : null;
    String dbName;
    String tableName;
    Object rawNewPart;
    Long txnId = null;
    Long writeId = null;

    if (isReq) {
      if (request == null) {
        throw new MetaException("rename_partition_req requires a request body");
      }
      dbName = (String) ThriftReflectionCache.invokeGetter(request, "getDbName");
      if (dbName == null) {
        dbName = NamespaceTranslator.extractDbName(request);
      }
      tableName = (String) ThriftReflectionCache.invokeGetter(request, "getTableName");
      rawNewPart = ThriftReflectionCache.invokeGetter(request, "getNewPart");
      Object rawTxnId = ThriftReflectionCache.invokeGetter(request, "getTxnId");
      if (rawTxnId instanceof Number n) {
        txnId = n.longValue();
      }
      Object rawWriteId = ThriftReflectionCache.invokeGetter(request, "getWriteId");
      if (rawWriteId instanceof Number n) {
        writeId = n.longValue();
      }
    } else {
      if (args == null || args.length < 4) {
        throw new MetaException("rename_partition requires dbName, tableName, partVals, and newPart");
      }
      dbName = (String) args[0];
      tableName = (String) args[1];
      rawNewPart = args[3];
    }

    if (dbName == null || dbName.isBlank()) {
      throw new MetaException(methodName + " requires a target database");
    }

    CatalogRouter.ResolvedNamespace namespace = support.router.resolveDatabase(dbName);
    RequestContext.currentObservation().recordNamespace(namespace);
    support.recordDefaultCatalogRouteIfImplicit(methodName, dbName, namespace);
    CatalogBackend backend = namespace.backend();
    support.validateCatalogAccess(backend, methodName, namespace.backendDbName());

    boolean hasTxn = (txnId != null && txnId > 0) || (writeId != null && writeId > 0);
    if (hasTxn && !namespace.catalogName().equals(support.config.defaultCatalog())) {
      throw new MetaException("ACID transactional operation '" + methodName + "' is not supported for non-default catalog '"
          + namespace.catalogName() + "'. Transaction management is only available in the default catalog '"
          + support.config.defaultCatalog() + "'");
    }

    Object result;
    if (isReq) {
      Object routedRequest = ThriftReflectionCache.deepCopy(request);
      ThriftReflectionCache.invokeStringSetter(routedRequest, "setDbName", namespace.backendDbName());
      String catName = ThriftReflectionCache.readString(request, "getCatName");
      if (catName != null) {
        ThriftReflectionCache.invokeStringSetter(routedRequest, "setCatName",
            NamespaceTranslator.internalCatalogName(catName, dbName, namespace,
                support.federationLayer.preserveBackendCatalogName()));
      }
      String validWriteIdList = ThriftReflectionCache.readString(request, "getValidWriteIdList");
      if (validWriteIdList != null) {
        ThriftReflectionCache.invokeStringSetter(routedRequest, "setValidWriteIdList",
            NamespaceInternalizer.transformValidWriteIdList(validWriteIdList, namespace));
      }
      if (rawNewPart != null) {
        Object internalizedPart = support.federationLayer.internalizeArgument(rawNewPart, namespace);
        if (externalTableLocationRewriter != null) {
          externalTableLocationRewriter.rewriteObjectArguments(new Object[]{internalizedPart}, namespace, methodName);
        }
        setNewPartOnRequest(routedRequest, internalizedPart);
      }

      try {
        result = support.invokeBackendNamed(backend, "rename_partition_req", routedRequest);
      } catch (Throwable cause) {
        if (ThriftFailureClassifier.isUnsupportedMethod(cause)) {
          result = fallbackToLegacy(backend, routedRequest);
        } else {
          throw cause;
        }
      }
    } else {
      Object[] routedArgs = Arrays.copyOf(args, args.length);
      routedArgs[0] = namespace.backendDbName();
      if (rawNewPart != null) {
        Object internalizedPart = support.federationLayer.internalizeArgument(rawNewPart, namespace);
        if (externalTableLocationRewriter != null) {
          externalTableLocationRewriter.rewriteObjectArguments(new Object[]{internalizedPart}, namespace, methodName);
        }
        routedArgs[3] = internalizedPart;
      }
      result = support.invokeDirect(backend, method, routedArgs);
    }

    if (icebergTablePointerGuard != null && tableName != null) {
      icebergTablePointerGuard.invalidate(namespace.catalogName(), namespace.backendDbName(), tableName);
    }

    return isReq ? support.federationLayer.externalizeResult(result, namespace) : result;
  }

  private static void setNewPartOnRequest(Object request, Object newPart) {
    if (request == null || newPart == null) {
      return;
    }
    for (Method method : request.getClass().getMethods()) {
      if ("setNewPart".equals(method.getName()) && method.getParameterCount() == 1) {
        Class<?> paramType = method.getParameterTypes()[0];
        if (paramType.isInstance(newPart)) {
          try {
            method.invoke(request, newPart);
            return;
          } catch (Exception e) {
            throw new IllegalStateException("Failed to invoke setNewPart on " + request.getClass().getName(), e);
          }
        }
      }
    }
    for (Method method : request.getClass().getMethods()) {
      if ("setNewPart".equals(method.getName()) && method.getParameterCount() == 1) {
        try {
          Object converted = ThriftValueConverter.convertTBase(newPart, method.getParameterTypes()[0]);
          method.invoke(request, converted);
          return;
        } catch (Exception e) {
          throw new IllegalStateException("Failed to invoke setNewPart with conversion on " + request.getClass().getName(), e);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Object fallbackToLegacy(CatalogBackend backend, Object routedRequest) throws Throwable {
    String db = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getDbName");
    String tbl = (String) ThriftReflectionCache.invokeGetter(routedRequest, "getTableName");
    Object partVals = ThriftReflectionCache.invokeGetter(routedRequest, "getPartVals");
    Object newPart = ThriftReflectionCache.invokeGetter(routedRequest, "getNewPart");

    List<String> partValsList = partVals == null ? List.of() : (List<String>) partVals;
    Partition apacheNewPart = (Partition) ThriftValueConverter.convertTBase(newPart, Partition.class);
    support.invokeDirect(backend, RENAME_PARTITION, new Object[]{db, tbl, partValsList, apacheNewPart});
    return null;
  }

  private static Method findMethod(String name, Class<?>... parameterTypes) {
    try {
      return ThriftHiveMetastore.Iface.class.getMethod(name, parameterTypes);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("ThriftHiveMetastore.Iface missing method " + name, e);
    }
  }
}
