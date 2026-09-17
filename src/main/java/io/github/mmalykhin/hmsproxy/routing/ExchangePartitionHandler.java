package io.github.mmalykhin.hmsproxy.routing;

import io.github.mmalykhin.hmsproxy.backend.CatalogBackend;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.hadoop.hive.metastore.api.MetaException;

final class ExchangePartitionHandler implements SpecialCaseHandler {
  private final RoutingSupport support;
  private final IcebergTablePointerGuard icebergTablePointerGuard;

  ExchangePartitionHandler(RoutingSupport support, IcebergTablePointerGuard icebergTablePointerGuard) {
    this.support = support;
    this.icebergTablePointerGuard = icebergTablePointerGuard;
  }

  @Override
  public Object handle(Method method, Object[] args) throws Throwable {
    String methodName = method.getName();
    if (args == null || args.length < 5
        || !(args[1] instanceof String sourceDb)
        || !(args[2] instanceof String sourceTable)
        || !(args[3] instanceof String destDb)
        || !(args[4] instanceof String destTable)) {
      throw new MetaException(methodName + " requires partitionSpecs, sourceDb, sourceTable, destDb, and destTable");
    }

    CatalogRouter.ResolvedNamespace sourceNamespace = support.router.resolveDatabase(sourceDb);
    CatalogRouter.ResolvedNamespace destNamespace = support.router.resolveDatabase(destDb);

    if (!sourceNamespace.catalogName().equals(destNamespace.catalogName())) {
      throw new MetaException("Cannot exchange partitions across different catalogs: source catalog '"
          + sourceNamespace.catalogName() + "' and target catalog '" + destNamespace.catalogName() + "'");
    }

    RequestContext.currentObservation().recordNamespace(sourceNamespace);
    support.recordDefaultCatalogRouteIfImplicit(methodName, sourceDb, sourceNamespace);
    CatalogBackend backend = sourceNamespace.backend();
    support.validateCatalogAccess(backend, methodName, sourceNamespace.backendDbName());
    if (!sourceNamespace.backendDbName().equalsIgnoreCase(destNamespace.backendDbName())) {
      support.validateCatalogAccess(backend, methodName, destNamespace.backendDbName());
    }

    Object[] routedArgs = Arrays.copyOf(args, args.length);
    routedArgs[1] = sourceNamespace.backendDbName();
    routedArgs[3] = destNamespace.backendDbName();

    Object result = support.invokeDirect(backend, method, routedArgs);

    if (icebergTablePointerGuard != null) {
      icebergTablePointerGuard.invalidate(sourceNamespace.catalogName(), sourceNamespace.backendDbName(), sourceTable);
      icebergTablePointerGuard.invalidate(destNamespace.catalogName(), destNamespace.backendDbName(), destTable);
    }

    return support.federationLayer.externalizeResult(result, destNamespace);
  }
}
