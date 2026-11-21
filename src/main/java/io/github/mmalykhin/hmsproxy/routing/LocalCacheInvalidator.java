package io.github.mmalykhin.hmsproxy.routing;

/**
 * Local-only cache invalidator that directly invalidates entries in in-memory caches of the current JVM.
 */
final class LocalCacheInvalidator implements CacheInvalidator {
  private final DatabaseListCache databaseListCache;
  private final DatabaseMetadataCache databaseMetadataCache;
  private final TableMetadataCache tableMetadataCache;
  private final PartitionMetadataCache partitionMetadataCache;
  private final CatalogRouter router;
  private volatile IcebergTablePointerGuard icebergTablePointerGuard;

  LocalCacheInvalidator(
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      TableMetadataCache tableMetadataCache,
      PartitionMetadataCache partitionMetadataCache
  ) {
    this(databaseListCache, databaseMetadataCache, tableMetadataCache, partitionMetadataCache, null);
  }

  LocalCacheInvalidator(
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      TableMetadataCache tableMetadataCache,
      PartitionMetadataCache partitionMetadataCache,
      CatalogRouter router
  ) {
    this.databaseListCache = databaseListCache;
    this.databaseMetadataCache = databaseMetadataCache;
    this.tableMetadataCache = tableMetadataCache;
    this.partitionMetadataCache = partitionMetadataCache;
    this.router = router;
  }

  void attachIcebergTablePointerGuard(IcebergTablePointerGuard guard) {
    this.icebergTablePointerGuard = guard;
  }

  @Override
  public void invalidateTable(String catalogName, String backendDbName, String tableName) {
    if (tableMetadataCache != null) {
      tableMetadataCache.invalidateTable(catalogName, backendDbName, tableName);
    }
    if (partitionMetadataCache != null) {
      partitionMetadataCache.invalidateTable(catalogName, backendDbName, tableName);
    }
  }

  void invalidateTableFromRemote(String catalogName, String backendDbName, String tableName) {
    invalidateTable(catalogName, backendDbName, tableName);
    IcebergTablePointerGuard guard = this.icebergTablePointerGuard;
    if (guard != null) {
      guard.invalidate(catalogName, backendDbName, tableName);
    }
  }

  @Override
  public void invalidateDatabase(String catalogName, String backendDbName) {
    if (databaseMetadataCache != null) {
      databaseMetadataCache.invalidate(catalogName, backendDbName);
    }
    if (databaseListCache != null) {
      databaseListCache.invalidate(catalogName);
    }
    if (tableMetadataCache != null) {
      tableMetadataCache.invalidateDatabase(catalogName, backendDbName);
    }
    if (partitionMetadataCache != null) {
      partitionMetadataCache.invalidateDatabase(catalogName, backendDbName);
    }
    if (router != null) {
      router.invalidateDatabaseRoutingCache(catalogName, backendDbName);
    }
    IcebergTablePointerGuard guard = this.icebergTablePointerGuard;
    if (guard != null) {
      guard.invalidateDatabase(catalogName, backendDbName);
    }
  }

  @Override
  public void invalidateCatalog(String catalogName) {
    if (databaseListCache != null) {
      databaseListCache.invalidate(catalogName);
    }
    if (databaseMetadataCache != null) {
      databaseMetadataCache.invalidateCatalog(catalogName);
    }
    if (tableMetadataCache != null) {
      tableMetadataCache.invalidateCatalog(catalogName);
    }
    if (partitionMetadataCache != null) {
      partitionMetadataCache.invalidateCatalog(catalogName);
    }
    if (router != null) {
      router.invalidateCatalogRoutingCaches(catalogName);
    }
    IcebergTablePointerGuard guard = this.icebergTablePointerGuard;
    if (guard != null) {
      guard.invalidateCatalog(catalogName);
    }
  }

  @Override
  public void invalidateAll() {
    if (databaseListCache != null) {
      databaseListCache.invalidateAll();
    }
    if (databaseMetadataCache != null) {
      databaseMetadataCache.invalidateAll();
    }
    if (tableMetadataCache != null) {
      tableMetadataCache.clear();
    }
    if (partitionMetadataCache != null) {
      partitionMetadataCache.clear();
    }
    if (router != null) {
      router.invalidateAllRoutingCaches();
    }
    IcebergTablePointerGuard guard = this.icebergTablePointerGuard;
    if (guard != null) {
      guard.invalidateAll();
    }
  }
}
