package io.github.mmalykhin.hmsproxy.routing;

/**
 * Local-only cache invalidator that directly invalidates entries in in-memory caches of the current JVM.
 */
final class LocalCacheInvalidator implements CacheInvalidator {
  private final DatabaseListCache databaseListCache;
  private final DatabaseMetadataCache databaseMetadataCache;
  private final TableMetadataCache tableMetadataCache;
  private final PartitionMetadataCache partitionMetadataCache;

  LocalCacheInvalidator(
      DatabaseListCache databaseListCache,
      DatabaseMetadataCache databaseMetadataCache,
      TableMetadataCache tableMetadataCache,
      PartitionMetadataCache partitionMetadataCache
  ) {
    this.databaseListCache = databaseListCache;
    this.databaseMetadataCache = databaseMetadataCache;
    this.tableMetadataCache = tableMetadataCache;
    this.partitionMetadataCache = partitionMetadataCache;
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
  }
}
