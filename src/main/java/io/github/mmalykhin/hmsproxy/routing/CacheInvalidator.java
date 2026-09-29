package io.github.mmalykhin.hmsproxy.routing;

/**
 * Strategy interface for invalidating metadata caches (table, partition, database, catalog)
 * either locally or across a distributed cluster of proxy instances.
 */
public interface CacheInvalidator extends AutoCloseable {

  /**
   * Invalidates table metadata and partition metadata for the specified table.
   */
  void invalidateTable(String catalogName, String backendDbName, String tableName);

  /**
   * Invalidates database metadata, database lists, and all tables/partitions within the database.
   */
  void invalidateDatabase(String catalogName, String backendDbName);

  /**
   * Invalidates all metadata cached for the specified catalog.
   */
  void invalidateCatalog(String catalogName);

  /**
   * Clears all cached metadata across all catalogs and databases.
   */
  void invalidateAll();

  @Override
  default void close() throws Exception {}
}
