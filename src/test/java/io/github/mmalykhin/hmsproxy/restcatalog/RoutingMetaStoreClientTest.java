package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.List;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

public class RoutingMetaStoreClientTest {
  @Test
  public void getDatabaseDelegatesToGetDatabaseRpc() throws Exception {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.databases.put("sales", RecordingThriftIface.database("sales"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    Database db = client.getDatabase("sales");

    Assert.assertEquals("sales", db.getName());
    Assert.assertEquals(List.of("get_database:sales"), delegate.calls);
  }

  @Test
  public void getAllDatabasesDelegates() throws Exception {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.allDatabases = List.of("sales", "marketing");
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    Assert.assertEquals(List.of("sales", "marketing"), client.getAllDatabases());
    Assert.assertEquals(List.of("get_all_databases"), delegate.calls);
  }

  @Test
  public void getAllTablesDelegates() throws Exception {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.tablesByDatabase.put("sales", List.of("orders", "customers"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    Assert.assertEquals(List.of("orders", "customers"), client.getAllTables("sales"));
    Assert.assertEquals(List.of("get_all_tables:sales"), delegate.calls);
  }

  @Test
  public void getTableDelegates() throws Exception {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.tables.put("sales.orders", RecordingThriftIface.table("sales", "orders"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    Table table = client.getTable("sales", "orders");
    Assert.assertEquals("orders", table.getTableName());
    Assert.assertEquals(List.of("get_table:sales:orders"), delegate.calls);
  }

  @Test
  public void tableExistsReturnsTrueWhenTableFound() throws Exception {
    RecordingThriftIface delegate = new RecordingThriftIface();
    delegate.tables.put("sales.orders", RecordingThriftIface.table("sales", "orders"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    Assert.assertTrue(client.tableExists("sales", "orders"));
  }

  @Test
  public void tableExistsReturnsFalseOnNoSuchObject() throws Exception {
    RecordingThriftIface delegate = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    Assert.assertFalse(client.tableExists("sales", "missing"));
  }

  @Test
  public void unsupportedMethodThrowsUnsupportedOperationException() {
    RecordingThriftIface delegate = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);

    try {
      client.dropDatabase("anything");
      Assert.fail("expected UnsupportedOperationException");
    } catch (UnsupportedOperationException expected) {
      Assert.assertTrue(expected.getMessage(),
          expected.getMessage().contains("dropDatabase"));
    } catch (Exception other) {
      Assert.fail("expected UnsupportedOperationException, got " + other.getClass().getName());
    }
  }

  @Test
  public void closeIsNoop() {
    RecordingThriftIface delegate = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate.iface);
    client.close();
    Assert.assertTrue(delegate.calls.isEmpty());
  }

  @Test
  public void scopedClientTranslatesDatabaseArguments() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    recording.databases.put("apache__default", RecordingThriftIface.database("apache__default"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Database db = client.getDatabase("default");
    Assert.assertEquals("default", db.getName());
    Assert.assertEquals(List.of("get_database:apache__default"), recording.calls);
  }

  @Test
  public void scopedClientFiltersAndStripsDatabaseListing() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    recording.allDatabases = List.of("default", "apache__default", "hdp__x");
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Assert.assertEquals(List.of("default"), client.getAllDatabases());
  }

  @Test
  public void scopedClientRewritesTableDbName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    recording.tables.put("apache__default.t1", RecordingThriftIface.table("apache__default", "t1"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Table t = client.getTable("default", "t1");
    Assert.assertEquals("default", t.getDbName());
  }

  @Test
  public void createTableTranslatesDatabaseName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Table table = RecordingThriftIface.table("default", "t1");
    client.createTable(table);
    Assert.assertEquals(List.of("create_table:apache__default.t1"), recording.calls);
  }

  @Test
  public void dropTableTranslatesDatabaseName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    client.dropTable("default", "t1", false, true);
    Assert.assertEquals(List.of("drop_table:apache__default.t1"), recording.calls);
  }

  @Test
  public void alterTableTranslatesDatabaseName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Table table = RecordingThriftIface.table("default", "t1");
    client.alter_table_with_environmentContext("default", "t1", table, null);
    Assert.assertEquals(List.of("alter_table:apache__default.t1"), recording.calls);
  }

  @Test
  public void lockAndUnlockReachTheDelegate() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(recording.iface);
    LockResponse response = client.lock(new LockRequest());
    Assert.assertEquals(RecordingThriftIface.LOCK_ID, response.getLockid());
    client.unlock(RecordingThriftIface.LOCK_ID);
    Assert.assertEquals(
        List.of("lock", "unlock:" + RecordingThriftIface.LOCK_ID), recording.calls);
  }
}
