package io.github.mmalykhin.hmsproxy.routing;

import org.junit.Assert;
import org.junit.Test;

public class CacheInvalidationEventTest {

  @Test
  public void testSerializeDeserializeTableEvent() throws Exception {
    CacheInvalidationEvent original = new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.TABLE,
        "default",
        "analytics_db",
        "user_events",
        "instance-abc-123",
        1727600000000L
    );

    byte[] serialized = original.serialize();
    Assert.assertNotNull(serialized);
    Assert.assertTrue(serialized.length > 0);

    CacheInvalidationEvent deserialized = CacheInvalidationEvent.deserialize(serialized);
    Assert.assertEquals(original.type(), deserialized.type());
    Assert.assertEquals(original.catalogName(), deserialized.catalogName());
    Assert.assertEquals(original.backendDbName(), deserialized.backendDbName());
    Assert.assertEquals(original.tableName(), deserialized.tableName());
    Assert.assertEquals(original.originInstanceId(), deserialized.originInstanceId());
    Assert.assertEquals(original.timestampMs(), deserialized.timestampMs());
  }

  @Test
  public void testSerializeDeserializeDatabaseEventWithNullTable() throws Exception {
    CacheInvalidationEvent original = new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.DATABASE,
        "hdp",
        "sales",
        null,
        "instance-xyz-789",
        1727600005000L
    );

    byte[] serialized = original.serialize();
    CacheInvalidationEvent deserialized = CacheInvalidationEvent.deserialize(serialized);
    Assert.assertEquals(CacheInvalidationEvent.Type.DATABASE, deserialized.type());
    Assert.assertEquals("hdp", deserialized.catalogName());
    Assert.assertEquals("sales", deserialized.backendDbName());
    Assert.assertNull(deserialized.tableName());
    Assert.assertEquals("instance-xyz-789", deserialized.originInstanceId());
    Assert.assertEquals(1727600005000L, deserialized.timestampMs());
  }

  @Test
  public void testSerializeDeserializeAllEvent() throws Exception {
    CacheInvalidationEvent original = new CacheInvalidationEvent(
        CacheInvalidationEvent.Type.ALL,
        null,
        null,
        null,
        "instance-999",
        1727600010000L
    );

    byte[] serialized = original.serialize();
    CacheInvalidationEvent deserialized = CacheInvalidationEvent.deserialize(serialized);
    Assert.assertEquals(CacheInvalidationEvent.Type.ALL, deserialized.type());
    Assert.assertNull(deserialized.catalogName());
    Assert.assertNull(deserialized.backendDbName());
    Assert.assertNull(deserialized.tableName());
    Assert.assertEquals("instance-999", deserialized.originInstanceId());
  }
}
