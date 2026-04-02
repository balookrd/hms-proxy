package io.github.mmalykhin.hmsproxy.observability;

import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AuditLogUtilTest {
  @Test
  public void rendersStructuredJsonValues() {
    String json = AuditLogUtil.toJson(Map.of(
        "event", "hms_proxy_audit",
        "requestId", 42,
        "routed", true,
        "user", "alice@example.com"));

    Assert.assertTrue(json.contains("\"event\":\"hms_proxy_audit\""));
    Assert.assertTrue(json.contains("\"requestId\":42"));
    Assert.assertTrue(json.contains("\"routed\":true"));
    Assert.assertTrue(json.contains("\"user\":\"alice@example.com\""));
  }
}
