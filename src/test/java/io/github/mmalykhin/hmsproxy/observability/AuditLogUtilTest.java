package io.github.mmalykhin.hmsproxy.observability;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class AuditLogUtilTest {
  private static final ObjectMapper JSON = new ObjectMapper();

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

  @Test
  public void escapesEveryControlCharacterIntoParsableJson() throws Exception {
    String controls = controlCharacters();
    Map<String, Object> fields = AuditLogUtil.orderedFields();
    fields.put("authenticatedUser", "alice" + controls + "@EXAMPLE.COM");
    fields.put("remoteAddress", controls + "10.0.0.1");

    String json = AuditLogUtil.toJson(fields);

    for (int codePoint = 0; codePoint < 0x20; codePoint++) {
      Assert.assertEquals(
          "raw control character 0x" + Integer.toHexString(codePoint) + " leaked into audit JSON",
          -1,
          json.indexOf(codePoint));
    }
    JsonNode parsed = JSON.readTree(json);
    Assert.assertEquals("alice" + controls + "@EXAMPLE.COM", parsed.get("authenticatedUser").asText());
    Assert.assertEquals(controls + "10.0.0.1", parsed.get("remoteAddress").asText());
  }

  @Test
  public void escapesControlCharactersInsideFieldNames() throws Exception {
    String name = "odd" + (char) 0x01 + "key";
    Map<String, Object> fields = AuditLogUtil.orderedFields();
    fields.put(name, "value");

    String json = AuditLogUtil.toJson(fields);

    Assert.assertEquals(-1, json.indexOf(0x01));
    JsonNode parsed = JSON.readTree(json);
    Assert.assertEquals("value", parsed.get(name).asText());
  }

  @Test
  public void preservesNonAsciiCharactersVerbatim() throws Exception {
    String value = "user/тест é 中文 😀 " + (char) 0x7f;
    Map<String, Object> fields = AuditLogUtil.orderedFields();
    fields.put("authenticatedUser", value);

    String json = AuditLogUtil.toJson(fields);

    Assert.assertTrue(json.contains(value));
    Assert.assertEquals(value, JSON.readTree(json).get("authenticatedUser").asText());
  }

  @Test
  public void escapesQuotesBackslashesAndCommonWhitespace() throws Exception {
    Map<String, Object> fields = AuditLogUtil.orderedFields();
    fields.put("detail", "quote\" backslash\\ newline\n carriage\r tab\t");

    String json = AuditLogUtil.toJson(fields);

    Assert.assertTrue(json.contains("quote\\\" backslash\\\\ newline\\n carriage\\r tab\\t"));
    Assert.assertEquals(
        "quote\" backslash\\ newline\n carriage\r tab\t",
        JSON.readTree(json).get("detail").asText());
  }

  @Test
  public void keepsNullNumberAndBooleanValuesUnquoted() throws Exception {
    Map<String, Object> fields = AuditLogUtil.orderedFields();
    fields.put("catalog", null);
    fields.put("durationMs", 17L);
    fields.put("routed", false);

    JsonNode parsed = JSON.readTree(AuditLogUtil.toJson(fields));

    Assert.assertTrue(parsed.get("catalog").isNull());
    Assert.assertEquals(17L, parsed.get("durationMs").asLong());
    Assert.assertFalse(parsed.get("routed").asBoolean());
  }

  private static String controlCharacters() {
    StringBuilder builder = new StringBuilder(0x20);
    for (int codePoint = 0; codePoint < 0x20; codePoint++) {
      builder.append((char) codePoint);
    }
    return builder.toString();
  }
}
