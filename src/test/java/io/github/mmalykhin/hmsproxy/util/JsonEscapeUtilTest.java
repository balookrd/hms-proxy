package io.github.mmalykhin.hmsproxy.util;

import org.junit.Assert;
import org.junit.Test;

public class JsonEscapeUtilTest {
  @Test
  public void escapesShortFormsForCommonControlCharacters() {
    Assert.assertEquals("\\b\\f\\n\\r\\t", JsonEscapeUtil.escape("\b\f\n\r\t"));
    Assert.assertEquals("\\\"\\\\", JsonEscapeUtil.escape("\"\\"));
  }

  @Test
  public void escapesRemainingControlCharactersAsUnicodeSequences() {
    Assert.assertEquals("\\u0000", JsonEscapeUtil.escape(String.valueOf((char) 0x00)));
    Assert.assertEquals("\\u0001", JsonEscapeUtil.escape(String.valueOf((char) 0x01)));
    Assert.assertEquals("\\u001b", JsonEscapeUtil.escape(String.valueOf((char) 0x1b)));
    Assert.assertEquals("\\u001f", JsonEscapeUtil.escape(String.valueOf((char) 0x1f)));
  }

  @Test
  public void returnsSameInstanceWhenNothingNeedsEscaping() {
    String value = "hive/host.example.com@EXAMPLE.COM тест 😀";

    Assert.assertSame(value, JsonEscapeUtil.escape(value));
  }

  @Test
  public void appendsEscapedTextOntoExistingBuilder() {
    StringBuilder builder = new StringBuilder("prefix:");

    JsonEscapeUtil.appendEscaped(builder, "a" + (char) 0x07 + "b");

    Assert.assertEquals("prefix:a\\u0007b", builder.toString());
  }
}
