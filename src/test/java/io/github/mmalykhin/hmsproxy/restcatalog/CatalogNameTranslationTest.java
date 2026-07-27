package io.github.mmalykhin.hmsproxy.restcatalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.junit.Test;

public class CatalogNameTranslationTest {
  private final CatalogNameTranslation translation = new CatalogNameTranslation("apache", "__");

  @Test
  public void toExternalPrependsCatalogPrefix() {
    assertEquals("apache__default", translation.toExternal("default"));
    assertEquals("apache__*", translation.toExternal("*"));
  }

  @Test
  public void fromExternalStripsOwnPrefixOnly() {
    assertEquals("default", translation.fromExternalOrNull("apache__default"));
    assertNull(translation.fromExternalOrNull("default"));
    assertNull(translation.fromExternalOrNull("hdp__default"));
    assertNull(translation.fromExternalOrNull("apache__"));
  }

  @Test
  public void internalNamesFiltersAndStrips() {
    assertEquals(List.of("default", "sales"),
        translation.internalNames(List.of("default", "apache__default", "hdp__x", "apache__sales")));
  }
}
