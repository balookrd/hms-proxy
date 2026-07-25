package io.github.mmalykhin.hmsproxy.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

public class DebugLogUtilTest {
  @Test
  public void formatsEmptyArgsWithoutRendering() {
    Assert.assertEquals("[]", DebugLogUtil.formatArgs(null));
    Assert.assertEquals("[]", DebugLogUtil.formatArgs(new Object[0]));
  }

  @Test
  public void rendersScalarsCollectionsAndMaps() {
    Assert.assertEquals("null", DebugLogUtil.formatValue(null));
    Assert.assertEquals("[a, 1, true]", DebugLogUtil.formatArgs(new Object[]{"a", 1, true}));
    Assert.assertEquals("[x, y]", DebugLogUtil.formatValue(List.of("x", "y")));
    Assert.assertEquals("{k=v}", DebugLogUtil.formatValue(Map.of("k", "v")));
  }

  @Test
  public void reportsOverflowForWideCollections() {
    List<Integer> values = java.util.stream.IntStream.range(0, 25).boxed().toList();

    String rendered = DebugLogUtil.formatValue(values);

    Assert.assertTrue(rendered, rendered.startsWith("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ... size=25]"));
  }

  @Test
  public void staysWithinTheCharBudget() {
    String wide = "x".repeat(9_000);

    String rendered = DebugLogUtil.formatValue(wide);

    Assert.assertTrue(rendered.endsWith("...<truncated>"));
    Assert.assertEquals(4_000 + "...<truncated>".length(), rendered.length());
  }

  @Test
  public void stopsRenderingElementsOnceTheBudgetIsSpent() {
    AtomicInteger renderedElements = new AtomicInteger();
    List<Object> elements = List.of(
        countingValue(renderedElements, 5_000),
        countingValue(renderedElements, 5_000),
        countingValue(renderedElements, 5_000));

    String rendered = DebugLogUtil.formatValue(elements);

    // Only the first oversized element is materialized; the rest are never toString()-ed.
    Assert.assertEquals(1, renderedElements.get());
    Assert.assertTrue(rendered, rendered.contains("...<truncated>"));
    Assert.assertTrue(rendered, rendered.endsWith("... size=3]"));
  }

  @Test
  public void reportsFormattingFailuresInsteadOfPropagating() {
    Object exploding = new Object() {
      @Override
      public String toString() {
        throw new IllegalStateException("boom");
      }
    };

    String rendered = DebugLogUtil.formatValue(exploding);

    Assert.assertTrue(rendered, rendered.startsWith("<debug-format-error value IllegalStateException"));
  }

  private static Object countingValue(AtomicInteger counter, int length) {
    return new Object() {
      @Override
      public String toString() {
        counter.incrementAndGet();
        return "y".repeat(length);
      }
    };
  }
}
