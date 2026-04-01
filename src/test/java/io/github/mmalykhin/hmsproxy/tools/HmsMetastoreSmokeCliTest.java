package io.github.mmalykhin.hmsproxy.tools;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class HmsMetastoreSmokeCliTest {
  @Test
  public void cliArgsParsesRepeatedAndCommaSeparatedValues() throws Exception {
    Object cli = parse("--files-added", "a,b", "--files-added", "c", "--partition", "p=1");

    Assert.assertEquals(List.of("a", "b", "c"), invokeList(cli, "requiredList", "files-added"));
    Assert.assertEquals(List.of("p=1"), invokeList(cli, "list", "partition"));
  }

  @Test
  public void cliArgsParsesExtraConfPairs() throws Exception {
    Object cli = parse("--conf", "a=b", "--conf", "c=d=e");

    @SuppressWarnings("unchecked")
    Method confMethod = cli.getClass().getDeclaredMethod("conf");
    confMethod.setAccessible(true);
    Map<String, String> conf = (Map<String, String>) confMethod.invoke(cli);
    Assert.assertEquals("b", conf.get("a"));
    Assert.assertEquals("d=e", conf.get("c"));
  }

  private static Object parse(String... args) throws Exception {
    Class<?> cliClass = Class.forName("io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli$CliArgs");
    Method parse = cliClass.getDeclaredMethod("parse", String[].class);
    parse.setAccessible(true);
    return parse.invoke(null, (Object) args);
  }

  @SuppressWarnings("unchecked")
  private static List<String> invokeList(Object target, String methodName, String key) throws Exception {
    Method method = target.getClass().getDeclaredMethod(methodName, String.class);
    method.setAccessible(true);
    return (List<String>) method.invoke(target, key);
  }
}
