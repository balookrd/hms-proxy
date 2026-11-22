package io.github.mmalykhin.hmsproxy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class PackageDependencyAcyclicityTest {
  private static final String BASE_PACKAGE = "io.github.mmalykhin.hmsproxy";
  private static final Set<String> CORE_PACKAGES = Set.of(
      "util",
      "thriftbridge",
      "config",
      "observability",
      "security",
      "compatibility",
      "backend",
      "frontend"
  );
  private static final Pattern PACKAGE_PATTERN =
      Pattern.compile("(?m)^\\s*package\\s+" + Pattern.quote(BASE_PACKAGE) + "(?:\\.([a-z0-9_]+))?(?:\\.[a-z0-9_.]+)?\\s*;");
  private static final Pattern IMPORT_PATTERN =
      Pattern.compile("(?m)^\\s*import\\s+(?:static\\s+)?" + Pattern.quote(BASE_PACKAGE) + "\\.([a-z0-9_]+)\\.");
  private static final Pattern FQN_PATTERN =
      Pattern.compile(Pattern.quote(BASE_PACKAGE) + "\\.([a-z0-9_]+)(?:\\.[a-z0-9_]+)*\\.[A-Z][A-Za-z0-9_]*");

  @Test
  public void corePackagesFormDirectedAcyclicGraph() throws IOException {
    Path sourceRoot = Path.of("src", "main", "java", "io", "github", "mmalykhin", "hmsproxy");
    Assert.assertTrue("Source root must exist: " + sourceRoot.toAbsolutePath(), Files.isDirectory(sourceRoot));

    Map<String, Map<String, Set<String>>> edges = new LinkedHashMap<>();
    for (String pkg : CORE_PACKAGES) {
      edges.put(pkg, new LinkedHashMap<>());
    }

    try (Stream<Path> files = Files.walk(sourceRoot)) {
      List<Path> javaFiles = files.filter(path -> path.toString().endsWith(".java")).sorted().toList();
      for (Path file : javaFiles) {
        String source = stripCommentsAndStrings(Files.readString(file, StandardCharsets.UTF_8));
        Matcher pkgMatcher = PACKAGE_PATTERN.matcher(source);
        if (!pkgMatcher.find()) {
          continue;
        }
        String srcPackage = pkgMatcher.group(1);
        if (srcPackage == null || !CORE_PACKAGES.contains(srcPackage)) {
          continue;
        }
        String fileName = sourceRoot.relativize(file).toString();
        recordTargets(source, IMPORT_PATTERN, srcPackage, fileName, edges);
        recordTargets(source, FQN_PATTERN, srcPackage, fileName, edges);
      }
    }

    assertNoEdge(edges, "security", "backend");
    assertNoEdge(edges, "security", "frontend");
    assertNoEdge(edges, "observability", "security");

    List<String> cycle = findCycle(edges);
    Assert.assertNull(
        cycle == null ? "" : "Found package dependency cycle among core packages: " + String.join(" -> ", cycle)
            + " with refs=" + formatCycleRefs(edges, cycle),
        cycle);
  }

  private static void recordTargets(
      String source,
      Pattern pattern,
      String srcPackage,
      String fileName,
      Map<String, Map<String, Set<String>>> edges
  ) {
    Matcher matcher = pattern.matcher(source);
    while (matcher.find()) {
      String targetPackage = matcher.group(1);
      if (targetPackage != null && !targetPackage.equals(srcPackage) && CORE_PACKAGES.contains(targetPackage)) {
        edges.get(srcPackage).computeIfAbsent(targetPackage, k -> new LinkedHashSet<>()).add(fileName);
      }
    }
  }

  private static void assertNoEdge(
      Map<String, Map<String, Set<String>>> edges,
      String from,
      String to
  ) {
    Set<String> refs = edges.getOrDefault(from, Collections.emptyMap()).getOrDefault(to, Collections.emptySet());
    Assert.assertTrue("Forbidden dependency edge " + from + " -> " + to + " via " + refs, refs.isEmpty());
  }

  private static List<String> findCycle(Map<String, Map<String, Set<String>>> edges) {
    Map<String, Integer> color = new LinkedHashMap<>();
    List<String> stack = new ArrayList<>();
    for (String node : CORE_PACKAGES.stream().sorted().toList()) {
      if (color.getOrDefault(node, 0) == 0) {
        List<String> cycle = dfs(node, edges, color, stack);
        if (cycle != null) {
          return cycle;
        }
      }
    }
    return null;
  }

  private static List<String> dfs(
      String node,
      Map<String, Map<String, Set<String>>> edges,
      Map<String, Integer> color,
      List<String> stack
  ) {
    color.put(node, 1);
    stack.add(node);
    List<String> neighbors = new ArrayList<>(edges.getOrDefault(node, Collections.emptyMap()).keySet());
    Collections.sort(neighbors);
    for (String next : neighbors) {
      int nextColor = color.getOrDefault(next, 0);
      if (nextColor == 1) {
        int startIndex = stack.indexOf(next);
        List<String> cycle = new ArrayList<>(stack.subList(startIndex, stack.size()));
        cycle.add(next);
        return cycle;
      }
      if (nextColor == 0) {
        List<String> found = dfs(next, edges, color, stack);
        if (found != null) {
          return found;
        }
      }
    }
    stack.remove(stack.size() - 1);
    color.put(node, 2);
    return null;
  }

  private static String formatCycleRefs(Map<String, Map<String, Set<String>>> edges, List<String> cycle) {
    if (cycle == null || cycle.size() < 2) {
      return "{}";
    }
    Map<String, Set<String>> refs = new LinkedHashMap<>();
    for (int i = 0; i < cycle.size() - 1; i++) {
      String from = cycle.get(i);
      String to = cycle.get(i + 1);
      refs.put(from + "->" + to, edges.getOrDefault(from, Collections.emptyMap()).getOrDefault(to, Set.of()));
    }
    return refs.toString();
  }

  private static String stripCommentsAndStrings(String source) {
    StringBuilder out = new StringBuilder(source.length());
    int n = source.length();
    int i = 0;
    while (i < n) {
      char c = source.charAt(i);
      if (c == '/' && i + 1 < n && source.charAt(i + 1) == '/') {
        i += 2;
        while (i < n && source.charAt(i) != '\n') {
          i++;
        }
      } else if (c == '/' && i + 1 < n && source.charAt(i + 1) == '*') {
        i += 2;
        while (i + 1 < n && !(source.charAt(i) == '*' && source.charAt(i + 1) == '/')) {
          i++;
        }
        i = Math.min(n, i + 2);
      } else if (c == '"') {
        out.append(' ');
        i++;
        while (i < n) {
          char sc = source.charAt(i++);
          if (sc == '\\' && i < n) {
            i++;
          } else if (sc == '"') {
            break;
          }
        }
      } else if (c == '\'') {
        out.append(' ');
        i++;
        while (i < n) {
          char sc = source.charAt(i++);
          if (sc == '\\' && i < n) {
            i++;
          } else if (sc == '\'') {
            break;
          }
        }
      } else {
        out.append(c);
        i++;
      }
    }
    return out.toString();
  }
}
