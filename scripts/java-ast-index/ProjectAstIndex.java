import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.ImportTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.tree.Tree;
import com.sun.source.tree.VariableTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.SourcePositions;
import com.sun.source.util.TreePath;
import com.sun.source.util.TreePathScanner;
import com.sun.source.util.Trees;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public final class ProjectAstIndex {
  private static final List<String> SOURCE_ROOTS = List.of("src/main/java", "src/test/java");
  private static final int MAX_REFERENCE_SAMPLES = 20;

  private final Path root;
  private final List<FileIndex> files = new ArrayList<>();
  private final Set<String> diagnostics = new LinkedHashSet<>();

  private ProjectAstIndex(Path root) {
    this.root = root;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: ProjectAstIndex <repo-root> <output-json>");
      System.exit(2);
    }

    Path root = Path.of(args[0]).toAbsolutePath().normalize();
    Path output = Path.of(args[1]).toAbsolutePath().normalize();
    ProjectAstIndex index = new ProjectAstIndex(root);
    index.build();
    index.write(output);
    index.writeTables(output.getParent());
  }

  private void build() throws IOException {
    List<Path> sources = findSources();
    if (sources.isEmpty()) {
      return;
    }

    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new IllegalStateException("JDK compiler is not available. Run with JDK 17, not a JRE.");
    }

    DiagnosticCollector<JavaFileObject> diagnosticCollector = new DiagnosticCollector<>();
    try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(
        diagnosticCollector, Locale.ROOT, StandardCharsets.UTF_8)) {
      Iterable<? extends JavaFileObject> javaFiles = fileManager.getJavaFileObjectsFromPaths(sources);
      JavacTask task = (JavacTask) compiler.getTask(
          null,
          fileManager,
          diagnosticCollector,
          List.of("-proc:none", "-XDallowStringFolding=false"),
          null,
          javaFiles);
      Iterable<? extends CompilationUnitTree> units = task.parse();
      Trees trees = Trees.instance(task);
      SourcePositions sourcePositions = trees.getSourcePositions();
      for (CompilationUnitTree unit : units) {
        files.add(indexUnit(unit, sourcePositions));
      }
    }

    for (Diagnostic<? extends JavaFileObject> diagnostic : diagnosticCollector.getDiagnostics()) {
      if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
        diagnostics.add(diagnostic.getMessage(Locale.ROOT));
      }
    }
    files.sort(Comparator.comparing(FileIndex::path));
  }

  private List<Path> findSources() throws IOException {
    List<Path> sources = new ArrayList<>();
    for (String sourceRoot : SOURCE_ROOTS) {
      Path directory = root.resolve(sourceRoot);
      if (!Files.isDirectory(directory)) {
        continue;
      }
      try (Stream<Path> stream = Files.walk(directory)) {
        stream
            .filter(path -> path.toString().endsWith(".java"))
            .sorted()
            .forEach(sources::add);
      }
    }
    return sources;
  }

  private FileIndex indexUnit(CompilationUnitTree unit, SourcePositions sourcePositions) {
    String path = root.relativize(Path.of(unit.getSourceFile().toUri())).toString();
    String packageName = unit.getPackageName() == null ? "" : unit.getPackageName().toString();
    List<String> imports = new ArrayList<>();
    for (ImportTree importTree : unit.getImports()) {
      imports.add(importTree.getQualifiedIdentifier().toString());
    }
    imports.sort(String::compareTo);

    UnitScanner scanner = new UnitScanner(unit, sourcePositions, packageName);
    scanner.scan(unit, null);

    return new FileIndex(path, packageName, imports, scanner.classes);
  }

  private void write(Path output) throws IOException {
    Files.createDirectories(output.getParent());
    try (Writer writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
      JsonWriter json = new JsonWriter(writer);
      json.beginObject();
      json.name("schemaVersion").value(2);
      json.name("generatedBy").value("scripts/build-java-ast-index.sh");
      json.name("sourceRoots").array(SOURCE_ROOTS);
      json.name("fileCount").value(files.size());
      json.name("diagnostics").array(new ArrayList<>(diagnostics));
      json.name("files");
      json.beginArray();
      for (FileIndex file : files) {
        json.beginObject();
        json.name("path").value(file.path());
        json.name("package").value(file.packageName());
        json.name("imports").array(file.imports());
        json.name("classes");
        json.beginArray();
        for (ClassIndex clazz : file.classes()) {
          writeClass(json, clazz);
        }
        json.endArray();
        json.endObject();
      }
      json.endArray();
      json.endObject();
    }
  }

  private void writeTables(Path outputDirectory) throws IOException {
    Files.createDirectories(outputDirectory);
    try (Writer symbols = Files.newBufferedWriter(outputDirectory.resolve("symbols.tsv"), StandardCharsets.UTF_8);
         Writer classes = Files.newBufferedWriter(outputDirectory.resolve("classes.tsv"), StandardCharsets.UTF_8);
         Writer methods = Files.newBufferedWriter(outputDirectory.resolve("methods.tsv"), StandardCharsets.UTF_8);
         Writer calls = Files.newBufferedWriter(outputDirectory.resolve("calls.tsv"), StandardCharsets.UTF_8);
         Writer news = Files.newBufferedWriter(outputDirectory.resolve("news.tsv"), StandardCharsets.UTF_8);
         Writer affectedTests = Files.newBufferedWriter(
             outputDirectory.resolve("affected-tests.tsv"), StandardCharsets.UTF_8)) {
      Map<String, Set<String>> callReferences = new LinkedHashMap<>();
      Map<String, Set<String>> newReferences = new LinkedHashMap<>();
      symbols.write("kind\tname\tqualifiedName\towner\tpath\tline\tdetail\n");
      classes.write("path\tline\tkind\tqualifiedName\textends\timplements\tannotations\tfields\n");
      methods.write("path\tline\tclass\tmethod\treturnType\tparameters\tannotations\tcalls\tnews\n");
      calls.write("call\tcount\tsampleReferences\n");
      news.write("new\tcount\tsampleReferences\n");
      affectedTests.write("productionClass\tproductionPath\tline\tlikelyTests\treasons\n");
      for (FileIndex file : files) {
        for (ClassIndex clazz : file.classes()) {
          writeTsv(symbols,
              clazz.kind(),
              clazz.name(),
              clazz.qualifiedName(),
              "",
              file.path(),
              Integer.toString(clazz.line()),
              classDetail(clazz));
          writeTsv(classes,
              file.path(),
              Integer.toString(clazz.line()),
              clazz.kind(),
              clazz.qualifiedName(),
              nullToEmpty(clazz.extendsName()),
              join(clazz.implementsNames()),
              join(clazz.annotations()),
              join(fieldNames(clazz.fields())));
          for (FieldIndex field : clazz.fields()) {
            writeTsv(symbols,
                "field",
                field.name(),
                clazz.qualifiedName() + "." + field.name(),
                clazz.qualifiedName(),
                file.path(),
                Integer.toString(field.line()),
                field.type());
          }
          for (MethodIndex method : clazz.methods()) {
            writeTsv(symbols,
                "method",
                method.name(),
                clazz.qualifiedName() + "." + method.name(),
                clazz.qualifiedName(),
                file.path(),
                Integer.toString(method.line()),
                methodDetail(method));
            writeTsv(methods,
                file.path(),
                Integer.toString(method.line()),
                clazz.qualifiedName(),
                method.name(),
                nullToEmpty(method.returnType()),
                join(method.parameters()),
                join(method.annotations()),
                join(method.calls()),
                join(method.news()));
            String reference = file.path() + ":" + method.line() + "#" + clazz.qualifiedName() + "." + method.name();
            for (String call : method.calls()) {
              callReferences.computeIfAbsent(call, ignored -> new LinkedHashSet<>()).add(reference);
            }
            for (String constructed : method.news()) {
              newReferences.computeIfAbsent(constructed, ignored -> new LinkedHashSet<>()).add(reference);
            }
          }
        }
      }
      writeReferenceTable(calls, callReferences);
      writeReferenceTable(news, newReferences);
      writeAffectedTests(affectedTests);
    }
  }

  private void writeAffectedTests(Writer writer) throws IOException {
    List<ProductionClass> productionClasses = productionClasses();
    List<TestFile> testFiles = testFiles();
    for (ProductionClass productionClass : productionClasses) {
      List<String> likelyTests = new ArrayList<>();
      List<String> reasons = new ArrayList<>();
      for (TestFile testFile : testFiles) {
        Set<String> testReasons = affectedTestReasons(productionClass, testFile);
        if (testReasons.isEmpty()) {
          continue;
        }
        likelyTests.add(testFile.reference());
        reasons.add(testFile.path() + "=" + join(new ArrayList<>(testReasons)));
      }
      if (!likelyTests.isEmpty()) {
        writeTsv(writer,
            productionClass.qualifiedName(),
            productionClass.path(),
            Integer.toString(productionClass.line()),
            join(likelyTests),
            join(reasons));
      }
    }
  }

  private List<ProductionClass> productionClasses() {
    List<ProductionClass> productionClasses = new ArrayList<>();
    for (FileIndex file : files) {
      if (!file.path().startsWith("src/main/java/")) {
        continue;
      }
      for (ClassIndex clazz : file.classes()) {
        if (!clazz.name().isEmpty()) {
          productionClasses.add(new ProductionClass(
              file.path(), file.packageName(), clazz.name(), clazz.qualifiedName(), clazz.line()));
        }
      }
    }
    productionClasses.sort(Comparator.comparing(ProductionClass::qualifiedName));
    return productionClasses;
  }

  private List<TestFile> testFiles() {
    List<TestFile> testFiles = new ArrayList<>();
    for (FileIndex file : files) {
      if (!file.path().startsWith("src/test/java/")) {
        continue;
      }
      Set<String> classNames = new LinkedHashSet<>();
      Set<String> constructedTypes = new LinkedHashSet<>();
      Set<String> signatureTypes = new LinkedHashSet<>();
      int firstLine = Integer.MAX_VALUE;
      for (ClassIndex clazz : file.classes()) {
        classNames.add(clazz.name());
        firstLine = Math.min(firstLine, clazz.line());
        for (FieldIndex field : clazz.fields()) {
          signatureTypes.add(field.type());
        }
        for (MethodIndex method : clazz.methods()) {
          signatureTypes.add(method.returnType());
          signatureTypes.addAll(method.parameters());
          constructedTypes.addAll(method.news());
        }
      }
      testFiles.add(new TestFile(
          file.path(),
          file.packageName(),
          file.imports(),
          new ArrayList<>(classNames),
          new ArrayList<>(constructedTypes),
          new ArrayList<>(signatureTypes),
          firstLine == Integer.MAX_VALUE ? -1 : firstLine));
    }
    return testFiles;
  }

  private static Set<String> affectedTestReasons(ProductionClass productionClass, TestFile testFile) {
    Set<String> reasons = new LinkedHashSet<>();
    for (String className : testFile.classNames()) {
      if (className.equals(productionClass.name() + "Test") || className.startsWith(productionClass.name())) {
        reasons.add("test-name");
      }
    }
    for (String importName : testFile.imports()) {
      if (importName.equals(productionClass.qualifiedName())) {
        reasons.add("import");
      } else if (importName.equals(productionClass.packageName() + ".*")) {
        reasons.add("wildcard-import");
      }
    }
    if (containsTypeName(testFile.constructedTypes(), productionClass.name(), productionClass.qualifiedName())) {
      reasons.add("new");
    }
    if (containsTypeName(testFile.signatureTypes(), productionClass.name(), productionClass.qualifiedName())) {
      reasons.add("signature");
    }
    return reasons;
  }

  private static boolean containsTypeName(List<String> values, String simpleName, String qualifiedName) {
    for (String value : values) {
      if (value == null) {
        continue;
      }
      if (value.equals(simpleName) || value.equals(qualifiedName) || value.contains(simpleName + " ")
          || value.contains("<" + simpleName) || value.contains("." + simpleName)
          || value.endsWith(" " + simpleName)) {
        return true;
      }
    }
    return false;
  }

  private static void writeReferenceTable(Writer writer, Map<String, Set<String>> references) throws IOException {
    List<String> keys = new ArrayList<>(references.keySet());
    keys.sort(String::compareTo);
    for (String key : keys) {
      List<String> allReferences = new ArrayList<>(references.get(key));
      List<String> samples = allReferences.subList(0, Math.min(MAX_REFERENCE_SAMPLES, allReferences.size()));
      writeTsv(writer, key, Integer.toString(allReferences.size()), join(samples));
    }
  }

  private static void writeTsv(Writer writer, String... values) throws IOException {
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        writer.write('\t');
      }
      writer.write(tsv(values[i]));
    }
    writer.write('\n');
  }

  private static String tsv(String value) {
    return value
        .replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\r", "\\r")
        .replace("\n", "\\n");
  }

  private static String join(List<String> values) {
    return String.join(",", values);
  }

  private static List<String> fieldNames(List<FieldIndex> fields) {
    List<String> names = new ArrayList<>();
    for (FieldIndex field : fields) {
      names.add(field.name());
    }
    return names;
  }

  private static String classDetail(ClassIndex clazz) {
    List<String> details = new ArrayList<>();
    if (clazz.extendsName() != null) {
      details.add("extends " + clazz.extendsName());
    }
    if (!clazz.implementsNames().isEmpty()) {
      details.add("implements " + join(clazz.implementsNames()));
    }
    return join(details);
  }

  private static String methodDetail(MethodIndex method) {
    String returnType = method.returnType() == null ? "" : method.returnType() + " ";
    return returnType + method.name() + "(" + join(method.parameters()) + ")";
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private void writeClass(JsonWriter json, ClassIndex clazz) throws IOException {
    json.beginObject();
    json.name("kind").value(clazz.kind());
    json.name("name").value(clazz.name());
    json.name("qualifiedName").value(clazz.qualifiedName());
    json.name("extends").value(clazz.extendsName());
    json.name("implements").array(clazz.implementsNames());
    json.name("annotations").array(clazz.annotations());
    json.name("line").value(clazz.line());
    json.name("fields");
    json.beginArray();
    for (FieldIndex field : clazz.fields()) {
      json.beginObject();
      json.name("name").value(field.name());
      json.name("type").value(field.type());
      json.name("line").value(field.line());
      json.endObject();
    }
    json.endArray();
    json.name("methods");
    json.beginArray();
    for (MethodIndex method : clazz.methods()) {
      json.beginObject();
      json.name("name").value(method.name());
      json.name("returnType").value(method.returnType());
      json.name("parameters").array(method.parameters());
      json.name("annotations").array(method.annotations());
      json.name("line").value(method.line());
      json.name("calls").array(method.calls());
      json.name("news").array(method.news());
      json.endObject();
    }
    json.endArray();
    json.endObject();
  }

  private static List<String> annotations(List<? extends Tree> modifiers) {
    List<String> annotations = new ArrayList<>();
    for (Tree annotation : modifiers) {
      annotations.add(annotation.toString());
    }
    annotations.sort(String::compareTo);
    return annotations;
  }

  private static String kindName(ClassTree tree) {
    return switch (tree.getKind()) {
      case ANNOTATION_TYPE -> "annotation";
      case CLASS -> "class";
      case ENUM -> "enum";
      case INTERFACE -> "interface";
      case RECORD -> "record";
      default -> tree.getKind().name().toLowerCase(Locale.ROOT);
    };
  }

  private static int lineOf(CompilationUnitTree unit, SourcePositions sourcePositions, Tree tree) {
    long position = sourcePositions.getStartPosition(unit, tree);
    if (position < 0) {
      return -1;
    }
    return (int) unit.getLineMap().getLineNumber(position);
  }

  private static String ownerName(ArrayDeque<String> classStack, String packageName, String name) {
    List<String> parts = new ArrayList<>(classStack);
    parts.add(name);
    String nestedName = String.join(".", parts);
    return packageName.isEmpty() ? nestedName : packageName + "." + nestedName;
  }

  private static final class UnitScanner extends TreePathScanner<Void, Void> {
    private final CompilationUnitTree unit;
    private final SourcePositions sourcePositions;
    private final String packageName;
    private final ArrayDeque<String> classStack = new ArrayDeque<>();
    private final List<ClassIndex> classes = new ArrayList<>();

    private UnitScanner(CompilationUnitTree unit, SourcePositions sourcePositions, String packageName) {
      this.unit = unit;
      this.sourcePositions = sourcePositions;
      this.packageName = packageName;
    }

    @Override
    public Void visitClass(ClassTree tree, Void unused) {
      String name = tree.getSimpleName().toString();
      String qualifiedName = ownerName(classStack, packageName, name);
      List<FieldIndex> fields = new ArrayList<>();
      List<MethodIndex> methods = new ArrayList<>();

      classStack.addLast(name);
      for (Tree member : tree.getMembers()) {
        if (member instanceof VariableTree variable) {
          fields.add(new FieldIndex(
              variable.getName().toString(),
              variable.getType() == null ? "" : variable.getType().toString(),
              lineOf(unit, sourcePositions, variable)));
        } else if (member instanceof MethodTree method) {
          methods.add(indexMethod(method));
        } else {
          scan(member, unused);
        }
      }
      classStack.removeLast();

      List<String> implementsNames = new ArrayList<>();
      for (Tree implement : tree.getImplementsClause()) {
        implementsNames.add(implement.toString());
      }
      ClassIndex clazz = new ClassIndex(
          kindName(tree),
          name,
          qualifiedName,
          tree.getExtendsClause() == null ? null : tree.getExtendsClause().toString(),
          implementsNames,
          annotations(tree.getModifiers().getAnnotations()),
          lineOf(unit, sourcePositions, tree),
          fields,
          methods);
      classes.add(clazz);
      return null;
    }

    private MethodIndex indexMethod(MethodTree method) {
      InvocationScanner invocationScanner = new InvocationScanner();
      invocationScanner.scan(new TreePath(getCurrentPath(), method), null);

      List<String> parameters = new ArrayList<>();
      for (VariableTree parameter : method.getParameters()) {
        parameters.add(parameter.getType() + " " + parameter.getName());
      }
      return new MethodIndex(
          method.getName().toString(),
          method.getReturnType() == null ? null : method.getReturnType().toString(),
          parameters,
          annotations(method.getModifiers().getAnnotations()),
          lineOf(unit, sourcePositions, method),
          invocationScanner.calls(),
          invocationScanner.news());
    }
  }

  private static final class InvocationScanner extends TreePathScanner<Void, Void> {
    private final Set<String> calls = new LinkedHashSet<>();
    private final Set<String> news = new LinkedHashSet<>();

    @Override
    public Void visitMethodInvocation(MethodInvocationTree tree, Void unused) {
      calls.add(methodName(tree.getMethodSelect()));
      return super.visitMethodInvocation(tree, unused);
    }

    @Override
    public Void visitNewClass(NewClassTree tree, Void unused) {
      if (tree.getIdentifier() != null) {
        news.add(tree.getIdentifier().toString());
      }
      return super.visitNewClass(tree, unused);
    }

    private List<String> calls() {
      return sorted(calls);
    }

    private List<String> news() {
      return sorted(news);
    }

    private static String methodName(Tree tree) {
      if (tree instanceof MemberSelectTree memberSelect) {
        return memberSelect.getIdentifier().toString();
      }
      if (tree instanceof IdentifierTree identifier) {
        return identifier.getName().toString();
      }
      return tree.toString();
    }

    private static List<String> sorted(Set<String> values) {
      List<String> sorted = new ArrayList<>(values);
      sorted.sort(String::compareTo);
      return sorted;
    }
  }

  private record FileIndex(
      String path,
      String packageName,
      List<String> imports,
      List<ClassIndex> classes) {
  }

  private record ClassIndex(
      String kind,
      String name,
      String qualifiedName,
      String extendsName,
      List<String> implementsNames,
      List<String> annotations,
      int line,
      List<FieldIndex> fields,
      List<MethodIndex> methods) {
  }

  private record FieldIndex(
      String name,
      String type,
      int line) {
  }

  private record MethodIndex(
      String name,
      String returnType,
      List<String> parameters,
      List<String> annotations,
      int line,
      List<String> calls,
      List<String> news) {
  }

  private record ProductionClass(
      String path,
      String packageName,
      String name,
      String qualifiedName,
      int line) {
  }

  private record TestFile(
      String path,
      String packageName,
      List<String> imports,
      List<String> classNames,
      List<String> constructedTypes,
      List<String> signatureTypes,
      int line) {
    private String reference() {
      String name = classNames.isEmpty() ? path : classNames.get(0);
      return path + ":" + line + "#" + name;
    }
  }

  private static final class JsonWriter {
    private final Writer writer;
    private final ArrayDeque<Scope> scopes = new ArrayDeque<>();
    private boolean afterName;

    private JsonWriter(Writer writer) {
      this.writer = Objects.requireNonNull(writer, "writer");
    }

    private JsonWriter beginObject() throws IOException {
      beforeValue();
      writer.write('{');
      scopes.addLast(new Scope(true));
      afterName = false;
      return this;
    }

    private JsonWriter endObject() throws IOException {
      writer.write('}');
      scopes.removeLast();
      afterName = false;
      return this;
    }

    private JsonWriter beginArray() throws IOException {
      beforeValue();
      writer.write('[');
      scopes.addLast(new Scope(false));
      afterName = false;
      return this;
    }

    private JsonWriter endArray() throws IOException {
      writer.write(']');
      scopes.removeLast();
      afterName = false;
      return this;
    }

    private JsonWriter name(String name) throws IOException {
      Scope scope = scopes.peekLast();
      if (scope == null || !scope.object) {
        throw new IllegalStateException("JSON name outside object");
      }
      beforeElement(scope);
      string(name);
      writer.write(':');
      afterName = true;
      return this;
    }

    private JsonWriter value(String value) throws IOException {
      beforeValue();
      if (value == null) {
        writer.write("null");
      } else {
        string(value);
      }
      return this;
    }

    private JsonWriter value(int value) throws IOException {
      beforeValue();
      writer.write(Integer.toString(value));
      return this;
    }

    private JsonWriter array(List<String> values) throws IOException {
      beginArray();
      for (String value : values) {
        value(value);
      }
      endArray();
      return this;
    }

    private void beforeValue() throws IOException {
      if (afterName) {
        afterName = false;
        return;
      }
      Scope scope = scopes.peekLast();
      if (scope != null && !scope.object) {
        beforeElement(scope);
      }
    }

    private void beforeElement(Scope scope) throws IOException {
      if (scope.first) {
        scope.first = false;
      } else {
        writer.write(',');
      }
    }

    private void string(String value) throws IOException {
      writer.write('"');
      for (int i = 0; i < value.length(); i++) {
        char ch = value.charAt(i);
        switch (ch) {
          case '"' -> writer.write("\\\"");
          case '\\' -> writer.write("\\\\");
          case '\b' -> writer.write("\\b");
          case '\f' -> writer.write("\\f");
          case '\n' -> writer.write("\\n");
          case '\r' -> writer.write("\\r");
          case '\t' -> writer.write("\\t");
          default -> {
            if (ch < 0x20) {
              writer.write(String.format("\\u%04x", (int) ch));
            } else {
              writer.write(ch);
            }
          }
        }
      }
      writer.write('"');
    }

    private static final class Scope {
      private final boolean object;
      private boolean first = true;

      private Scope(boolean object) {
        this.object = object;
      }
    }
  }
}
