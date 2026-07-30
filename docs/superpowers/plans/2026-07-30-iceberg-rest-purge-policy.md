# Iceberg REST purge policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give `DELETE /v1/{prefix}/namespaces/{ns}/tables/{tbl}?purgeRequested=true` an explicit,
configurable boundary (`rest-catalog.purge.mode` = `ALLOW` / `ALLOWLIST` / `REFUSE`) without
changing what today's deployments get by default.

**Architecture:** A new `IcebergPurgePolicy` answers both purge questions - "may this purge run at
all?" (from the table's `TableMetadata`) and "may this path be deleted?" (through a
`PrefixGuardedFileIO` decorator). `RoutingHiveCatalog.dropTable` is the single call site: in
`ALLOW` it delegates straight to `HiveCatalog`, otherwise it consults the policy and then performs
Iceberg's own two steps itself (metastore drop, then `CatalogUtil.dropTableData` with the guarded
FileIO). Refusals surface as `ForbiddenException`, which the vendored `RESTCatalogAdapter` already
maps to `403`.

**Tech Stack:** Java 17, Maven, JUnit 4, Iceberg 1.9.2 (`iceberg-core`, `iceberg-hive-metastore`),
Hadoop `FileSystem`/`Path`, slf4j.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-30-iceberg-rest-purge-policy-design.md`.
- JDK 17 only: `export JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19`.
  Tests touching `FileSystem`/UGI silently self-exclude on newer JVMs.
- Build/test offline: `mvn -o test`, `mvn -o -Dtest=<Class> test`.
- Default stays `ALLOW`: `dropTableWithPurgeDeletesDataFiles`, `dropTableWithoutPurgeKeepsDataFiles`,
  `dropTableWithPurgeUnderFederatedNamespaceDeletesNothing` and
  `dropTableWithPurgeReadsManifestsOfATableAskingForSnappy` must pass unmodified.
- Config parsing is strict: `ConfigParsing.parseEnum` for the enum, contradictory combinations
  throw at startup, no silent fallback.
- Java style: 2-space indent, explicit imports, small classes, comments only where they explain
  non-obvious compatibility/security/routing behaviour.
- Do not run `git commit` or `git push` - the user commits. Steps say "stage" where a commit would
  normally go; leave the working tree with the changes staged only if the user asks.
- Documentation changes come in EN/RU pairs (`README.md`+`README.ru.md`,
  `CHANGELOG.md`+`CHANGELOG.ru.md`).

## File Structure

| File | Responsibility |
| --- | --- |
| `src/main/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlist.java` (create) | Parse a comma-separated prefix list; match a path against it with `/`-boundary semantics. Pure strings, no Hadoop. |
| `src/main/java/io/github/mmalykhin/hmsproxy/routing/FileSystemExternalTableDropPurger.java` (modify) | Drop its private list parsing/matching, call the shared util. |
| `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogPurgeMode.java` (create) | The enum. |
| `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfig.java` (modify) | Two new components: `purgeMode`, `purgeAllowedPrefixes`. |
| `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfigParser.java` (modify) | Parse both keys, reject the two contradictory combinations. |
| `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIO.java` (create) | FileIO decorator constraining only the delete methods. |
| `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicy.java` (create) | The single place deciding whether a purge may run and which paths it may delete. |
| `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingHiveCatalog.java` (modify) | `dropTable` override applying the policy. |
| `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java` (modify) | Accept the policy, hand it to the catalog. |
| `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServices.java` (modify) | Build one policy from `ProxyConfig`, pass it to each service. |

---

### Task 1: Shared path-prefix allowlist

Extracts the prefix semantics the Thrift purger already has into a util both purge paths use, so
the two cannot drift apart in how they read a prefix.

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlist.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/FileSystemExternalTableDropPurger.java:101-153`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlistTest.java`

**Interfaces:**
- Produces:
  - `public static List<String> parse(String commaSeparatedOrNull)` - trims, drops blanks, never null.
  - `public static boolean matches(String location, List<String> prefixes)` - exact match or match
    with a `/` boundary; `false` for a null location or an empty list.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlistTest.java`:

```java
package io.github.mmalykhin.hmsproxy.util;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class PathPrefixAllowlistTest {
  @Test
  public void parseTrimsAndDropsBlankEntries() {
    Assert.assertEquals(
        List.of("hdfs://ns/a", "hdfs://ns/b"),
        PathPrefixAllowlist.parse(" hdfs://ns/a , ,hdfs://ns/b "));
  }

  @Test
  public void parseOfNullOrBlankIsEmpty() {
    Assert.assertEquals(List.of(), PathPrefixAllowlist.parse(null));
    Assert.assertEquals(List.of(), PathPrefixAllowlist.parse("   "));
  }

  @Test
  public void matchesOnPathSeparatorBoundaryOnly() {
    List<String> prefixes = List.of("hdfs://ns/warehouse/db");
    Assert.assertTrue(PathPrefixAllowlist.matches("hdfs://ns/warehouse/db/t/data.parquet", prefixes));
    Assert.assertTrue("the prefix itself is inside its own tree",
        PathPrefixAllowlist.matches("hdfs://ns/warehouse/db", prefixes));
    Assert.assertFalse("a sibling directory sharing the name prefix must not match",
        PathPrefixAllowlist.matches("hdfs://ns/warehouse/dbx/t/data.parquet", prefixes));
  }

  @Test
  public void matchesHonoursATrailingSlashInTheConfiguredPrefix() {
    Assert.assertTrue(PathPrefixAllowlist.matches(
        "hdfs://ns/warehouse/db/t", List.of("hdfs://ns/warehouse/db/")));
    Assert.assertFalse(PathPrefixAllowlist.matches(
        "hdfs://ns/warehouse/dbx", List.of("hdfs://ns/warehouse/db/")));
  }

  @Test
  public void emptyAllowlistMatchesNothingAndNullLocationNeverMatches() {
    Assert.assertFalse(PathPrefixAllowlist.matches("hdfs://ns/a", List.of()));
    Assert.assertFalse(PathPrefixAllowlist.matches(null, List.of("hdfs://ns/a")));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=PathPrefixAllowlistTest test
```

Expected: compilation failure - `cannot find symbol: class PathPrefixAllowlist`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlist.java`:

```java
package io.github.mmalykhin.hmsproxy.util;

import java.util.Arrays;
import java.util.List;

/**
 * Comma-separated path-prefix allowlist shared by the two purge paths (the Thrift listener's
 * external-table purge and the Iceberg REST purge), so a prefix means the same thing in both.
 * Matching is on a path-separator boundary, never a bare string prefix: "hdfs://ns/db" must not
 * cover "hdfs://ns/dbx". Locations are expected already qualified by the caller - this class
 * never touches a FileSystem.
 */
public final class PathPrefixAllowlist {
  private PathPrefixAllowlist() {
  }

  public static List<String> parse(String commaSeparatedOrNull) {
    if (commaSeparatedOrNull == null || commaSeparatedOrNull.isBlank()) {
      return List.of();
    }
    return Arrays.stream(commaSeparatedOrNull.split(","))
        .map(String::trim)
        .filter(value -> !value.isEmpty())
        .toList();
  }

  public static boolean matches(String location, List<String> prefixes) {
    if (location == null) {
      return false;
    }
    for (String prefix : prefixes) {
      String normalizedPrefix = prefix.trim();
      if (normalizedPrefix.isEmpty()) {
        continue;
      }
      if (location.equals(normalizedPrefix)) {
        return true;
      }
      String boundaryPrefix = normalizedPrefix.endsWith("/") ? normalizedPrefix : normalizedPrefix + "/";
      if (location.startsWith(boundaryPrefix)) {
        return true;
      }
    }
    return false;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=PathPrefixAllowlistTest test
```

Expected: PASS.

- [ ] **Step 5: Point the Thrift purger at the shared util**

In `FileSystemExternalTableDropPurger.java`, add the import
`io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist`, delete the private
`matchesAllowedPrefixes` method entirely, and replace the body of `allowedPrefixes` plus its one
call site:

```java
  private List<String> allowedPrefixes(CatalogBackend backend) {
    CatalogConfig catalogConfig = config.catalogs().get(backend.name());
    if (catalogConfig == null) {
      return List.of();
    }
    return PathPrefixAllowlist.parse(catalogConfig.hiveConf().get(ALLOWED_PREFIXES_CONF_KEY));
  }
```

and in `prepare`:

```java
    if (!PathPrefixAllowlist.matches(location, allowedPrefixes)) {
```

Remove the now-unused `java.util.Arrays` import.

- [ ] **Step 6: Run the Thrift purge suite to verify no behaviour changed**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=RoutingMetaStoreProxyDropPurgeTest+PathPrefixAllowlistTest test
```

Expected: PASS, no test edits needed.

- [ ] **Step 7: Stage**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlist.java src/test/java/io/github/mmalykhin/hmsproxy/util/PathPrefixAllowlistTest.java src/main/java/io/github/mmalykhin/hmsproxy/routing/FileSystemExternalTableDropPurger.java
```

---

### Task 2: Configuration keys

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogPurgeMode.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfig.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfigParser.java`
- Modify (constructor call sites): `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RestCatalogServerTest.java`,
  `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServicesTest.java`,
  `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`,
  `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/SpnegoIntegrationTest.java` (any file that
  calls `new RestCatalogConfig(...)`; find them with
  `grep -rln "new RestCatalogConfig(" src/test`)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfigParserTest.java`

**Interfaces:**
- Consumes: `PathPrefixAllowlist.parse` from Task 1.
- Produces:
  - `enum RestCatalogPurgeMode { ALLOW, ALLOWLIST, REFUSE }`
  - `RestCatalogConfig(boolean enabled, String bindHost, int port, int minWorkerThreads,
    int maxWorkerThreads, String kerberosPrincipal, String kerberosKeytab,
    RestCatalogPurgeMode purgeMode, List<String> purgeAllowedPrefixes)` - the two new components
    are last, `purgeAllowedPrefixes` is never null (empty list when unset).

- [ ] **Step 1: Write the failing tests**

Append to `RestCatalogConfigParserTest` (keep the existing `loadRestConfig` helper and
`COMMON_PROPS`; read the top of the file first to match them):

```java
  @Test
  public void purgeModeDefaultsToAllowWithNoPrefixes() throws Exception {
    RestCatalogConfig config = loadRestConfig("server.port=9083\n" + COMMON_PROPS);
    Assert.assertEquals(RestCatalogPurgeMode.ALLOW, config.purgeMode());
    Assert.assertEquals(List.of(), config.purgeAllowedPrefixes());
  }

  @Test
  public void purgeModeIsCaseInsensitiveAndPrefixesAreSplit() throws Exception {
    RestCatalogConfig config = loadRestConfig("server.port=9083\n" + COMMON_PROPS
        + "rest-catalog.purge.mode=allowlist\n"
        + "rest-catalog.purge.allowed-prefixes=hdfs://ns/warehouse/, hdfs://ns/tmp/\n");
    Assert.assertEquals(RestCatalogPurgeMode.ALLOWLIST, config.purgeMode());
    Assert.assertEquals(
        List.of("hdfs://ns/warehouse/", "hdfs://ns/tmp/"), config.purgeAllowedPrefixes());
  }

  @Test
  public void unknownPurgeModeIsRejectedWithTheAcceptedValues() {
    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.mode=BEST_EFFORT\n"));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("rest-catalog.purge.mode"));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("ALLOWLIST"));
  }

  @Test
  public void allowlistModeWithoutPrefixesIsRejected() {
    IllegalArgumentException e = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.mode=ALLOWLIST\n"));
    Assert.assertTrue(e.getMessage(),
        e.getMessage().contains("rest-catalog.purge.allowed-prefixes"));
  }

  @Test
  public void prefixesWithoutAllowlistModeAreRejected() {
    IllegalArgumentException allow = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.allowed-prefixes=hdfs://ns/warehouse/\n"));
    Assert.assertTrue(allow.getMessage(), allow.getMessage().contains("ALLOWLIST"));
    IllegalArgumentException refuse = Assert.assertThrows(IllegalArgumentException.class,
        () -> loadRestConfig("server.port=9083\n" + COMMON_PROPS
            + "rest-catalog.purge.mode=REFUSE\n"
            + "rest-catalog.purge.allowed-prefixes=hdfs://ns/warehouse/\n"));
    Assert.assertTrue(refuse.getMessage(), refuse.getMessage().contains("ALLOWLIST"));
  }
```

Add the imports the new cases need: `java.util.List`,
`io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode` (same package - no import
needed if the test lives in `config.restcatalog`; check the test's package declaration).

- [ ] **Step 2: Run the tests to verify they fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=RestCatalogConfigParserTest test
```

Expected: compilation failure - `cannot find symbol: RestCatalogPurgeMode` / `method purgeMode()`.

- [ ] **Step 3: Add the enum**

Create `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogPurgeMode.java`:

```java
package io.github.mmalykhin.hmsproxy.config.restcatalog;

/** What the REST front door does with {@code DELETE ...?purgeRequested=true}. */
public enum RestCatalogPurgeMode {
  /** Delete whatever the table's metadata and manifests point at - the Iceberg REST default. */
  ALLOW,
  /** Delete only under rest-catalog.purge.allowed-prefixes; refuse or skip anything else. */
  ALLOWLIST,
  /** Refuse every purge request with 403; a drop without the parameter still works. */
  REFUSE
}
```

- [ ] **Step 4: Extend the config record**

Rewrite `RestCatalogConfig.java`:

```java
package io.github.mmalykhin.hmsproxy.config.restcatalog;

import java.util.List;

public record RestCatalogConfig(
    boolean enabled,
    String bindHost,
    int port,
    int minWorkerThreads,
    int maxWorkerThreads,
    String kerberosPrincipal,
    String kerberosKeytab,
    RestCatalogPurgeMode purgeMode,
    List<String> purgeAllowedPrefixes
) {
  public RestCatalogConfig {
    purgeMode = purgeMode == null ? RestCatalogPurgeMode.ALLOW : purgeMode;
    purgeAllowedPrefixes = purgeAllowedPrefixes == null ? List.of() : List.copyOf(purgeAllowedPrefixes);
  }

  public static RestCatalogConfig disabled() {
    return new RestCatalogConfig(
        false, "0.0.0.0", 8181, 8, 64, null, null, RestCatalogPurgeMode.ALLOW, List.of());
  }

  public boolean kerberosEnabled() {
    return kerberosPrincipal != null && kerberosKeytab != null;
  }
}
```

- [ ] **Step 5: Parse and validate**

In `RestCatalogConfigParser.parse`, before the `return`:

```java
    RestCatalogPurgeMode purgeMode = ConfigParsing.parseEnum(
        RestCatalogPurgeMode.class,
        reader.getOrNull("rest-catalog.purge.mode"),
        "rest-catalog.purge.mode",
        RestCatalogPurgeMode.ALLOW);
    List<String> purgeAllowedPrefixes =
        PathPrefixAllowlist.parse(reader.getOrNull("rest-catalog.purge.allowed-prefixes"));
    // Contradictions fail the start, checked whether or not the listener is enabled: a typo that
    // only surfaces once someone turns the listener on in production is exactly what strict
    // parsing exists to prevent.
    if (purgeMode == RestCatalogPurgeMode.ALLOWLIST && purgeAllowedPrefixes.isEmpty()) {
      throw new IllegalArgumentException(
          "rest-catalog.purge.mode=ALLOWLIST requires a non-empty rest-catalog.purge.allowed-prefixes");
    }
    if (purgeMode != RestCatalogPurgeMode.ALLOWLIST && !purgeAllowedPrefixes.isEmpty()) {
      throw new IllegalArgumentException(
          "rest-catalog.purge.allowed-prefixes is only used with rest-catalog.purge.mode=ALLOWLIST, but the mode is "
              + purgeMode);
    }
```

and widen the returned record:

```java
    return new RestCatalogConfig(
        enabled, bindHost, port, minWorkerThreads, maxWorkerThreads, principal, keytab,
        purgeMode, purgeAllowedPrefixes);
```

Add imports: `io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist`, `java.util.List`.

- [ ] **Step 6: Fix the existing constructor call sites**

```bash
grep -rn "new RestCatalogConfig(" src/test src/main
```

Every 7-argument call becomes a 9-argument one by appending
`, RestCatalogPurgeMode.ALLOW, List.of()`, with the import
`io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode` (and `java.util.List` where
missing). Do not add a convenience 7-argument constructor: the explicit call sites are what makes a
future third mode visible at every place a listener is built.

- [ ] **Step 7: Run the config and REST suites**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest='RestCatalogConfigParserTest+ProxyConfigLoaderTest+RestCatalogServerTest+IcebergRestServicesTest' test
```

Expected: PASS.

- [ ] **Step 8: Stage**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog src/test/java/io/github/mmalykhin/hmsproxy/config/restcatalog src/test/java/io/github/mmalykhin/hmsproxy/restcatalog
```

---

### Task 3: Delete-guarding FileIO decorator

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIO.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIOTest.java`

**Interfaces:**
- Consumes: `PathPrefixAllowlist.matches` (Task 1).
- Produces:
  - `PrefixGuardedFileIO(FileIO delegate, List<String> qualifiedPrefixes, Configuration conf, String tableName)`
  - implements `org.apache.iceberg.io.FileIO`; only `deleteFile(String)`, `deleteFile(InputFile)`
    and `deleteFile(OutputFile)` are constrained, everything else delegates.
  - `List<String> skippedPaths()` - test/observability accessor listing what it refused to delete.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIOTest.java`:

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PrefixGuardedFileIOTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void deletesInsideTheAllowlistAndSkipsOutside() throws Exception {
    File inside = tempFolder.newFolder("inside");
    File outside = tempFolder.newFolder("outside");
    File allowed = new File(inside, "a.parquet");
    File refused = new File(outside, "b.parquet");
    Files.write(allowed.toPath(), new byte[] {1});
    Files.write(refused.toPath(), new byte[] {1});

    PrefixGuardedFileIO io = new PrefixGuardedFileIO(
        new HadoopFileIO(new Configuration()),
        List.of("file:" + inside.getAbsolutePath()),
        new Configuration(),
        "default.t");

    io.deleteFile("file:" + allowed.getAbsolutePath());
    io.deleteFile("file:" + refused.getAbsolutePath());

    Assert.assertFalse("a path inside the allowlist must be deleted", allowed.exists());
    Assert.assertTrue("a path outside the allowlist must survive", refused.exists());
    Assert.assertEquals(1, io.skippedPaths().size());
    Assert.assertTrue(io.skippedPaths().toString(),
        io.skippedPaths().get(0).contains(refused.getName()));
  }

  @Test
  public void anUnqualifiedPathIsMatchedAfterQualification() throws Exception {
    File inside = tempFolder.newFolder("qualify_me");
    File allowed = new File(inside, "c.parquet");
    Files.write(allowed.toPath(), new byte[] {1});

    PrefixGuardedFileIO io = new PrefixGuardedFileIO(
        new HadoopFileIO(new Configuration()),
        List.of("file:" + inside.getAbsolutePath()),
        new Configuration(),
        "default.t");

    // No scheme: the manifest may carry a bare path, and the guard must qualify it the same way
    // it qualified the prefixes rather than refusing it as unmatched.
    io.deleteFile(allowed.getAbsolutePath());

    Assert.assertFalse(allowed.exists());
    Assert.assertEquals(List.of(), io.skippedPaths());
  }

  @Test
  public void readAndWriteMethodsAreNotConstrained() throws Exception {
    File outside = tempFolder.newFolder("write_outside");
    PrefixGuardedFileIO io = new PrefixGuardedFileIO(
        new HadoopFileIO(new Configuration()),
        List.of("file:/nowhere"),
        new Configuration(),
        "default.t");

    // Only deletion is guarded: a purge never writes, but the same FileIO instance must not
    // change how anything else behaves if it is ever reused.
    Assert.assertNotNull(io.newOutputFile("file:" + outside.getAbsolutePath() + "/d.parquet"));
    Assert.assertNotNull(io.newInputFile("file:" + outside.getAbsolutePath() + "/d.parquet"));
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=PrefixGuardedFileIOTest test
```

Expected: compilation failure - `cannot find symbol: class PrefixGuardedFileIO`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIO.java`:

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO decorator that refuses to delete a path outside the configured allowlist. It exists
 * because a purge deletes whatever the table's manifests reference, and in the REST protocol the
 * manifests are written by the client: a commit can point a snapshot at arbitrary paths, and the
 * proxy would otherwise delete them under its own credentials. Checking the table location up
 * front cannot catch that - those paths are only known while walking the manifests.
 *
 * <p>A refused path is skipped and logged, never thrown on: by the time deletion runs the table
 * is already dropped in the metastore, so failing here would leave a half-purged table and a 500.
 *
 * <p>Only the delete methods are constrained; reads and writes delegate unchanged. Not
 * serializable in practice (it holds a Hadoop {@link Configuration}), which is fine because this
 * FileIO never leaves the proxy's JVM - Iceberg only serializes a FileIO for distributed
 * planning, which this front door does not do.
 */
final class PrefixGuardedFileIO implements FileIO {
  private static final Logger LOG = LoggerFactory.getLogger(PrefixGuardedFileIO.class);

  private final FileIO delegate;
  private final List<String> qualifiedPrefixes;
  private final transient Configuration conf;
  private final String tableName;
  private final List<String> skippedPaths = new CopyOnWriteArrayList<>();

  PrefixGuardedFileIO(
      FileIO delegate, List<String> qualifiedPrefixes, Configuration conf, String tableName) {
    this.delegate = delegate;
    this.qualifiedPrefixes = List.copyOf(qualifiedPrefixes);
    this.conf = conf;
    this.tableName = tableName;
  }

  List<String> skippedPaths() {
    return List.copyOf(skippedPaths);
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public InputFile newInputFile(String path, long length) {
    return delegate.newInputFile(path, length);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    if (!allowed(path)) {
      return;
    }
    delegate.deleteFile(path);
  }

  @Override
  public void deleteFile(InputFile file) {
    deleteFile(file.location());
  }

  @Override
  public void deleteFile(OutputFile file) {
    deleteFile(file.location());
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private boolean allowed(String path) {
    String qualified;
    try {
      qualified = PurgePathQualifier.qualify(path, conf);
    } catch (IOException e) {
      // Fail closed: a path whose filesystem cannot even be resolved cannot be shown to be
      // inside the allowlist, and this code deletes data.
      skip(path, "its location could not be qualified: " + e.getMessage());
      return false;
    }
    if (PathPrefixAllowlist.matches(qualified, qualifiedPrefixes)) {
      return true;
    }
    skip(qualified, "it is outside rest-catalog.purge.allowed-prefixes");
    return false;
  }

  private void skip(String path, String reason) {
    skippedPaths.add(path);
    LOG.warn("purge of table '{}' skipped deleting '{}' because {}", tableName, path, reason);
  }
}
```

- [ ] **Step 4: Add the shared qualifier the decorator and the policy both use**

Create `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/PurgePathQualifier.java`:

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Turns a configured prefix or a path out of a manifest into the one qualified form both sides of
 * an allowlist comparison must be in: a bare path, "file:/tmp/x" and "file:///tmp/x" all have to
 * compare equal, or the allowlist would refuse paths it means to permit.
 */
final class PurgePathQualifier {
  private PurgePathQualifier() {
  }

  static String qualify(String location, Configuration conf) throws IOException {
    Path path = new Path(location);
    FileSystem fileSystem = path.getFileSystem(conf);
    return path.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory()).toString();
  }

  /** Qualifies every prefix, skipping any whose filesystem cannot be resolved. */
  static List<String> qualifyPrefixes(List<String> prefixes, Configuration conf) {
    List<String> qualified = new ArrayList<>(prefixes.size());
    for (String prefix : prefixes) {
      try {
        qualified.add(qualify(prefix, conf));
      } catch (IOException | IllegalArgumentException e) {
        // A prefix that cannot be qualified simply matches nothing; startup already rejected an
        // empty list, and refusing to serve at all because one prefix names an unreachable
        // filesystem would take down reads too.
        qualified.add(prefix);
      }
    }
    return List.copyOf(qualified);
  }
}
```

Remove the now-unused `FileSystem`/`Path` imports from `PrefixGuardedFileIO` (it calls the
qualifier instead).

- [ ] **Step 5: Run the test to verify it passes**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=PrefixGuardedFileIOTest test
```

Expected: PASS. If the allowlist comparison fails on `file:/...` vs `file:///...`, the fix belongs
in `PurgePathQualifier` (qualify both sides), not in the test.

- [ ] **Step 6: Stage**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIO.java src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/PurgePathQualifier.java src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/PrefixGuardedFileIOTest.java
```

---

### Task 4: The purge policy

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicy.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicyTest.java`

**Interfaces:**
- Consumes: `RestCatalogPurgeMode`, `PathPrefixAllowlist`, `PrefixGuardedFileIO`, `PurgePathQualifier`.
- Produces:
  - `IcebergPurgePolicy(RestCatalogPurgeMode mode, List<String> allowedPrefixes)`
  - `boolean isDefaultBehaviour()` - true only for `ALLOW`; the catalog uses it to keep the
    untouched fast path.
  - `String refusalFor(String tableName, TableMetadata metadata, Configuration conf)` - null when
    the purge may proceed, otherwise the message for the `403`.
  - `FileIO guard(FileIO io, Configuration conf, String tableName)` - the delegate itself in
    `ALLOW`, a `PrefixGuardedFileIO` otherwise.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicyTest.java`:

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergPurgePolicyTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void allowModeRefusesNothingAndWrapsNothing() throws Exception {
    IcebergPurgePolicy policy = new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of());
    HadoopFileIO io = new HadoopFileIO(new Configuration());

    Assert.assertTrue(policy.isDefaultBehaviour());
    Assert.assertNull(policy.refusalFor("default.t", metadataAt("t"), new Configuration()));
    Assert.assertSame(io, policy.guard(io, new Configuration(), "default.t"));
  }

  @Test
  public void refuseModeRefusesEveryPurge() throws Exception {
    IcebergPurgePolicy policy = new IcebergPurgePolicy(RestCatalogPurgeMode.REFUSE, List.of());

    String refusal = policy.refusalFor("default.t", metadataAt("t"), new Configuration());

    Assert.assertNotNull(refusal);
    Assert.assertTrue(refusal, refusal.contains("rest-catalog.purge.mode"));
    Assert.assertFalse(policy.isDefaultBehaviour());
  }

  @Test
  public void allowlistModeAcceptsATableInsideTheListAndRefusesOneOutside() throws Exception {
    File allowedRoot = tempFolder.newFolder("allowed");
    TableMetadata inside = metadataAt("allowed/t_inside");
    TableMetadata outside = metadataAt("elsewhere/t_outside");
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:" + allowedRoot.getAbsolutePath()));

    Assert.assertNull(policy.refusalFor("default.t_inside", inside, new Configuration()));
    String refusal = policy.refusalFor("default.t_outside", outside, new Configuration());
    Assert.assertNotNull(refusal);
    Assert.assertTrue(refusal, refusal.contains("rest-catalog.purge.allowed-prefixes"));
  }

  @Test
  public void allowlistModeRefusesWhenOnlyTheMetadataFileIsOutside() throws Exception {
    File allowedRoot = tempFolder.newFolder("allowed_meta");
    TableMetadata inside = metadataAt("allowed_meta/t");
    // A table whose data lives inside the allowlist but whose pointer was moved outside it:
    // dropping it would still delete metadata JSON in the other tree.
    TableMetadata movedMetadata = TableMetadata.buildFrom(inside)
        .setPreviousFileLocation("file:" + tempFolder.getRoot().getAbsolutePath() + "/other/v1.metadata.json")
        .build();
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:" + allowedRoot.getAbsolutePath()));

    Assert.assertNull(policy.refusalFor("default.t", inside, new Configuration()));
    Assert.assertNotNull(policy.refusalFor("default.t", movedMetadata, new Configuration()));
  }

  @Test
  public void allowlistModeWrapsTheFileIo() throws Exception {
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:/allowed"));
    HadoopFileIO io = new HadoopFileIO(new Configuration());

    Assert.assertTrue(policy.guard(io, new Configuration(), "default.t") instanceof PrefixGuardedFileIO);
  }

  private TableMetadata metadataAt(String relativeDir) throws Exception {
    File dir = new File(tempFolder.getRoot(), relativeDir);
    Assert.assertTrue(dir.mkdirs() || dir.isDirectory());
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), "file:" + dir.getAbsolutePath(), Map.of());
  }
}
```

If `TableMetadata.buildFrom(...).setPreviousFileLocation(...)` does not exist in Iceberg 1.9.2,
replace that case with one that builds metadata whose `location()` is inside the allowlist and
whose `metadataFileLocation()` is outside it - check with
`javap -cp ~/.m2/repository/org/apache/iceberg/iceberg-core/1.9.2/iceberg-core-1.9.2.jar org.apache.iceberg.TableMetadata`
and use whatever builder the version offers; the behaviour under test is "metadata file outside the
list is refused", not the particular builder call.

- [ ] **Step 2: Run the test to verify it fails**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=IcebergPurgePolicyTest test
```

Expected: compilation failure - `cannot find symbol: class IcebergPurgePolicy`.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicy.java`:

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import io.github.mmalykhin.hmsproxy.util.PathPrefixAllowlist;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

/**
 * The single place deciding what {@code DELETE ...?purgeRequested=true} may destroy. Kept apart
 * from {@link WriteRouteGate}, which answers the different question of which catalog a namespace
 * belongs to: the gate keeps a purge inside the default catalog, this policy bounds which paths
 * inside it may be deleted. Do not grow a second purge check anywhere else.
 *
 * <p>Two lines of enforcement, because neither is sufficient alone. The pre-flight check
 * ({@link #refusalFor}) answers loudly, before anything is dropped, for the ordinary case of a
 * table living in the wrong tree. The FileIO guard ({@link #guard}) covers what pre-flight cannot
 * see: the data and manifest paths only discovered while walking manifests the client wrote.
 */
final class IcebergPurgePolicy {
  private final RestCatalogPurgeMode mode;
  private final List<String> allowedPrefixes;

  IcebergPurgePolicy(RestCatalogPurgeMode mode, List<String> allowedPrefixes) {
    this.mode = Objects.requireNonNull(mode, "mode");
    this.allowedPrefixes = List.copyOf(allowedPrefixes);
  }

  boolean isDefaultBehaviour() {
    return mode == RestCatalogPurgeMode.ALLOW;
  }

  /** Null when the purge may proceed; otherwise the message the client gets with the 403. */
  String refusalFor(String tableName, TableMetadata metadata, Configuration conf) {
    if (mode == RestCatalogPurgeMode.ALLOW) {
      return null;
    }
    if (mode == RestCatalogPurgeMode.REFUSE) {
      return "Purge is disabled on this proxy (rest-catalog.purge.mode=REFUSE); table '"
          + tableName + "' was not dropped. Retry without purgeRequested to drop it and keep its"
          + " files.";
    }
    if (metadata == null) {
      // No metadata means nothing to walk and nothing to delete; the drop itself is not a purge.
      return null;
    }
    List<String> qualifiedPrefixes = PurgePathQualifier.qualifyPrefixes(allowedPrefixes, conf);
    String outside = firstPathOutside(
        qualifiedPrefixes, conf, metadata.location(), metadata.metadataFileLocation());
    if (outside == null) {
      return null;
    }
    return "Purge is restricted to rest-catalog.purge.allowed-prefixes; table '" + tableName
        + "' was not dropped because '" + outside + "' lies outside them.";
  }

  /** The delegate itself in ALLOW, so the default path keeps today's FileIO untouched. */
  FileIO guard(FileIO io, Configuration conf, String tableName) {
    if (mode != RestCatalogPurgeMode.ALLOWLIST) {
      return io;
    }
    return new PrefixGuardedFileIO(
        io, PurgePathQualifier.qualifyPrefixes(allowedPrefixes, conf), conf, tableName);
  }

  private static String firstPathOutside(
      List<String> qualifiedPrefixes, Configuration conf, String... locations) {
    for (String location : locations) {
      if (location == null) {
        continue;
      }
      String qualified;
      try {
        qualified = PurgePathQualifier.qualify(location, conf);
      } catch (IOException e) {
        // Fail closed, the same way the FileIO guard does: a location that cannot be qualified
        // cannot be shown to be inside the allowlist.
        return location;
      }
      if (!PathPrefixAllowlist.matches(qualified, qualifiedPrefixes)) {
        return qualified;
      }
    }
    return null;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=IcebergPurgePolicyTest test
```

Expected: PASS.

- [ ] **Step 5: Stage**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicy.java src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergPurgePolicyTest.java
```

---

### Task 5: Wire the policy into the catalog and the REST services

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingHiveCatalog.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java:94-126`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServices.java:47-60`
- Modify: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingHiveCatalogTest.java` (constructor arity)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: `IcebergPurgePolicy` (Task 4), `RestCatalogConfig.purgeMode()`/`purgeAllowedPrefixes()`
  (Task 2).
- Produces:
  - `RoutingHiveCatalog(IMetaStoreClient client, Configuration conf, IcebergPurgePolicy purgePolicy)`
  - `IcebergRestService(String catalogName, ThriftHiveMetastore.Iface delegate,
    CatalogNameTranslation translationOrNull, String defaultCatalogName,
    Function<String, String> catalogForExternalDb, Configuration hadoopConf,
    IcebergPurgePolicy purgePolicy)` - the policy is the new last parameter.

- [ ] **Step 1: Write the failing endpoint tests**

In `IcebergRestEndpointIntegrationTest`, make the fixture able to restart the listener under a
different mode. Change `buildConfig()` to take the REST config and add a helper (place both next to
the existing `buildConfig`):

```java
  // Restarts the listener under a different purge policy. The @Before fixture (fake metastore,
  // registered tables) is left alone, so a test can register its tables first and then decide
  // which policy they are dropped under.
  private void restartWithPurgePolicy(RestCatalogPurgeMode mode, List<String> allowedPrefixes)
      throws Exception {
    server.close();
    services.close();
    ProxyConfig config = buildConfig(
        new RestCatalogConfig(true, "127.0.0.1", 0, 1, 4, null, null, mode, allowedPrefixes));
    Function<String, String> catalogForExternalDb = externalDbName ->
        externalDbName != null && externalDbName.startsWith(CATALOG2_NAME + "__")
            ? CATALOG2_NAME
            : CATALOG_NAME;
    services = IcebergRestServices.open(config, delegate.iface, catalogForExternalDb);
    server = RestCatalogServer.open(config, services, metrics);
    Assert.assertNotNull("server must restart", server);
  }
```

`buildConfig()` becomes `buildConfig(RestCatalogConfig restCatalog)` with
`.restCatalog(restCatalog)`; `setUp` passes
`new RestCatalogConfig(true, "127.0.0.1", 0, 1, 4, null, null, RestCatalogPurgeMode.ALLOW, List.of())`.

Then add the four tests:

```java
  @Test
  public void purgeIsRefusedWithForbiddenWhenModeIsRefuse() throws Exception {
    File dataFile = registerTableWithCommittedDataFile("refuse_me");
    restartWithPurgePolicy(RestCatalogPurgeMode.REFUSE, List.of());

    HttpResponse<String> response =
        delete("/v1/" + CATALOG_NAME + "/namespaces/default/tables/refuse_me?purgeRequested=true");

    Assert.assertEquals("body: " + response.body(), 403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
    Assert.assertTrue("a refused purge must keep the data file: " + dataFile, dataFile.exists());
    Assert.assertFalse("a refused purge must not drop the table: " + delegate.calls,
        delegate.calls.contains("drop_table:default.refuse_me"));
  }

  @Test
  public void dropWithoutPurgeStillWorksWhenModeIsRefuse() throws Exception {
    File dataFile = registerTableWithCommittedDataFile("refuse_mode_plain_drop");
    restartWithPurgePolicy(RestCatalogPurgeMode.REFUSE, List.of());

    HttpResponse<String> response =
        delete("/v1/" + CATALOG_NAME + "/namespaces/default/tables/refuse_mode_plain_drop");

    Assert.assertEquals("body: " + response.body(), 204, response.statusCode());
    Assert.assertTrue(delegate.calls.toString(),
        delegate.calls.contains("drop_table:default.refuse_mode_plain_drop"));
    Assert.assertTrue("a drop without purge must leave the data alone", dataFile.exists());
  }

  @Test
  public void purgeInsideTheAllowlistDeletesDataFiles() throws Exception {
    File dataFile = registerTableWithCommittedDataFile("allowed_purge");
    restartWithPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:" + tempFolder.getRoot().getAbsolutePath()));

    HttpResponse<String> response = delete(
        "/v1/" + CATALOG_NAME + "/namespaces/default/tables/allowed_purge?purgeRequested=true");

    Assert.assertEquals("body: " + response.body(), 204, response.statusCode());
    Assert.assertTrue(delegate.calls.toString(),
        delegate.calls.contains("drop_table:default.allowed_purge"));
    Assert.assertFalse("a permitted purge must delete the data file: " + dataFile, dataFile.exists());
  }

  @Test
  public void purgeOutsideTheAllowlistIsRefusedAndDropsNothing() throws Exception {
    File dataFile = registerTableWithCommittedDataFile("outside_purge");
    restartWithPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:/nowhere/that/exists"));

    HttpResponse<String> response = delete(
        "/v1/" + CATALOG_NAME + "/namespaces/default/tables/outside_purge?purgeRequested=true");

    Assert.assertEquals("body: " + response.body(), 403, response.statusCode());
    Assert.assertTrue(response.body(), response.body().contains("ForbiddenException"));
    Assert.assertTrue("a refused purge must keep the data file: " + dataFile, dataFile.exists());
    Assert.assertFalse("a refused purge must not drop the table: " + delegate.calls,
        delegate.calls.contains("drop_table:default.outside_purge"));
  }

  /**
   * The manifest-borne case the pre-flight check cannot see: the table itself sits inside the
   * allowlist, but its snapshot references a data file in another tree - the shape a client can
   * produce on purpose, since in the REST protocol the client writes the manifests. The purge
   * must delete the table's own files and leave the foreign one alone.
   */
  @Test
  public void purgeSkipsAManifestPathOutsideTheAllowlist() throws Exception {
    File foreignDir = tempFolder.newFolder("foreign");
    File foreignFile = new File(foreignDir, "someone_elses.parquet");
    Files.write(foreignFile.toPath(), new byte[] {9});
    File ownDataFile = registerTableWithCommittedDataFile("mixed_purge", Map.of(), foreignFile);
    restartWithPurgePolicy(RestCatalogPurgeMode.ALLOWLIST,
        List.of("file:" + new File(tempFolder.getRoot(), "mixed_purge").getAbsolutePath()));

    HttpResponse<String> response = delete(
        "/v1/" + CATALOG_NAME + "/namespaces/default/tables/mixed_purge?purgeRequested=true");

    Assert.assertEquals("body: " + response.body(), 204, response.statusCode());
    Assert.assertFalse("the table's own data file must be purged: " + ownDataFile,
        ownDataFile.exists());
    Assert.assertTrue("a manifest path outside the allowlist must survive: " + foreignFile,
        foreignFile.exists());
  }
```

The last test needs a `registerTableWithCommittedDataFile` overload that also appends a data file
living outside the table directory. Add it next to the existing overloads:

```java
  // Same as registerTableWithCommittedDataFile(name, properties), but the committed snapshot also
  // references a file outside the table's own directory - what a client can do by writing its own
  // manifest, and what the FileIO guard exists to bound.
  private File registerTableWithCommittedDataFile(
      String tableName, Map<String, String> properties, File extraDataFileOutsideTable)
      throws Exception {
    File tableDir = tempFolder.newFolder(tableName);
    File dataFile = new File(tableDir, "data/data.parquet");
    dataFile.getParentFile().mkdirs();
    Files.write(dataFile.toPath(), new byte[] {1, 2, 3});

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    org.apache.iceberg.Table table = new HadoopTables(new Configuration()).create(
        schema, PartitionSpec.unpartitioned(), properties, "file://" + tableDir.getAbsolutePath());
    table.newAppend()
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("file://" + dataFile.getAbsolutePath())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(dataFile.length())
            .withRecordCount(1)
            .build())
        .appendFile(DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("file://" + extraDataFileOutsideTable.getAbsolutePath())
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(extraDataFileOutsideTable.length())
            .withRecordCount(1)
            .build())
        .commit();

    Table hiveTable = RecordingThriftIface.table("default", tableName);
    hiveTable.getParameters().put(
        "metadata_location", ((HasTableOperations) table).operations().current().metadataFileLocation());
    delegate.tables.put("default." + tableName, hiveTable);
    return dataFile;
  }
```

Add imports as needed: `io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode`.

- [ ] **Step 2: Run the new tests to verify they fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
```

Expected: the four new tests fail - `REFUSE` and both `ALLOWLIST` cases return `204` and delete
files, because no policy is wired in yet.

- [ ] **Step 3: Apply the policy in the catalog**

In `RoutingHiveCatalog.java`, take the policy in the constructor and override `dropTable`:

```java
  private final IMetaStoreClient client;
  private final IcebergPurgePolicy purgePolicy;

  public RoutingHiveCatalog(IMetaStoreClient client, Configuration conf, IcebergPurgePolicy purgePolicy) {
    this.client = Objects.requireNonNull(client, "client");
    this.purgePolicy = Objects.requireNonNull(purgePolicy, "purgePolicy");
    setConf(Objects.requireNonNull(conf, "conf"));
  }

  /**
   * A purge deletes real files under the proxy's own credentials, so it goes through
   * {@link IcebergPurgePolicy} instead of straight to HiveCatalog. In the default ALLOW mode this
   * delegates untouched. Otherwise the same two steps HiveCatalog performs are performed here in
   * the same order - metastore drop first, then the files - with the policy consulted before the
   * drop and the FileIO guarded during the delete.
   */
  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!purge || purgePolicy.isDefaultBehaviour()) {
      return super.dropTable(identifier, purge);
    }
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    try {
      lastMetadata = ops.current();
    } catch (NotFoundException e) {
      // The metastore entry outlived its metadata file: nothing to walk and nothing to delete.
      lastMetadata = null;
    }
    String refusal = purgePolicy.refusalFor(identifier.toString(), lastMetadata, getConf());
    if (refusal != null) {
      throw new ForbiddenException("%s", refusal);
    }
    boolean dropped = super.dropTable(identifier, false);
    if (dropped && lastMetadata != null) {
      CatalogUtil.dropTableData(
          purgePolicy.guard(ops.io(), getConf(), identifier.toString()), lastMetadata);
    }
    return dropped;
  }
```

Imports to add: `org.apache.iceberg.CatalogUtil`, `org.apache.iceberg.TableMetadata`,
`org.apache.iceberg.TableOperations`, `org.apache.iceberg.catalog.TableIdentifier`,
`org.apache.iceberg.exceptions.ForbiddenException`, `org.apache.iceberg.exceptions.NotFoundException`.

- [ ] **Step 4: Thread the policy through the services**

`IcebergRestService`: add `IcebergPurgePolicy purgePolicy` as the last constructor parameter,
`Objects.requireNonNull` it, and pass it to `new RoutingHiveCatalog(client, hadoopConf, purgePolicy)`.

`IcebergRestServices.open(config, delegate, catalogForExternalDb, hadoopConfForCatalog)`: build the
policy once and hand it to every service:

```java
    IcebergPurgePolicy purgePolicy = new IcebergPurgePolicy(
        config.restCatalog().purgeMode(), config.restCatalog().purgeAllowedPrefixes());
```

then `new IcebergRestService(catalog, delegate, translation, config.defaultCatalog(),
catalogForExternalDb, hadoopConfForCatalog.apply(catalog), purgePolicy)`.

`RoutingHiveCatalogTest`: update its three `new RoutingHiveCatalog(client, new Configuration())`
calls to
`new RoutingHiveCatalog(client, new Configuration(), new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of()))`.

- [ ] **Step 5: Run the REST suites**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest='IcebergRest*+RoutingHiveCatalogTest+WriteRouteGateTest+RestCatalogServerTest+SpnegoIntegrationTest' test
```

Expected: PASS, including the four pre-existing purge tests under the default `ALLOW`.

- [ ] **Step 6: Run the whole suite**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test
```

Expected: PASS.

- [ ] **Step 7: Stage**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/restcatalog src/test/java/io/github/mmalykhin/hmsproxy/restcatalog
```

---

### Task 6: Documentation

**Files:**
- Modify: `README.md` (Iceberg REST purge paragraph around line 1101, and the configuration
  reference where `rest-catalog.*` keys are listed)
- Modify: `README.ru.md` (the same two places)
- Modify: `CHANGELOG.md`, `CHANGELOG.ru.md` (unreleased section)
- Modify: `src/main/resources/hms-proxy-example.properties` (after the `rest-catalog.kerberos.*`
  block around line 102)
- Modify: `AGENTS.md` (the `restcatalog` bullet in "Архитектурные заметки")

- [ ] **Step 1: Update the README purge paragraph (EN)**

The paragraph currently ends with "it has no allowlist and no `BEST_EFFORT` switch - the client
asked for it through the spec's own parameter - so the write gate is the only thing keeping it
inside the default catalog". Replace that clause: the write gate still keeps a purge inside the
default catalog, and `rest-catalog.purge.mode` now bounds what it may delete there. Document all
three modes, that the default is `ALLOW` (unchanged behaviour), that a refusal is a `403` before
anything is dropped, and that in `ALLOWLIST` a path outside the list found while walking manifests
is skipped rather than deleted - naming why that second check exists (the client writes the
manifests).

- [ ] **Step 2: Mirror it in `README.ru.md`**

Same content in the Russian purge section (around line 1100 in `README.ru.md`; locate it with
`grep -n "purgeRequested" README.ru.md`). Keep the terminology already used there; do not
transliterate "allowlist" if the file already uses another form - check with
`grep -n "allowlist" README.ru.md`.

- [ ] **Step 3: Add the keys to the configuration reference in both READMEs**

Next to the other `rest-catalog.*` keys:

```properties
rest-catalog.purge.mode=ALLOWLIST
rest-catalog.purge.allowed-prefixes=hdfs://ns-default/warehouse/tablespace/
```

State that `allowed-prefixes` is required by, and only valid with, `ALLOWLIST`, and that both
contradictions fail at startup.

- [ ] **Step 4: Add the example properties**

In `src/main/resources/hms-proxy-example.properties`, after the `rest-catalog.kerberos.*` lines:

```properties
# What DELETE .../tables/{tbl}?purgeRequested=true may delete. ALLOW (default) deletes whatever
# the table's metadata and manifests point at, the way Iceberg REST catalogs do; ALLOWLIST deletes
# only under the prefixes below and refuses (403) a table outside them; REFUSE answers 403 to every
# purge, leaving a plain drop working. allowed-prefixes is required by ALLOWLIST and rejected with
# the other modes.
# rest-catalog.purge.mode=ALLOWLIST
# rest-catalog.purge.allowed-prefixes=hdfs://ns-catalog1/warehouse/tablespace/
```

- [ ] **Step 5: Changelog, both languages**

Add an entry to the unreleased section of `CHANGELOG.md` and `CHANGELOG.ru.md` (match the existing
heading style - read the top of each file first): the new keys, the default being unchanged, and
the two-line enforcement in `ALLOWLIST`.

- [ ] **Step 6: AGENTS.md note**

In the `restcatalog` bullet, the sentence "…синхронно до ответа `204` и без allowlist - в отличие
от purge внешних таблиц на Thrift-пути" no longer holds. Replace it with: the purge boundary lives
in `IcebergPurgePolicy` (`rest-catalog.purge.mode`, default `ALLOW` = today's behaviour), separate
from `WriteRouteGate`; in `ALLOWLIST` the pre-flight check refuses with `403` and the
`PrefixGuardedFileIO` skips manifest-borne paths outside the list, because in the REST protocol the
client writes the manifests; no other class grows its own purge check. Keep it to two or three
sentences, in the file's existing Russian style.

- [ ] **Step 7: Verify the docs build and nothing else drifted**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=CapabilityMatrixDocSyncTest test
```

Expected: PASS (this change adds no capability rows, so the generated matrix must be untouched).

- [ ] **Step 8: Stage**

```bash
git add README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md AGENTS.md src/main/resources/hms-proxy-example.properties
```

---

### Task 7: Stand verification of the default

The whole point of the `ALLOW` default is that nothing changes for existing deployments, so it is
measured on the stand rather than asserted.

**Files:** none modified (unless the run finds a defect).

- [ ] **Step 1: Build the fat jar**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -DskipTests package
```

Expected: `target/hms-proxy-<version>-fat.jar` exists.

- [ ] **Step 2: Bring up the stand**

```bash
cd smoke-stand && ./prepare.sh && docker compose up -d --build
```

Expected: containers healthy. Read `smoke-stand/README.md` first if `prepare.sh` needs inputs.

- [ ] **Step 3: Run the Iceberg interop scenario in the default configuration**

```bash
smoke-stand/run-iceberg-interop-smoke.sh --prefix hdp
```

Expected: the scenario passes, including its closing purge check that no data, manifest or
metadata files are left behind. This is the assertion that the default mode still purges.

- [ ] **Step 4: Report and stop the stand**

Report the scenario's outcome verbatim in the final message, and state which stand profiles were
**not** run (Kerberos, `apache`, `hive4`) so nothing is implied to have been verified that was not.

```bash
cd smoke-stand && docker compose stop
```

---

## Self-Review

- **Spec coverage:** modes (Task 2, 5), refusal semantics (Task 4, 5), two-line enforcement
  (Tasks 3, 4, 5), configuration + contradictions (Task 2), `PathPrefixAllowlist` extraction
  (Task 1), components list (Tasks 1-5), failure handling (Task 3 skip-not-throw, Task 5
  `NotFoundException`), tests (Tasks 1-5), stand verification (Task 7), documentation (Task 6),
  out-of-scope items untouched.
- **Placeholders:** none - every code step carries its code; the one conditional (the
  `TableMetadata` builder call in Task 4) names the exact command to resolve it and the behaviour
  that must hold either way.
- **Type consistency:** `IcebergPurgePolicy(mode, prefixes)`, `isDefaultBehaviour()`,
  `refusalFor(String, TableMetadata, Configuration)`, `guard(FileIO, Configuration, String)`,
  `PrefixGuardedFileIO(FileIO, List<String>, Configuration, String)`, `skippedPaths()`,
  `PathPrefixAllowlist.parse/matches`, `PurgePathQualifier.qualify/qualifyPrefixes` are used with
  the same names and arities in every task that references them.
