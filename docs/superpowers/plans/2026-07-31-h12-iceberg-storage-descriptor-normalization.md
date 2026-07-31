# H12: Iceberg storage-descriptor normalization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an Iceberg table created through the Hive 4 front door readable by the 3.1-line engines, by normalizing the `StorageDescriptor` the Hive 4 client sends before it reaches the backend.

**Architecture:** A small pure function (`IcebergStorageDescriptorNormalizer`) runs inside `Hive4FrontendBridge`, right after the incoming Hive 4 `Table` has been converted into the typed Apache 3.1.3 `Table`. It rewrites the descriptor's format fields only when they are empty or abstract, and only when the client itself declared the Iceberg storage handler. Guarded by a config key defaulting to on.

**Tech Stack:** Java 17, Maven, JUnit 4, Thrift-generated Hive metastore API, docker-compose smoke stand.

**Spec:** `docs/superpowers/specs/2026-07-31-h12-iceberg-storage-descriptor-normalization-design.md`

## Global Constraints

- Build and test only on JDK 17: `JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19`.
- Commits, PR text and code comments in English. Never add `Co-Authored-By: Claude` or any Claude/Anthropic attribution.
- Never run `git commit` or `git push` without an explicit instruction from the user. Steps below that say "Commit" mean: stage, show the diff, and wait.
- Work directly in `main`; do not create feature branches.
- Tests live in `tests/` subdirectories next to the source module only where that convention already applies; this repo's Java tests live under `src/test/java/...` mirroring the package — follow the existing layout.
- EN/RU documentation pairs must be updated together: `README.md`/`README.ru.md`, `CHANGELOG.md`/`CHANGELOG.ru.md`, `smoke-stand/TEST-MATRIX.md`/`.ru.md`.
- In Russian text never use "карьер" for carrier or "ворота" for gate.
- Config key, fixed by the spec: `frontend.hive4.normalize-iceberg-storage-descriptor`, default `true`.
- Trigger, fixed by the spec: table parameter `storage_handler` equals `org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`.
- A concrete format value set by the client is never overwritten.

## Deviation from the spec, decided while planning

The spec asks for a counter recording how often a rewrite happened. `FrontendProcessorFactory.create(ProxyConfig, ThriftHiveMetastore.Iface)` has no `ProxyObservability`, and neither do the three bridges; adding one would change the signature shared by `ApacheFrontendBridge` and `HortonworksFrontendBridge` for a code path that only ever runs on DDL.

This plan therefore logs one INFO line per rewrite instead, which is proportionate for DDL volume and needs no plumbing. If a Prometheus counter is wanted later, it is a separate task that threads `ProxyObservability` into `FrontendProcessorFactory`. **Confirm this substitution with the user before Task 3.**

---

### Task 1: Measure the real descriptors before writing any code

This task produces facts, not code. Its output decides whether Task 2 rewrites one field or three. No implementation starts until this task's findings are written down.

**Files:**
- Modify: `docs/superpowers/specs/2026-07-31-h12-iceberg-storage-descriptor-normalization-design.md` (fill in the "candidate, pending measurement" rows)

- [ ] **Step 1: Bring the stand up on the Hive 4 backend, plain profile**

```bash
cd smoke-stand && ./prepare.sh && docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe up -d
```

Note: `target/` was cleaned, so `prepare.sh` will fail without a fat jar. Build one first:

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o package -DskipTests
```

- [ ] **Step 2: Confirm H12 actually fails today**

```bash
cd smoke-stand && ./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4
```

Expected: the run reports the 3.1 participants failing to read the Hive 4-created table, with
`Cannot create an instance of InputFormat class org.apache.hadoop.mapred.FileInputFormat` in the
beeline output, and ends at 3 rows rather than 5.

**If this run passes, stop.** A green run means the scenario does not exercise what this plan changes, and every later "it works now" claim would be worthless. Report that to the user before continuing.

- [ ] **Step 3: Dump the descriptor of a Hive 4-created Iceberg table**

```bash
docker exec stand-hs2-hive4 beeline -u 'jdbc:hive2://hs2-hive4:10000/default' -n hive \
  --silent=true --outputformat=tsv2 \
  -e 'create table if not exists h12_probe_hive4 (id int) stored by iceberg;'
docker exec stand-hs2-hive4 beeline -u 'jdbc:hive2://hs2-hive4:10000/default' -n hive \
  --silent=true --outputformat=tsv2 -e 'describe formatted h12_probe_hive4;' \
  | grep -iE 'inputformat|outputformat|serde|storage_handler'
```

- [ ] **Step 4: Dump the descriptor of a REST-created Iceberg table for comparison**

```bash
docker exec stand-proxy java -jar /opt/hms-proxy/iceberg-rest-writer.jar create \
  --uri http://proxy:9183 --namespace default --table h12_probe_rest
docker exec stand-hs2-hive4 beeline -u 'jdbc:hive2://hs2-hive4:10000/default' -n hive \
  --silent=true --outputformat=tsv2 -e 'describe formatted h12_probe_rest;' \
  | grep -iE 'inputformat|outputformat|serde|storage_handler'
```

- [ ] **Step 5: Record the findings in the spec**

Replace the two "candidate, pending measurement" rows in the spec's rewrite table with what was measured. Three outcomes are possible, and all three are acceptable:

- Only `inputFormat` differs → delete both candidate rows; Task 2 implements one field.
- `outputFormat` also abstract/empty → keep that row, filling the target with the exact class name printed for `h12_probe_rest`.
- `serializationLib` also empty → same.

Also record the exact `storage_handler` parameter value observed, and confirm it matches the constant the spec fixes. If it does not, stop and report — the trigger would not fire.

- [ ] **Step 6: Clean up the probe tables**

```bash
docker exec stand-hs2-hive4 beeline -u 'jdbc:hive2://hs2-hive4:10000/default' -n hive \
  --silent=true -e 'drop table h12_probe_hive4; drop table h12_probe_rest;'
```

- [ ] **Step 7: Commit the spec update** (stage and show the diff; wait for the user)

```bash
git add docs/superpowers/specs/2026-07-31-h12-iceberg-storage-descriptor-normalization-design.md
git commit -m "Record the measured Hive 4 and REST Iceberg descriptors in the H12 design"
```

---

### Task 2: The normalizer, as a pure function

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/frontend/IcebergStorageDescriptorNormalizer.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/frontend/IcebergStorageDescriptorNormalizerTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks (Task 1 produces facts, not code).
- Produces: `IcebergStorageDescriptorNormalizer(boolean enabled)` with the single method `void normalize(org.apache.hadoop.hive.metastore.api.Table table)`, package-private in `io.github.mmalykhin.hmsproxy.frontend`. Task 3 constructs it and calls `normalize`.

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/io/github/mmalykhin/hmsproxy/frontend/IcebergStorageDescriptorNormalizerTest.java`:

```java
package io.github.mmalykhin.hmsproxy.frontend;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

/**
 * Hive 4 leaves an Iceberg table's inputFormat as the abstract
 * {@code org.apache.hadoop.mapred.FileInputFormat}, which a Hive 3.1 client instantiates and
 * fails on. The proxy is the only place both sides meet, so it rewrites the value the client
 * declared into the one Iceberg's own HiveTableOperations writes - the shape every engine on the
 * stand reads (TEST-MATRIX H9-H11).
 */
public class IcebergStorageDescriptorNormalizerTest {
  private static final String ICEBERG_INPUT_FORMAT =
      "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
  private static final String ABSTRACT_INPUT_FORMAT = "org.apache.hadoop.mapred.FileInputFormat";

  @Test
  public void abstractInputFormatOfAnIcebergTableIsRewritten() {
    Table table = icebergTable(ABSTRACT_INPUT_FORMAT);

    new IcebergStorageDescriptorNormalizer(true).normalize(table);

    Assert.assertEquals(ICEBERG_INPUT_FORMAT, table.getSd().getInputFormat());
  }

  @Test
  public void missingInputFormatOfAnIcebergTableIsRewritten() {
    Table table = icebergTable(null);

    new IcebergStorageDescriptorNormalizer(true).normalize(table);

    Assert.assertEquals(ICEBERG_INPUT_FORMAT, table.getSd().getInputFormat());
  }

  @Test
  public void emptyInputFormatOfAnIcebergTableIsRewritten() {
    Table table = icebergTable("   ");

    new IcebergStorageDescriptorNormalizer(true).normalize(table);

    Assert.assertEquals(ICEBERG_INPUT_FORMAT, table.getSd().getInputFormat());
  }

  /** A value the client chose itself is never second-guessed: that would break working setups. */
  @Test
  public void aConcreteInputFormatIsLeftAlone() {
    Table table = icebergTable("com.example.CustomIcebergInputFormat");

    new IcebergStorageDescriptorNormalizer(true).normalize(table);

    Assert.assertEquals("com.example.CustomIcebergInputFormat", table.getSd().getInputFormat());
  }

  /** Without the Iceberg storage handler this is an ordinary Hive table, whatever it looks like. */
  @Test
  public void aTableThatDoesNotDeclareTheIcebergHandlerIsLeftAlone() {
    Table table = icebergTable(ABSTRACT_INPUT_FORMAT);
    table.getParameters().remove("storage_handler");

    new IcebergStorageDescriptorNormalizer(true).normalize(table);

    Assert.assertEquals(ABSTRACT_INPUT_FORMAT, table.getSd().getInputFormat());
  }

  @Test
  public void theFlagOffMeansNothingIsRewritten() {
    Table table = icebergTable(ABSTRACT_INPUT_FORMAT);

    new IcebergStorageDescriptorNormalizer(false).normalize(table);

    Assert.assertEquals(ABSTRACT_INPUT_FORMAT, table.getSd().getInputFormat());
  }

  /** A compatibility fix must never be the reason a DDL statement fails. */
  @Test
  public void anUnexpectedlyShapedTableIsSurvivedWithoutThrowing() {
    Table noSd = new Table();
    noSd.setParameters(icebergParameters());
    new IcebergStorageDescriptorNormalizer(true).normalize(noSd);

    Table noParameters = new Table();
    noParameters.setSd(storageDescriptor(ABSTRACT_INPUT_FORMAT));
    new IcebergStorageDescriptorNormalizer(true).normalize(noParameters);

    new IcebergStorageDescriptorNormalizer(true).normalize(null);
  }

  private static Table icebergTable(String inputFormat) {
    Table table = new Table();
    table.setDbName("sales");
    table.setTableName("events");
    table.setParameters(icebergParameters());
    table.setSd(storageDescriptor(inputFormat));
    return table;
  }

  private static Map<String, String> icebergParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    parameters.put("table_type", "ICEBERG");
    return parameters;
  }

  private static StorageDescriptor storageDescriptor(String inputFormat) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setInputFormat(inputFormat);
    sd.setSerdeInfo(new SerDeInfo());
    return sd;
  }
}
```

- [ ] **Step 2: Run the tests and watch them fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=IcebergStorageDescriptorNormalizerTest
```

Expected: compilation failure — `cannot find symbol: class IcebergStorageDescriptorNormalizer`. That is the correct first failure.

- [ ] **Step 3: Write the implementation**

Create `src/main/java/io/github/mmalykhin/hmsproxy/frontend/IcebergStorageDescriptorNormalizer.java`:

```java
package io.github.mmalykhin.hmsproxy.frontend;

import java.util.Map;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rewrites the format fields of an Iceberg table declared through the Hive 4 front door into the
 * shape every engine can read.
 *
 * <p>Hive 4 resolves an Iceberg table's real input format through the storage handler at plan
 * time, so it leaves the descriptor carrying the abstract
 * {@code org.apache.hadoop.mapred.FileInputFormat} (or nothing at all, when the handler class is
 * spelled out explicitly). Hive 3.1 instantiates whatever the descriptor names and fails with
 * "Cannot create an instance of InputFormat class". The target value is not a guess: it is what
 * Iceberg's own {@code HiveTableOperations} writes, and TEST-MATRIX H9-H11 show those tables are
 * readable by every engine on the stand, Hive 4 included.
 *
 * <p>This class does <b>not</b> decide whether a table is an Iceberg table - that question belongs
 * to {@code IcebergTablePointerGuard} and is answered from the metastore's record. It fires on
 * what the client explicitly declared in its DDL, and on {@code create_table} there is no record
 * to read yet.
 *
 * <p>It never throws and never overwrites a concrete value the client chose: failing a DDL
 * statement, or second-guessing a working non-standard setup, would both be worse than the
 * incompatibility being repaired.
 */
final class IcebergStorageDescriptorNormalizer {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergStorageDescriptorNormalizer.class);
  private static final String STORAGE_HANDLER = "storage_handler";
  private static final String ICEBERG_STORAGE_HANDLER =
      "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  private static final String ABSTRACT_INPUT_FORMAT = "org.apache.hadoop.mapred.FileInputFormat";
  private static final String ICEBERG_INPUT_FORMAT =
      "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";

  private final boolean enabled;

  IcebergStorageDescriptorNormalizer(boolean enabled) {
    this.enabled = enabled;
  }

  void normalize(Table table) {
    if (!enabled || table == null || !declaresIcebergStorageHandler(table)) {
      return;
    }
    StorageDescriptor sd = table.getSd();
    if (sd == null) {
      return;
    }
    if (needsConcreteFormat(sd.getInputFormat())) {
      LOG.info("normalized inputFormat of Iceberg table '{}.{}' from '{}' to '{}'"
              + " so the 3.1 line can read it",
          table.getDbName(), table.getTableName(), sd.getInputFormat(), ICEBERG_INPUT_FORMAT);
      sd.setInputFormat(ICEBERG_INPUT_FORMAT);
    }
  }

  private static boolean declaresIcebergStorageHandler(Table table) {
    Map<String, String> parameters = table.getParameters();
    return parameters != null && ICEBERG_STORAGE_HANDLER.equals(parameters.get(STORAGE_HANDLER));
  }

  /** Absent, blank, or the abstract base class Hive 4 leaves behind - never a concrete choice. */
  private static boolean needsConcreteFormat(String value) {
    return value == null || value.isBlank() || ABSTRACT_INPUT_FORMAT.equals(value);
  }
}
```

- [ ] **Step 4: Run the tests and watch them pass**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=IcebergStorageDescriptorNormalizerTest
```

Expected: `Tests run: 7, Failures: 0, Errors: 0`.

- [ ] **Step 5: Extend to outputFormat / serializationLib only if Task 1 measured them as broken**

Skip this step entirely if Task 1 found only `inputFormat` differs. Otherwise, for each field Task 1 recorded as abstract or empty, add the constant with the exact value printed for `h12_probe_rest`, a `needsConcreteFormat` check beside the existing one, and a test pair mirroring `abstractInputFormatOfAnIcebergTableIsRewritten` and `aConcreteInputFormatIsLeftAlone` for that field. Run the suite again and expect it green.

- [ ] **Step 6: Commit** (stage and show the diff; wait for the user)

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/frontend/IcebergStorageDescriptorNormalizer.java \
        src/test/java/io/github/mmalykhin/hmsproxy/frontend/IcebergStorageDescriptorNormalizerTest.java
git commit -m "Normalize the storage descriptor of a Hive 4-declared Iceberg table"
```

---

### Task 3: Config key and wiring into the Hive 4 bridge

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/config/frontend/Hive4FrontendConfig.java`
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/config/frontend/Hive4FrontendConfigParser.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/ProxyConfig.java` (record component, defaulting, builder — mirror `icebergPointerGuard` at lines 39, 63-64, 87)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoader.java` (parse and pass — mirror lines 70 and 89)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/frontend/Hive4FrontendBridge.java` (lines 79-103 wiring, 104-118 handler fields, 202-203 and 256-259 call sites)
- Modify: `src/main/resources/hms-proxy-example.properties`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/frontend/Hive4FrontendBridgeTest.java`

**Interfaces:**
- Consumes: `IcebergStorageDescriptorNormalizer(boolean)` and its `normalize(Table)` from Task 2.
- Produces: `ProxyConfig.hive4Frontend()` returning `Hive4FrontendConfig`, whose accessor is `normalizeIcebergStorageDescriptor()`.

- [ ] **Step 1: Write the failing config test**

Add to `src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java`:

```java
  @Test
  public void hive4StorageDescriptorNormalizationDefaultsToOn() throws Exception {
    ProxyConfig config = loadConfig(baseProperties());

    Assert.assertTrue(config.hive4Frontend().normalizeIcebergStorageDescriptor());
  }

  @Test
  public void hive4StorageDescriptorNormalizationCanBeTurnedOff() throws Exception {
    String properties = baseProperties()
        + "frontend.hive4.normalize-iceberg-storage-descriptor=false\n";

    ProxyConfig config = loadConfig(properties);

    Assert.assertFalse(config.hive4Frontend().normalizeIcebergStorageDescriptor());
  }
```

Use the file's existing helpers for building properties and loading (`baseProperties()`/`loadConfig(...)` are placeholders for whatever that test class already calls — read the top of the file and reuse its own harness verbatim rather than inventing one).

- [ ] **Step 2: Run it and watch it fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=ProxyConfigLoaderTest
```

Expected: compilation failure — `cannot find symbol: method hive4Frontend()`.

- [ ] **Step 3: Add the config record and parser**

`src/main/java/io/github/mmalykhin/hmsproxy/config/frontend/Hive4FrontendConfig.java`:

```java
package io.github.mmalykhin.hmsproxy.config.frontend;

/**
 * Settings of the Hive 4 front door.
 *
 * <p>{@code normalizeIcebergStorageDescriptor} rewrites the format fields of an Iceberg table a
 * Hive 4 client declares, so the record the metastore keeps is the one 3.1-line engines can read
 * (TEST-MATRIX H12). It is a key rather than unconditional behaviour because it changes what the
 * client sent and the result is persisted: an operator needs a way out that does not require a
 * new build.
 */
public record Hive4FrontendConfig(boolean normalizeIcebergStorageDescriptor) {
  public static Hive4FrontendConfig defaults() {
    return new Hive4FrontendConfig(true);
  }
}
```

`src/main/java/io/github/mmalykhin/hmsproxy/config/frontend/Hive4FrontendConfigParser.java`:

```java
package io.github.mmalykhin.hmsproxy.config.frontend;

import io.github.mmalykhin.hmsproxy.config.PropertyReader;

public final class Hive4FrontendConfigParser {
  private Hive4FrontendConfigParser() {
  }

  public static Hive4FrontendConfig parse(PropertyReader reader) {
    return new Hive4FrontendConfig(
        reader.getBoolean("frontend.hive4.normalize-iceberg-storage-descriptor", true));
  }
}
```

- [ ] **Step 4: Wire it into ProxyConfig and the loader**

In `ProxyConfig.java`, add `Hive4FrontendConfig hive4Frontend` as a record component next to `icebergPointerGuard` (line 39), default it in the compact constructor exactly as line 63-64 does:

```java
    hive4Frontend = hive4Frontend == null ? Hive4FrontendConfig.defaults() : hive4Frontend;
```

and add the matching builder field and setter next to line 87.

In `ProxyConfigLoader.java`, beside line 70:

```java
    Hive4FrontendConfig hive4Frontend = Hive4FrontendConfigParser.parse(reader);
```

and beside line 89:

```java
        .hive4Frontend(hive4Frontend)
```

- [ ] **Step 5: Run the config test and watch it pass**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=ProxyConfigLoaderTest
```

Expected: green, with the two new tests included in the count.

- [ ] **Step 6: Write the failing bridge test**

Add to `src/test/java/io/github/mmalykhin/hmsproxy/frontend/Hive4FrontendBridgeTest.java`. It drives the real Hive 4 `create_table_req` wrapper and asserts on the Apache-side `Table` the bridge hands the backend:

```java
  @Test
  public void createTableThroughTheHive4WrapperNormalizesAnIcebergDescriptor() throws Exception {
    Assume.assumeTrue(Files.isReadable(HIVE_4_JAR));
    AtomicReference<Table> received = new AtomicReference<>();

    ThriftHiveMetastore.Iface apacheHandler = proxyHandler((proxy, method, args) -> {
      if ("create_table_with_environment_context".equals(method.getName())) {
        received.set((Table) args[0]);
        return null;
      }
      throw new UnsupportedOperationException(method.getName());
    });

    Hive4FrontendBridge.BridgeBundle bridge =
        Hive4FrontendBridge.createBridge(config(), apacheHandler);
    Object hive4Table = newHive4IcebergTable(bridge.classLoader());
    Class<?> requestClass = bridge.classLoader()
        .loadClass("org.apache.hadoop.hive.metastore.api.CreateTableRequest");
    Object request = requestClass.getConstructor(hive4Table.getClass()).newInstance(hive4Table);
    Method method = bridge.ifaceClass().getMethod("create_table_req", requestClass);

    method.invoke(bridge.handlerProxy(), request);

    Assert.assertNotNull("the bridge never reached the backend", received.get());
    Assert.assertEquals("org.apache.iceberg.mr.hive.HiveIcebergInputFormat",
        received.get().getSd().getInputFormat());
  }

  /** A Hive 4 Table carrying what `STORED BY ICEBERG` produces: the abstract input format. */
  private static Object newHive4IcebergTable(ClassLoader loader) throws Exception {
    Class<?> tableClass = loader.loadClass("org.apache.hadoop.hive.metastore.api.Table");
    Class<?> sdClass = loader.loadClass("org.apache.hadoop.hive.metastore.api.StorageDescriptor");
    Class<?> serdeClass = loader.loadClass("org.apache.hadoop.hive.metastore.api.SerDeInfo");
    Object sd = sdClass.getConstructor().newInstance();
    sdClass.getMethod("setInputFormat", String.class)
        .invoke(sd, "org.apache.hadoop.mapred.FileInputFormat");
    sdClass.getMethod("setSerdeInfo", serdeClass)
        .invoke(sd, serdeClass.getConstructor().newInstance());
    Object table = tableClass.getConstructor().newInstance();
    tableClass.getMethod("setDbName", String.class).invoke(table, "default");
    tableClass.getMethod("setTableName", String.class).invoke(table, "events");
    tableClass.getMethod("setSd", sdClass).invoke(table, sd);
    tableClass.getMethod("setParameters", java.util.Map.class).invoke(table,
        java.util.Map.of("storage_handler",
            "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"));
    return table;
  }
```

If `CreateTableRequest` has no single-argument constructor in this Hive 4 jar, build it with the no-arg constructor and `setTable`; check the class with `javap` using the full JDK path (a bare `javap` prints "Unable to locate a Java Runtime"):

```bash
/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19/Contents/Home/bin/javap \
  -cp hive-metastore/hive-standalone-metastore-common-4.1.0.jar \
  org.apache.hadoop.hive.metastore.api.CreateTableRequest | head -20
```

- [ ] **Step 7: Run it and watch it fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=Hive4FrontendBridgeTest
```

Expected: FAIL with `expected:<org.apache.iceberg.mr.hive.HiveIcebergInputFormat> but was:<org.apache.hadoop.mapred.FileInputFormat>` — the bridge passes the descriptor through untouched today.

- [ ] **Step 8: Wire the normalizer into the bridge**

In `Hive4FrontendBridge.createBridge` (line ~82), build the normalizer from the config and hand it to the handler:

```java
        new BridgeInvocationHandler(classLoader, apacheHandler,
            new IcebergStorageDescriptorNormalizer(
                config.hive4Frontend().normalizeIcebergStorageDescriptor())));
```

Add the field and constructor parameter to `BridgeInvocationHandler` (lines 105-118):

```java
    private final IcebergStorageDescriptorNormalizer descriptorNormalizer;
```

Call it in `handleCreateTableReq` immediately after the conversion at line 203:

```java
      Table table = (Table) ThriftValueConverter.convertTBase(invokeNoArgs(request, "getTable"), Table.class);
      descriptorNormalizer.normalize(table);
```

and identically in `handleAlterTableReq` after line 259.

- [ ] **Step 9: Run it and watch it pass**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=Hive4FrontendBridgeTest
```

Expected: green.

- [ ] **Step 10: Check whether the shared positional path also needs the call**

```bash
grep -n 'create_table_with_environment_context\|"create_table"' \
  src/main/java/io/github/mmalykhin/hmsproxy/frontend/Hive4FrontendBridge.java
```

A Hive 4 client uses the `*_req` wrappers for create and alter, so the shared path should not carry an Iceberg create. If the grep shows those methods reachable on the shared path, add the same `normalize` call there and a test mirroring Step 6; if it does not, write one sentence in the commit message recording that this was checked and why no call was added.

- [ ] **Step 11: Document the key in the example properties**

Add to `src/main/resources/hms-proxy-example.properties`, near the other frontend settings:

```properties
# A Hive 4 client declaring STORED BY ICEBERG leaves the descriptor's inputFormat as the abstract
# org.apache.hadoop.mapred.FileInputFormat, which a Hive 3.1 client cannot instantiate. The proxy
# rewrites it to the concrete format Iceberg's own HiveTableOperations writes. Turn this off only
# if a Hive release starts relying on the abstract value.
# frontend.hive4.normalize-iceberg-storage-descriptor=true
```

- [ ] **Step 12: Run the whole suite**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test
```

Expected: `BUILD SUCCESS`, total count 702 plus the new tests.

- [ ] **Step 13: Commit** (stage and show the diff; wait for the user)

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/config/frontend/ \
        src/main/java/io/github/mmalykhin/hmsproxy/config/ProxyConfig.java \
        src/main/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoader.java \
        src/main/java/io/github/mmalykhin/hmsproxy/frontend/Hive4FrontendBridge.java \
        src/main/resources/hms-proxy-example.properties \
        src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java \
        src/test/java/io/github/mmalykhin/hmsproxy/frontend/Hive4FrontendBridgeTest.java
git commit -m "Apply the Iceberg descriptor normalization on the Hive 4 front door"
```

---

### Task 4: Prove it on the stand, then write it down

**Files:**
- Modify: `smoke-stand/TEST-MATRIX.md`, `smoke-stand/TEST-MATRIX.ru.md` (H12 row, the asymmetry paragraph, and a revalidation-log entry)
- Modify: `AGENTS.md` (the carve-out sentence the spec requires)
- Modify: `README.md`, `README.ru.md` (the Hive 4 client row's compatibility note)
- Modify: `CHANGELOG.md`, `CHANGELOG.ru.md`

- [ ] **Step 1: Stage the new jar onto the stand**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o package -DskipTests
cd smoke-stand && ./prepare.sh
docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe up -d --build --no-deps proxy
```

Confirm the container is running the jar just built:

```bash
docker exec stand-proxy sh -c 'ls -l /opt/hms-proxy/hms-proxy.jar' | awk '{print $5}'
ls -l target/hms-proxy-*-fat.jar | awk '{print $5}'
```

Expected: the two byte counts match.

- [ ] **Step 2: Run the scenario that failed in Task 1, plain profile**

```bash
cd smoke-stand && ./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4
```

Expected: 5 rows, all four front doors agreeing — where Task 1 Step 2 saw 3 rows and an `InputFormat` failure.

- [ ] **Step 3: Confirm Hive 4 still reads its own table**

The interop scenario already has Hive 4 read the table it created; confirm that participant is green in the same run rather than assuming it. If the scenario's output does not make this explicit, read the table directly:

```bash
docker exec stand-hs2-hive4 beeline -u 'jdbc:hive2://hs2-hive4:10000/default' -n hive \
  --silent=true --showHeader=false --outputformat=tsv2 \
  -e 'select id, src from smoke_iceberg_interop order by id;'
```

The whole design rests on Hive 4 ignoring this field; that assumption gets tested, not trusted.

- [ ] **Step 4: Repeat on the Kerberos profile**

```bash
cd smoke-stand
docker compose --env-file .env.hive4-kerberos --profile kerberos --profile hive4 --profile hive4fe up -d
./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4 --kerberos
```

Note: switching profiles recreates the HDFS chain; the documented stale-DNS restart applies. Switching only the proxy does not work — the metastores keep their own auth and answer `500`.

- [ ] **Step 5: Update the H12 row and the asymmetry paragraph in both matrices**

The H12 row currently reads "REST only → 3 rows; the 3.1-line engines **cannot** read it". Change it to record the new behaviour and what makes it work, keeping the ❌-by-design framing out of it since it now passes. The asymmetry paragraph below the table must stop saying "nothing here is a routing or compatibility decision it could make differently" — that sentence becomes false the moment this ships. Rewrite it to say what the proxy now does and why the abstract value was safe to replace.

- [ ] **Step 6: Add the AGENTS.md carve-out**

The rule at `AGENTS.md` line 86 says Iceberg-ness is decided only in `IcebergTablePointerGuard`. Add one sentence recording that `frontend/IcebergStorageDescriptorNormalizer` fires on the client's declared `storage_handler` rather than deciding Iceberg-ness, that on `create_table` there is no record to read, and that its purpose is descriptor portability rather than pointer protection.

- [ ] **Step 7: Update README (both languages) and CHANGELOG (both languages)**

In README the Hive 4 client row lists the compatibility downgrades (`lock` with `EXCL_WRITE`); add the descriptor normalization beside it. In CHANGELOG add a `### Fixed` entry under a `## 2026-07-31` heading describing the defect, the measured target shape, the config key, and the stand evidence.

- [ ] **Step 8: Add a revalidation-log entry to both matrices**

Record: the jar under test, both profiles, the before/after row counts, and the trap that Task 1 exists to prevent (a green run before the fix would have invalidated the evidence).

- [ ] **Step 9: Verify the documentation pairs stayed in sync**

```bash
grep -c 'normalize-iceberg-storage-descriptor' README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md
grep -c -iE 'карьер|ворота' CHANGELOG.ru.md smoke-stand/TEST-MATRIX.ru.md README.ru.md
```

Expected: the first command reports a non-zero count for every file; the second reports 0 everywhere.

- [ ] **Step 10: Commit** (stage and show the diff; wait for the user)

```bash
git add AGENTS.md README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md \
        smoke-stand/TEST-MATRIX.md smoke-stand/TEST-MATRIX.ru.md
git commit -m "Record H12 as fixed: the 3.1 line now reads a Hive 4-created Iceberg table"
```

---

## Self-review notes

- **Spec coverage:** problem and target shape → Task 1; scope and placement → Task 3 Steps 8 and 10; trigger and the AGENTS carve-out → Task 2 Step 3 and Task 4 Step 6; rewrite table → Task 2 Steps 3 and 5; configuration → Task 3 Steps 3-4 and 11; error handling → Task 2 Step 1's `anUnexpectedlyShapedTableIsSurvivedWithoutThrowing`; testing → Tasks 2-4; the open unknown → Task 1.
- **Known deviation:** the spec's Prometheus counter is replaced by an INFO log, for the reason recorded at the top of this plan. Confirm with the user before Task 3.
- **Naming consistency:** `IcebergStorageDescriptorNormalizer(boolean)` / `normalize(Table)` / `ProxyConfig.hive4Frontend()` / `normalizeIcebergStorageDescriptor()` are used identically in Tasks 2, 3 and 4.
- **One deliberate soft spot:** Task 3 Step 1 reuses `ProxyConfigLoaderTest`'s existing harness rather than inventing helper names, because that file's helpers were not read while planning. The implementer must read the top of that file first.
