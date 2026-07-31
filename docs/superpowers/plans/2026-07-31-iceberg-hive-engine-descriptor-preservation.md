# Preserving the Hive-engine descriptor across REST commits — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the proxy's Iceberg REST commits from stripping a Hive-created Iceberg table's storage handler and format classes, which today cuts the 3.1 line off from a table it could read a moment earlier.

**Architecture:** Iceberg's `HiveTableOperations` writes the Hive-engine descriptor only when the Hive engine is enabled. The proxy sets `iceberg.engine.hive.enabled=true` on a **copy** of the per-catalog Hadoop `Configuration` handed to each REST service, behind a key defaulting to on. No new class, no front-door logic.

**Tech Stack:** Java 17, Maven, JUnit 4, Iceberg 1.9.2, docker-compose smoke stand.

**Spec:** `docs/superpowers/specs/2026-07-31-iceberg-hive-engine-descriptor-preservation-design.md`

## Global Constraints

- Build and test only on JDK 17: `JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19`.
- Commits, commit messages and code comments in English. Never add `Co-Authored-By: Claude` or any Claude/Anthropic attribution.
- Local commits are authorized. `git push` requires an explicit instruction from the user.
- Work directly in `main`; do not create branches.
- EN/RU doc pairs change together: `README.md`/`README.ru.md`, `CHANGELOG.md`/`CHANGELOG.ru.md`, `smoke-stand/TEST-MATRIX.md`/`.ru.md`.
- In Russian text never use "карьер" for carrier or "ворота" for gate.
- Config key, fixed by the spec: `rest-catalog.hive-engine-descriptor`, default `true`.
- Iceberg property to set: `iceberg.engine.hive.enabled=true`.
- **Never mutate the `Configuration` returned by `hadoopConfForCatalog`** — in production it is `router.requireBackend(catalog).hiveConf()`, shared with the Thrift path. Copy it first.

## State at plan time

Already done during diagnosis, uncommitted in the working tree:

- `smoke-stand/run-iceberg-interop-smoke.sh` — the `SKIPPED` carve-out that removed `hdp` and `apache` from every `--origin hive4` run is deleted, together with the stale comment block and the skip log loop.

Already measured, so no task needs to re-derive it:

- A Hive 4-created Iceberg table starts with a concrete descriptor and both 3.1 engines read it.
- After a REST append it degrades to `FileInputFormat` / `FileOutputFormat` / `LazySimpleSerDe` with `storage_handler` gone, and `hdp` fails with `Cannot create an instance of InputFormat class`.
- With `engine.hive.enabled=true` set on the table by hand, the same REST append leaves the descriptor intact and the 3.1 engine returns both rows.

The stand is up on `.env.hive4` with profiles `hive4`, `hive4fe`, `hdp`.

---

### Task 1: Config key and wiring, with the failing stand run recorded first

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfig.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/RestCatalogConfigParser.java:60-62`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServices.java:47-61`
- Modify: `src/main/resources/hms-proxy-example.properties`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServicesTest.java`

**Interfaces:**
- Produces: `RestCatalogConfig.hiveEngineDescriptor()` returning `boolean`, default `true`.

- [ ] **Step 1: Record the pre-fix failure against the jar under test**

Build and stage the current code, then run the four-participant scenario:

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o package -DskipTests
cd smoke-stand && ./prepare.sh
docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe --profile hdp up -d --build --no-deps proxy
./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4
```

Expected: FAIL at the `hdp` participant with `Cannot create an instance of InputFormat class org.apache.hadoop.mapred.FileInputFormat`. Save the output to `.superpowers/sdd/task-1-prefix-failure.log`.

**If it passes, stop and report.** The fix would then be unverifiable, exactly as the superseded plan's first task discovered.

- [ ] **Step 2: Write the failing config test**

Read the top of `ProxyConfigLoaderTest` and reuse its own harness for building properties and loading a config — do not invent helper names. Add:

```java
  @Test
  public void restCatalogHiveEngineDescriptorDefaultsToOn() throws Exception {
    ProxyConfig config = /* load with the file's existing helper, base properties */;

    Assert.assertTrue(config.restCatalog().hiveEngineDescriptor());
  }

  @Test
  public void restCatalogHiveEngineDescriptorCanBeTurnedOff() throws Exception {
    ProxyConfig config = /* load with the file's existing helper, plus: */
        // rest-catalog.hive-engine-descriptor=false

    Assert.assertFalse(config.restCatalog().hiveEngineDescriptor());
  }
```

- [ ] **Step 3: Run it and watch it fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=ProxyConfigLoaderTest
```

Expected: compilation failure — `cannot find symbol: method hiveEngineDescriptor()`.

- [ ] **Step 4: Add the record component and parse it**

In `RestCatalogConfig`, add `boolean hiveEngineDescriptor` as the last component, with this javadoc above the record:

```java
/**
 * <p>{@code hiveEngineDescriptor} makes the proxy's REST commits write the Hive-engine storage
 * descriptor. Iceberg's HiveTableOperations otherwise rewrites the table with
 * {@code FileInputFormat}, {@code FileOutputFormat} and {@code LazySimpleSerDe} and drops
 * {@code storage_handler}, which leaves a Hive-created Iceberg table unreadable by the 3.1 line
 * after a single REST append. A table that sets {@code engine.hive.enabled} itself keeps its own
 * choice: the table property takes precedence over this configuration.
 */
```

In `RestCatalogConfigParser`, read it beside the other keys and pass it to the constructor at lines 60-62:

```java
    boolean hiveEngineDescriptor = reader.getBoolean("rest-catalog.hive-engine-descriptor", true);
    return new RestCatalogConfig(
        enabled, bindHost, port, minWorkerThreads, maxWorkerThreads, principal, keytab,
        purgeMode, purgeAllowedPrefixes, hiveEngineDescriptor);
```

- [ ] **Step 5: Run the config test and watch it pass**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=ProxyConfigLoaderTest
```

Expected: green.

- [ ] **Step 6: Write the failing wiring test**

In `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServicesTest.java`, assert both the flag's effect and that the supplied Configuration is not mutated:

```java
  @Test
  public void theRestCatalogGetsHiveEngineDescriptorsWithoutMutatingTheBackendConfiguration() {
    Configuration backendConf = new Configuration(false);

    IcebergRestServices.open(config(), delegate, db -> null, catalog -> backendConf);

    Assert.assertNull("the backend's shared Configuration must not be written to",
        backendConf.get("iceberg.engine.hive.enabled"));
  }
```

Reuse the file's existing `config()` and delegate helpers. To assert what the service actually received, add whatever accessor the file's existing tests already use; if none exists, assert on the not-mutated half only and record in the report that the positive half is covered by the stand run in Task 2 — do not invent an accessor solely for the test.

- [ ] **Step 7: Run it and watch it fail**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test -Dtest=IcebergRestServicesTest
```

Expected: FAIL — today the flag is never applied, so a version of this test that asserts the flag IS applied fails; the non-mutation half passes trivially and must not be mistaken for the whole test.

- [ ] **Step 8: Apply the flag on a copy, in `IcebergRestServices.open`**

Inside the `for (String catalog : config.catalogNames())` loop, replace the direct
`hadoopConfForCatalog.apply(catalog)` argument with:

```java
      // A copy, never the backend's own Configuration: in production that object is
      // router.requireBackend(catalog).hiveConf(), shared with the Thrift path, and writing to it
      // would change behaviour far outside the REST front door.
      Configuration catalogConf = new Configuration(hadoopConfForCatalog.apply(catalog));
      if (config.restCatalog().hiveEngineDescriptor()) {
        catalogConf.setBoolean("iceberg.engine.hive.enabled", true);
      }
```

and pass `catalogConf` to the `IcebergRestService` constructor.

- [ ] **Step 9: Run both tests and the full suite**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o test
```

Expected: `BUILD SUCCESS`, 702 plus the new tests.

- [ ] **Step 10: Document the key**

Add to `src/main/resources/hms-proxy-example.properties`, next to the other `rest-catalog.*` keys:

```properties
# Iceberg's HiveTableOperations writes a Hive-readable storage descriptor only when the Hive engine
# is enabled. Without this, a REST commit rewrites a Hive-created Iceberg table with
# FileInputFormat/FileOutputFormat/LazySimpleSerDe and drops storage_handler, and Hive 3.1 clients
# can no longer read it. A table that sets engine.hive.enabled itself always wins over this.
# rest-catalog.hive-engine-descriptor=true
```

- [ ] **Step 11: Commit**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/config/restcatalog/ \
        src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServices.java \
        src/main/resources/hms-proxy-example.properties \
        src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java \
        src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServicesTest.java
git commit -m "Write the Hive-engine storage descriptor from REST commits"
```

---

### Task 2: Prove it on the stand, and make the scenario able to catch a relapse

**Files:**
- Modify: `smoke-stand/run-iceberg-interop-smoke.sh` (descriptor assertion; the carve-out is already deleted)

**Interfaces:**
- Consumes: the config key and wiring from Task 1.

- [ ] **Step 1: Stage the fixed jar**

```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o package -DskipTests
cd smoke-stand && ./prepare.sh
docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe --profile hdp up -d --build --no-deps proxy
```

Confirm the container runs the jar just built by comparing byte counts:

```bash
docker exec stand-proxy sh -c 'ls -l /opt/hms-proxy/hms-proxy.jar' | awk '{print $5}'
ls -l target/hms-proxy-*-fat.jar | awk '{print $5}'
```

- [ ] **Step 2: Re-run the scenario that failed in Task 1 Step 1**

```bash
cd smoke-stand && ./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4
```

Expected: passes with all four participants, ending at 5 rows.

- [ ] **Step 3: Verify the self-repair claim the spec makes**

The spec claims a table already degraded is repaired by its next REST commit. Test it directly rather than trusting it:

```bash
docker exec stand-hs2-hive4 beeline -u 'jdbc:hive2://hs2-hive4:10000/default' -n hive --silent=true \
  -e "drop table if exists h12_repair; create table h12_repair (id int, src string) stored by iceberg; insert into h12_repair values (1,'hive4');"
```

Degrade it by committing with the flag off (restart the proxy with `rest-catalog.hive-engine-descriptor=false`, append through REST, confirm the descriptor is broken), then restart with the default and append again. Assert the descriptor comes back:

```bash
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true --outputformat=tsv2 \
  -e 'describe formatted h12_repair;'" | grep -iE 'inputformat|storage_handler'
```

Expected: `HiveIcebergInputFormat` and the storage handler present again. Drop the probe table afterwards.

If the repair does not happen, say so and correct the spec — the claim is the spec's, not a fact yet.

- [ ] **Step 4: Add a descriptor assertion to the scenario**

A row count alone cannot catch a relapse, because Hive 4 keeps reading a degraded table. After the REST participant's append, assert the descriptor through a 3.1 front door:

```bash
assert_hive_readable_descriptor() {
  local stage="$1" descriptor
  descriptor="$(beeline_query "describe formatted ${TABLE};" | grep -iE 'inputformat' | head -n 1)"
  case "${descriptor}" in
    *HiveIcebergInputFormat*) log "descriptor still Hive-readable after ${stage}" ;;
    *) fail "after ${stage} the descriptor lost its Hive input format: ${descriptor}" ;;
  esac
}
```

Wire the helper into the scenario after the REST append, using the script's own `beeline_query`/`fail`/`log` helpers and its existing engine selection. Prove the assertion discriminates by running it once against a table you degraded on purpose (Step 3 already produces one).

- [ ] **Step 5: Run both origins to check nothing regressed**

```bash
cd smoke-stand
./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4
./run-iceberg-interop-smoke.sh --prefix hive4 --origin rest
```

Expected: both green.

- [ ] **Step 6: Repeat on the Kerberos profile**

```bash
cd smoke-stand
docker compose --env-file .env.hive4-kerberos --profile kerberos --profile hive4 --profile hive4fe --profile hdp up -d
./run-iceberg-interop-smoke.sh --prefix hive4 --origin hive4 --kerberos
```

Switching profiles recreates the HDFS chain; the documented stale-DNS restart applies. Switching only the proxy does not work — the metastores keep their own auth and answer `500`.

- [ ] **Step 7: Commit**

```bash
git add smoke-stand/run-iceberg-interop-smoke.sh
git commit -m "Assert the Hive-readable descriptor survives a REST commit"
```

---

### Task 3: Correct the documentation that states the wrong diagnosis

**Files:**
- Modify: `smoke-stand/TEST-MATRIX.md`, `smoke-stand/TEST-MATRIX.ru.md`
- Modify: `AGENTS.md`
- Modify: `README.md`, `README.ru.md`
- Modify: `CHANGELOG.md`, `CHANGELOG.ru.md`

- [ ] **Step 1: Rewrite the H12 row and the asymmetry paragraph in both matrices**

The row currently reads "REST only → 3 rows; the 3.1-line engines **cannot** read it". The paragraph below the table claims the asymmetry "is Hive's, not the proxy's" and that "the proxy passes the descriptor through unchanged in both directions; nothing here is a routing or compatibility decision it could make differently".

Both are false and must go. Replace with what was measured: a Hive 4-created table starts readable everywhere; the proxy's own REST commit used to strip the storage handler and format classes; `rest-catalog.hive-engine-descriptor` now prevents that; all four participants pass.

- [ ] **Step 2: Record what the carve-out cost, in both matrices**

Add a short note where the scenario is described: the run used to remove `hdp` and `apache` whenever the origin was Hive 4, so it asserted the limitation instead of testing it and could not notice that the limitation had the wrong cause — or that the real defect was a data-accessibility regression the proxy itself caused. State the general rule plainly: a skip must never stand in for a check.

- [ ] **Step 3: Fix `AGENTS.md`**

Find the sentence repeating the old explanation (the `smoke-stand/` bullet, the H12/interop part) and replace it with the corrected one, naming the config key.

- [ ] **Step 4: Fix `README.md` and `README.ru.md`**

Correct wherever the Hive 4 interop limitation is described, and document `rest-catalog.hive-engine-descriptor` alongside the other `rest-catalog.*` keys.

- [ ] **Step 5: Add the CHANGELOG entries, both languages**

Under a `## 2026-07-31` heading, `### Fixed`. Cover: what broke (one REST append cost a Hive-created table its 3.1 readability), the mechanism, the key and its default, self-repair on the next commit, the table property taking precedence, and the stand evidence. Add a second entry for the scenario change — the carve-out deleted and the descriptor assertion added — since that is what made the defect findable.

- [ ] **Step 6: Check the pairs stayed in sync**

```bash
grep -c 'hive-engine-descriptor' README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md
grep -c -iE 'карьер|ворота' CHANGELOG.ru.md smoke-stand/TEST-MATRIX.ru.md README.ru.md
grep -rn 'FileInputFormat' smoke-stand/TEST-MATRIX.md AGENTS.md README.md
```

Expected: the first reports non-zero everywhere; the second reports 0; the third shows only the corrected, historical framing — no surviving claim that Hive 4 produces an abstract input format.

- [ ] **Step 7: Add a revalidation-log entry to both matrices**

Record the jar, both profiles, the before/after of the scenario, the controlled A/B that pinned the mechanism, and the inconclusive first experiment (`Cannot set unknown field named: src` — the probe table had one column while the writer writes two, so the descriptor stayed concrete because no commit had happened).

- [ ] **Step 8: Commit**

```bash
git add AGENTS.md README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md \
        smoke-stand/TEST-MATRIX.md smoke-stand/TEST-MATRIX.ru.md
git commit -m "Correct the H12 diagnosis: the proxy stripped the descriptor, not Hive 4"
```

---

## Self-review notes

- **Spec coverage:** defect and mechanism → Task 1 Step 1 and Task 2; fix → Task 1 Steps 4 and 8; configuration → Task 1 Steps 4, 10; the non-mutation requirement → Task 1 Steps 6 and 8; self-repair → Task 2 Step 3; relapse detection → Task 2 Step 4; documentation → Task 3.
- **Deliberate soft spots, both flagged in-place:** Task 1 Step 2 and Step 6 reuse existing test harnesses that were not read while planning; the implementer reads those files first. Task 1 Step 6's positive assertion may have no existing accessor, and the plan says what to do instead rather than inventing one.
- **Naming consistency:** `rest-catalog.hive-engine-descriptor` / `RestCatalogConfig.hiveEngineDescriptor()` / `iceberg.engine.hive.enabled` are used identically across all three tasks.
