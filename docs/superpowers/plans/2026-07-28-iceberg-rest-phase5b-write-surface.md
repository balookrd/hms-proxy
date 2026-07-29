# Iceberg REST Phase 5b: Completing the Write Surface Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement namespace DDL and make the view writes and transaction commits phase 5a inadvertently enabled official — advertised, tested, documented — per `docs/superpowers/specs/2026-07-28-iceberg-rest-phase5b-write-surface-design.md`.

**Architecture:** Three client methods (`createDatabase`, `dropDatabase`, `alterDatabase`) are the only missing implementation; everything else already works because 5a's table plumbing is what `HiveViewOperations` and the transaction commit path use. The rest of the phase is advertising, coverage and documentation.

**Tech Stack:** Java 17 (`JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19`), Maven offline (`mvn -o`), JUnit 4, Iceberg 1.9.2, the docker-compose smoke stand.

## Global Constraints

- Java 17; 2-space indent; explicit imports; no new dependencies.
- The default-catalog write restriction is unchanged and must not be relaxed: `WriteRouteGate` keys on the catalog the namespace resolves to, never on the URL prefix, and already covers all thirteen write routes. No task may narrow it.
- Advertise only what is actually served and permitted; non-default catalogs keep advertising reads only.
- Phase-3 per-request metrics bookkeeping (`RequestOutcome`, the `finally` recording) must keep working.
- Bilingual docs: every EN doc change lands with its RU counterpart in the same commit.
- English commit messages, no attribution footers. Commits pre-approved; do NOT push.
- Never run `docker compose down` on the stand — HDFS data and registered tables live in volumes. The stand is currently in the **Kerberos** profile; compose calls for it need `--env-file .env.kerberos --profile kerberos`.

---

### Task 1: Namespace DDL in RoutingMetaStoreClient

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClient.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClientTest.java`
- Test fixture: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RecordingThriftIface.java`

**Interfaces:**
- Produces: the client answers `createDatabase`, `dropDatabase`, `alterDatabase` instead of throwing `UnsupportedOperationException`.

- [ ] **Step 1: Learn the exact signatures**

```bash
JAVAP=~/Library/Java/JavaVirtualMachines/liberica-17.0.19/bin/javap
mkdir -p /tmp/imsc && unzip -o -q hive-metastore/hive-standalone-metastore-3.1.3.jar \
  'org/apache/hadoop/hive/metastore/IMetaStoreClient.class' -d /tmp/imsc
$JAVAP -cp /tmp/imsc org.apache.hadoop.hive.metastore.IMetaStoreClient \
  | grep -E 'createDatabase|dropDatabase|alterDatabase'
```

Record the output in the report and implement only the overloads Iceberg calls — confirm which by disassembling `HiveCatalog`:

```bash
mkdir -p /tmp/hto && unzip -o -q ~/.m2/repository/org/apache/iceberg/iceberg-hive-metastore/1.9.2/iceberg-hive-metastore-1.9.2.jar -d /tmp/hto
$JAVAP -c -p -cp /tmp/hto org.apache.iceberg.hive.HiveCatalog | grep -E 'createDatabase|dropDatabase|alterDatabase'
```

Leave every other overload throwing, as the existing branches do.

- [ ] **Step 2: Write the failing tests**

Add to `RoutingMetaStoreClientTest`, in the file's existing style (build a `RecordingThriftIface`, wrap it, call, assert on `recording.calls` and on the returned value). Cover the untranslated client and a `new CatalogNameTranslation("apache", "__")` one. The `Database` argument carries its own name, so — exactly as the phase-5a `alter_table` test learned the hard way — the fixture must record the **Database's own name**, not only the string argument, or a missing translation would not fail the test:

```java
  @Test
  public void createDatabaseTranslatesName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    client.createDatabase(RecordingThriftIface.database("sales"));
    assertEquals(List.of("create_database:apache__sales"), recording.calls);
  }

  @Test
  public void dropDatabaseTranslatesName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    client.dropDatabase("sales", false, true, false);
    assertEquals(List.of("drop_database:apache__sales"), recording.calls);
  }

  @Test
  public void alterDatabaseTranslatesBothArgumentAndPayload() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    client.alterDatabase("sales", RecordingThriftIface.database("sales"));
    assertEquals(List.of("alter_database:apache__sales:apache__sales"), recording.calls);
  }
```

Adjust the `dropDatabase` overload and the recorded strings to whatever Step 1 showed; the third test's two-part recording (argument name and payload name) is the point — keep that shape.

- [ ] **Step 3: Run to verify red**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -Dtest=RoutingMetaStoreClientTest test
```

Expected: `UnsupportedOperationException: HMS proxy REST gateway does not support IMetaStoreClient.<method>`.

- [ ] **Step 4: Implement**

Add the branches to the switch in `RoutingInvocationHandler.invoke`, translating with the existing `db(...)` helper. For `createDatabase` and `alterDatabase` translate the `Database` payload's own name on a **copy** (thrift copy constructor, as `rewriteDatabase` already does), never by mutating the caller's object. Delegate to `delegate.create_database(...)`, `delegate.drop_database(...)`, `delegate.alter_database(...)` per the Thrift interface.

- [ ] **Step 5: Prove the tests discriminate**

Temporarily remove the payload translation from `alterDatabase`, run `alterDatabaseTranslatesBothArgumentAndPayload`, confirm it FAILS, restore, confirm green. Record both runs — phase 5a shipped a test that could not fail because the fixture ignored the payload, and this step exists so that cannot recur.

- [ ] **Step 6: Run green, then the package**

```bash
mvn -o -Dtest=RoutingMetaStoreClientTest test
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

- [ ] **Step 7: Commit** (include the phase-5b spec and this plan, both untracked)

```bash
git add src/main src/test docs/superpowers
git commit -m "Let the REST metastore client create, alter and drop namespaces"
```

---

### Task 2: Advertise the full served write surface

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java` (`WRITE_ENDPOINTS`)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: namespace DDL from Task 1.
- Produces: the default catalog's `endpoints` lists table writes, view writes, namespace DDL and the transaction commit; other catalogs list reads only.

- [ ] **Step 1: Confirm which `Endpoint` constants exist and how they render**

Iceberg 1.9.2's `org.apache.iceberg.rest.Endpoint` is the source of truth. List its constants and print the ones you intend to add so the assertions match the real serialized form:

```bash
JAVAP=~/Library/Java/JavaVirtualMachines/liberica-17.0.19/bin/javap
mkdir -p /tmp/ep && unzip -o -q ~/.m2/repository/org/apache/iceberg/iceberg-core/1.9.2/iceberg-core-1.9.2.jar \
  'org/apache/iceberg/rest/Endpoint.class' -d /tmp/ep
$JAVAP -constants -cp /tmp/ep org.apache.iceberg.rest.Endpoint | grep -E 'V1_(CREATE|UPDATE|DELETE|REPLACE|RENAME)_(VIEW|NAMESPACE)|V1_COMMIT_TRANSACTION|V1_UPDATE_NAMESPACE'
```

Report which constants exist. If one you expected is absent, do not invent it — say so and leave that route unadvertised, noting the mismatch.

- [ ] **Step 2: Write the failing tests**

```java
  @Test
  public void defaultCatalogAdvertisesViewAndNamespaceWrites() throws Exception {
    String body = get("/v1/catalog1/config").body();
    assertTrue(body, body.contains("POST /v1/{prefix}/namespaces/{namespace}/views"));
    assertTrue(body, body.contains("POST /v1/{prefix}/namespaces"));
    assertTrue(body, body.contains("POST /v1/{prefix}/transactions/commit"));
  }

  @Test
  public void nonDefaultCatalogAdvertisesNoWritesAtAll() throws Exception {
    String body = get("/v1/catalog2/config").body();
    assertFalse(body, body.contains("POST /v1/{prefix}/namespaces/{namespace}/views"));
    assertFalse(body, body.contains("POST /v1/{prefix}/transactions/commit"));
    assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
  }
```

Replace each expected string with the real rendering Step 1 printed if it differs.

- [ ] **Step 3: Run red, implement, run green**

Extend `WRITE_ENDPOINTS` with the constants Step 1 confirmed. Keep the read list and the non-default behaviour untouched.

```bash
mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

- [ ] **Step 4: Full suite**

```bash
mvn -o test
```

Expected: 0 failures, 0 skipped.

- [ ] **Step 5: Commit**

```bash
git add src/main src/test
git commit -m "Advertise the view, namespace and transaction writes the front door serves"
```

---

### Task 3: Smoke coverage, stand runs, documentation

**Files:**
- Modify: `scripts/run-real-installation-smoke.sh` — extend the `HMS_SMOKE_REST_WRITE_TABLE`-guarded block
- Modify: `smoke-stand/env/simple.env`, `scripts/hms-real-installation-smoke.simple.env.example` if new variables are needed
- Modify: `README.md`, `README.ru.md`, `CHANGELOG.md`, `CHANGELOG.ru.md`
- Modify after the runs: `smoke-stand/TEST-MATRIX.md`, `smoke-stand/TEST-MATRIX.ru.md`

- [ ] **Step 1: Add the smoke checks**

Three round trips, each asserting the effect rather than only a status. I verified all three shapes against the live stand, so these are known-good:

*Namespace DDL:* `POST /v1/${prefix}/namespaces` with `{"namespace":["smoke_rest_ns"]}` → 200; `GET /v1/${prefix}/namespaces/smoke_rest_ns` → 200; `POST /v1/${prefix}/namespaces/smoke_rest_ns/properties` with `{"removals":[],"updates":{"smoke":"yes"}}` → 200; `GET` it again and assert the property is present in the body; `DELETE /v1/${prefix}/namespaces/smoke_rest_ns` → 204; `GET` again → 404.

*View writes:* `POST /v1/${prefix}/namespaces/${namespace}/views` with
`{"name":"smoke_rest_view","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]},"view-version":{"version-id":1,"timestamp-ms":1753700000000,"schema-id":0,"summary":{"operation":"create"},"default-namespace":["${namespace}"],"representations":[{"type":"sql","sql":"select 1","dialect":"hive"}]},"properties":{}}`
→ 200 and the body carries a `metadata-location`; `GET .../views` lists it; `DELETE .../views/smoke_rest_view` → 204.

*Transaction commit:* create a table, capture its `table-uuid` and `metadata-location`, then `POST /v1/${prefix}/transactions/commit` with
`{"table-changes":[{"identifier":{"namespace":["${namespace}"],"name":"<table>"},"requirements":[{"type":"assert-table-uuid","uuid":"<uuid>"}],"updates":[{"action":"set-properties","updates":{"txn":"yes"}}]}]}`
→ 204, then GET the table and assert its `metadata-location` DIFFERS from the captured one. That difference is the proof the transaction actually committed; a no-op must fail the smoke.

Use the runner's existing `rest_request`/`fail` idioms and its grep/sed JSON extraction (no jq). Drop leftovers defensively at the start so a rerun on a dirty stand cannot half-fail.

- [ ] **Step 2: `bash -n scripts/run-real-installation-smoke.sh` passes.**

- [ ] **Step 3: Rebuild, restage and run on the plain profile**

The stand is currently in the Kerberos profile. Rebuild and restage the jar, then switch it to the plain profile for these runs:

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -DskipTests package
cd smoke-stand && ./prepare.sh && docker compose up -d --build
```

Poll until healthy, then:

```bash
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario rest
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario all
```

Both must end `completed successfully`.

- [ ] **Step 4: Prove one new assertion can fail**

Break the transaction-commit assertion (demand the `metadata-location` be unchanged), rerun, confirm the runner reports the failure, restore, rerun green. Record both runs.

- [ ] **Step 5: Re-run the Kerberos profile for the write paths**

Phase 5a's Kerberos pass is what exposed the missing `HiveConf` wiring, so the new write paths get the same treatment:

```bash
cd smoke-stand && docker compose --env-file .env.kerberos --profile kerberos up -d --build
```

Poll until healthy, then from inside `stand-proxy` (`kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL`, then `curl --negotiate -u :` against `http://proxy:9183`), drive the namespace round trip and the view round trip and report each response. If either fails under Kerberos but passed on the plain profile, that is a real finding — report it rather than working around it.

- [ ] **Step 6: Docs and matrix, both locales; commit everything**

The documentation must say plainly that view writes and transaction commits were already reachable after phase 5a and that this phase makes them official — advertised, covered and documented — rather than implying they are new. State that namespace DDL is genuinely new. Repeat the default-catalog restriction and that it applies to every one of these routes.

```bash
git add scripts smoke-stand README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md
git commit -m "Cover the completed Iceberg REST write surface in the stand smoke"
```

---

## Self-Review

- **Spec coverage:** namespace DDL client methods (T1); advertising the full served write surface (T2); smoke for all three round trips plus the discriminating transaction assertion (T3 Steps 1, 4); Kerberos re-run for the new write paths (T3 Step 5); bilingual docs stating honestly what was already reachable (T3 Step 6); the `CREATE_NAMESPACE` federated-name gate test already exists from 5a and is explicitly left in place (Global Constraints forbid narrowing the gate). No gaps.
- **Placeholder scan:** none — every request body is spelled out and was probed against the live stand; the one conditional (a missing `Endpoint` constant) names exactly what to do and to report.
- **Type consistency:** `createDatabase`/`dropDatabase`/`alterDatabase` are declared in T1's Interfaces and consumed by T2's advertising; `WRITE_ENDPOINTS` is the existing field name from phase 5a Task 3; `RoutingMetaStoreClient.create(delegate, translation)` is the existing two-arg factory.
