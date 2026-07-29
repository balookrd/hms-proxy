# Iceberg REST Phase 5a: Table Writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Iceberg REST clients create, commit to, rename, register and drop tables in the proxy's default catalog, per `docs/superpowers/specs/2026-07-28-iceberg-rest-phase5a-table-writes-design.md`.

**Architecture:** Two pieces. `RoutingMetaStoreClient` gains the write and lock methods Iceberg's `HiveTableOperations` and `MetastoreLock` need, each with the same name translation the read branches use. A gate in the REST layer refuses write routes whose target namespace resolves to a catalog other than `routing.default-catalog`, because only that catalog has real locks — the synthetic shim grants without conflict checking, so a commit elsewhere would silently lose updates.

**Tech Stack:** Java 17 (`JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19`), Maven offline (`mvn -o`), JUnit 4, Iceberg 1.9.2, the local docker-compose smoke stand.

## Global Constraints

- Java 17; 2-space indent; explicit imports; no new dependencies.
- Writes are permitted **only** where the target namespace resolves to `routing.default-catalog`. Everywhere else they are refused — this is the safety property the whole phase exists to preserve, not a configuration preference.
- The gate keys on the resolved catalog, never on the URL prefix: the default prefix exposes other catalogs' databases as `<catalog><separator><db>`, and a write to such a federated name must be refused.
- Reuse `CatalogRouter.resolveDatabase(String)` for that resolution; do not add a second parser of `<catalog><separator><db>` anywhere.
- `REPORT_METRICS` is a POST but is not a catalog write — it must keep answering 204 and must never be caught by the gate. Gate on an explicit write-route set, never on the HTTP method.
- Refusals use `403 ForbiddenException` with a message naming the cause. No other status changes.
- Do not weaken or bypass the routing layer's own guards (`CatalogAccessModeGuard`, transactional-DDL guard); REST writes must face what Thrift writes face.
- Phase-3 per-request metrics bookkeeping (`RequestOutcome`, the `finally` recording) must keep working.
- Bilingual docs: every EN doc change lands with its RU counterpart in the same commit.
- English commit messages, no attribution footers. Commits pre-approved; do NOT push.
- Never run `docker compose down` on the stand — its HDFS data and registered tables live in volumes.

---

### Task 1: Write and lock methods in RoutingMetaStoreClient

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClient.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClientTest.java`
- Test fixture: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RecordingThriftIface.java` (add handlers for the new RPCs)

**Interfaces:**
- Produces: the client proxy answers `createTable`, `dropTable`, `alter_table_with_environmentContext`, `lock`, `checkLock`, `unlock`, `showLocks`, `heartbeat` instead of throwing `UnsupportedOperationException`.

- [ ] **Step 1: Learn the exact interface signatures**

The switch dispatches on method name and parameter shape, so the signatures must be exact:

```bash
JAVAP=~/Library/Java/JavaVirtualMachines/liberica-17.0.19/bin/javap
JAR=$(ls hive-metastore/hive-standalone-metastore-3.1.3.jar)
mkdir -p /tmp/imsc && unzip -o -q "$JAR" 'org/apache/hadoop/hive/metastore/IMetaStoreClient.class' -d /tmp/imsc
$JAVAP -cp /tmp/imsc org.apache.hadoop.hive.metastore.IMetaStoreClient \
  | grep -E 'createTable|dropTable|alter_table|[^a-zA-Z]lock|checkLock|unlock|showLocks|heartbeat'
```

Record the output in your report. Implement the overloads Iceberg actually calls (see Step 3); leave the others throwing, as the existing read branches do for unused overloads.

- [ ] **Step 2: Write the failing tests**

Add to `RoutingMetaStoreClientTest`, following the file's existing style (it builds a `RecordingThriftIface`, wraps it, calls the client, and asserts on both `recording.calls` and the returned value). Cover both the untranslated client and a `new CatalogNameTranslation("apache", "__")` one:

```java
  @Test
  public void createTableTranslatesDatabaseName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Table table = RecordingThriftIface.table("default", "t1");
    client.createTable(table);
    assertEquals(List.of("create_table:apache__default.t1"), recording.calls);
  }

  @Test
  public void dropTableTranslatesDatabaseName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    client.dropTable("default", "t1", false, true);
    assertEquals(List.of("drop_table:apache__default.t1"), recording.calls);
  }

  @Test
  public void alterTableTranslatesDatabaseName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Table table = RecordingThriftIface.table("default", "t1");
    client.alter_table_with_environmentContext("default", "t1", table, null);
    assertEquals(List.of("alter_table:apache__default.t1"), recording.calls);
  }

  @Test
  public void lockAndUnlockReachTheDelegate() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    IMetaStoreClient client = RoutingMetaStoreClient.create(recording.iface);
    LockResponse response = client.lock(new LockRequest());
    assertEquals(RecordingThriftIface.LOCK_ID, response.getLockid());
    client.unlock(RecordingThriftIface.LOCK_ID);
    assertEquals(List.of("lock", "unlock:" + RecordingThriftIface.LOCK_ID), recording.calls);
  }
```

`RecordingThriftIface` needs matching handlers: `create_table`, `drop_table`, `alter_table_with_environment_context`, `lock`, `check_lock`, `unlock`, `show_locks`, `heartbeat`, each appending to `calls` in the shape asserted above, and `lock`/`check_lock` returning a `LockResponse` with a constant `LOCK_ID` and `LockState.ACQUIRED`. Read the fixture first and match its existing conventions.

- [ ] **Step 3: Run to verify they fail**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -Dtest=RoutingMetaStoreClientTest test
```

Expected: the new cases fail with `UnsupportedOperationException: HMS proxy REST gateway does not support IMetaStoreClient.<method>`.

- [ ] **Step 4: Implement the branches**

Add cases to the switch in `RoutingInvocationHandler.invoke`, each translating the database argument with the existing `db(...)` helper and rewriting results with the existing `rewriteTable`/`rewriteDatabase` helpers where a `Table` or `Database` comes back:

- `createTable`: the argument is a `Table`; translate its `dbName` before delegating (copy it the way `rewriteTable` does rather than mutating the caller's object), then call `delegate.create_table(...)`.
- `dropTable`: match the overload Iceberg uses and delegate to `delegate.drop_table(...)` / `drop_table_with_environment_context(...)` as the signature dictates.
- `alter_table_with_environmentContext`: translate the db argument and the `Table`'s own `dbName`, then delegate to `delegate.alter_table_with_environment_context(...)`.
- `lock`, `checkLock`, `unlock`, `showLocks`, `heartbeat`: delegate straight through. **Do not translate anything inside a `LockRequest`** — its components carry database names that the proxy's own `LockHandler` resolves, and translating here would double-translate. Note this reasoning in a short comment, since it is the one place the pattern differs from the others.

- [ ] **Step 5: Run to verify green, then the package**

```bash
mvn -o -Dtest=RoutingMetaStoreClientTest test
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

- [ ] **Step 6: Commit** (include the phase-5a spec and this plan, both untracked)

```bash
git add src/main src/test docs/superpowers
git commit -m "Let the REST metastore client issue table writes and commit locks"
```

---

### Task 2: The default-catalog write gate

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/WriteRouteGate.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java` (consult the gate before dispatch)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java` and `IcebergRestServices.java` (carry what the gate needs)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java` (pass the `CatalogRouter`)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/WriteRouteGateTest.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: `CatalogRouter.resolveDatabase(String externalDbName)` returning `CatalogRouter.ResolvedNamespace` with `catalogName()`; `RESTCatalogAdapter.Route`.
- Produces: `WriteRouteGate.check(Route route, Map<String,String> vars, Object body)` returning `null` when allowed, or a refusal message when not.

- [ ] **Step 1: Write the gate's unit tests**

```java
public class WriteRouteGateTest {
  @Test
  public void readRoutesAreAlwaysAllowed() { /* LOAD_TABLE against any namespace -> null */ }

  @Test
  public void reportMetricsIsNotTreatedAsAWrite() { /* REPORT_METRICS -> null */ }

  @Test
  public void writeToDefaultCatalogNamespaceIsAllowed() { /* CREATE_TABLE, ns "default" -> null */ }

  @Test
  public void writeToFederatedNamespaceUnderDefaultPrefixIsRefused() {
    /* CREATE_TABLE, ns "apache__default" -> non-null message naming the catalog */
  }

  @Test
  public void renameIsCheckedOnBothSourceAndDestination() {
    /* RENAME_TABLE whose body names a federated source, or a federated destination -> refused */
  }
}
```

Write these out fully against the real `Route` constants and a `CatalogRouter` built the way `IcebergRestEndpointIntegrationTest` builds its `ProxyConfig` (two catalogs, `hdp` default, separator `__`). If constructing a real `CatalogRouter` in a unit test proves to need backends, define the gate to take a narrower collaborator — a `java.util.function.Function<String, String>` mapping an external db name to a catalog name — and have production wire `router::resolveDatabase` composed with `ResolvedNamespace::catalogName` into it. Say in your report which shape you chose and why.

- [ ] **Step 2: Run to verify they fail** (class does not exist).

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -Dtest=WriteRouteGateTest test
```

- [ ] **Step 3: Implement the gate**

The write-route set is explicit — `CREATE_TABLE`, `UPDATE_TABLE`, `DROP_TABLE`, `RENAME_TABLE`, `REGISTER_TABLE` — and everything else, including `REPORT_METRICS` and every read, passes untouched. For the path-shaped routes the namespace comes from `vars.get("namespace")` (URL-decoded the way the handler already decodes it). `RENAME_TABLE` carries no namespace in its path: its body is a `RenameTableRequest` with `source()` and `destination()` `TableIdentifier`s, and **both** namespaces must resolve to the default catalog.

Resolution goes through the collaborator from Step 1; a namespace that does not resolve at all is not the gate's business — let the normal dispatch produce its usual 404.

The refusal message must name the resolved catalog and the reason, for example:
`"Writes are only supported in the default catalog 'hdp'; namespace 'apache__default' belongs to catalog 'apache', which is served by the synthetic lock shim and provides no writer isolation."`

- [ ] **Step 4: Wire it into the handler**

Consult the gate after the route and body are resolved and before `service.dispatch(...)`. On refusal, write `403` with type `ForbiddenException` and the gate's message through the existing `writeError(...)` path, set `outcome.status`/`outcome.route` as the other refusal paths do, and return.

- [ ] **Step 5: Add the integration tests**

In `IcebergRestEndpointIntegrationTest`, using the two-catalog fixture already there:

```java
  @Test
  public void createTableUnderNonDefaultPrefixIsRefused() throws Exception {
    HttpResponse<String> response = post(
        "/v1/catalog2/namespaces/default/tables", "{\"name\":\"t9\",\"schema\":{}}");
    assertEquals(403, response.statusCode());
    assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }

  @Test
  public void createTableUnderFederatedNamespaceIsRefused() throws Exception {
    HttpResponse<String> response = post(
        "/v1/catalog1/namespaces/catalog2__default/tables", "{\"name\":\"t9\",\"schema\":{}}");
    assertEquals(403, response.statusCode());
    assertTrue(response.body(), response.body().contains("ForbiddenException"));
  }
```

The second test is the whole point of the phase's safety argument — a reviewer should be able to see it fail if the gate keys on the URL prefix instead of the resolved catalog. Verify that by temporarily making the gate compare prefixes and confirming this test goes red; record that in your report.

- [ ] **Step 6: Full suite**

```bash
mvn -o test
```

Expected: `BUILD SUCCESS`, 0 failures, 0 skipped.

- [ ] **Step 7: Commit**

```bash
git add src/main src/test
git commit -m "Refuse Iceberg REST writes outside the default catalog"
```

---

### Task 3: Advertise writes only where they are served

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java` (`loadConfig`)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: the service already knows its own `catalogName()`; it needs to know whether it is the default catalog.
- Produces: `loadConfig()` returns the read endpoint list for every catalog, plus the write endpoints for the default catalog only.

- [ ] **Step 1: Write the failing tests**

```java
  @Test
  public void defaultCatalogConfigAdvertisesWrites() throws Exception {
    String body = get("/v1/catalog1/config").body();
    assertTrue(body, body.contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
  }

  @Test
  public void nonDefaultCatalogConfigStillAdvertisesReadsOnly() throws Exception {
    String body = get("/v1/catalog2/config").body();
    assertFalse(body, body.contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
    assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
  }
```

Confirm the exact rendering of each `Endpoint` constant before asserting, the way phase 4b did — print one and match its real serialized form.

- [ ] **Step 2: Run to verify they fail**, then implement: the service takes a flag (or compares its catalog name against the default it is already given) and appends `Endpoint.V1_CREATE_TABLE`, `V1_UPDATE_TABLE`, `V1_DELETE_TABLE`, `V1_RENAME_TABLE` and `V1_REGISTER_TABLE` to the advertised list when it is the default catalog. Verify each constant exists in Iceberg 1.9.2's `org.apache.iceberg.rest.Endpoint` before using it and drop any that does not, saying so in your report.

- [ ] **Step 3: Run to verify green, then the package**

```bash
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

- [ ] **Step 4: Commit**

```bash
git add src/main src/test
git commit -m "Advertise write endpoints only for the default catalog"
```

---

### Task 4: Stand validation, smoke coverage and docs

**Files:**
- Modify: `scripts/run-real-installation-smoke.sh` (a write block in `run_rest_smoke`)
- Modify: `smoke-stand/env/simple.env` and `scripts/hms-real-installation-smoke.simple.env.example`
- Modify: `README.md`, `README.ru.md`, `CHANGELOG.md`, `CHANGELOG.ru.md`
- Modify after the run: `smoke-stand/TEST-MATRIX.md`, `smoke-stand/TEST-MATRIX.ru.md`

- [ ] **Step 1: Rebuild and restage the stand**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -DskipTests package
cd smoke-stand && ./prepare.sh && docker compose up -d --build
```

Poll `docker compose ps` until every container is healthy (up to ~8 min, `sleep 20` between polls).

- [ ] **Step 2: Drive a real write round trip by hand first**

Before writing any assertion, confirm the product actually works end to end, and capture each response verbatim for the report:

```bash
# create
curl -sS -X POST -H 'Content-Type: application/json' \
  -d '{"name":"smoke_rest_written","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":false,"type":"int"}]}}' \
  -w '\n[%{http_code}]\n' http://localhost:19183/v1/hdp/namespaces/default/tables
# read back
curl -s -w '\n[%{http_code}]\n' http://localhost:19183/v1/hdp/namespaces/default/tables/smoke_rest_written
# exists
curl -s -o /dev/null -w '%{http_code}\n' -I http://localhost:19183/v1/hdp/namespaces/default/tables/smoke_rest_written
# drop
curl -s -o /dev/null -w '%{http_code}\n' -X DELETE http://localhost:19183/v1/hdp/namespaces/default/tables/smoke_rest_written
```

If the create fails because HiveCatalog cannot pick a location, report the error rather than inventing configuration: the stand's databases carry a `locationUri`, and `HiveCatalog` derives the default warehouse path from it, so a failure here is a real finding about the interaction.

- [ ] **Step 3: Confirm the commit lock reached the real backend, not the shim**

This is the spec's central safety claim, so observe it rather than assume it:

```bash
docker logs stand-proxy --since 10m 2>&1 | grep -iE 'lock|synthetic' | tail -20
```

Expected: the lock for the write routes to the `hdp` backend. A line showing the synthetic shim serving it is a finding — report it and stop rather than papering over it.

- [ ] **Step 4: Add the smoke block**

In `run_rest_smoke`, guarded by a new `HMS_SMOKE_REST_WRITE_TABLE` (skipped when unset): create the table, assert `200`; load it and assert `metadata-location` comes back; drop it and assert a 2xx; then the two negatives — a create under `${second_prefix}` and a create under the federated name `${second_prefix}${separator}${namespace}` — both asserting `403`. Use the runner's existing `rest_request`/`fail` idioms, and make the create body a single-column schema as in Step 2. Set the variable in `smoke-stand/env/simple.env` and document it, commented, in the example env file and in the runner's usage text.

- [ ] **Step 5: Run the scenarios**

```bash
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario rest
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario all
```

Both must end `completed successfully`.

- [ ] **Step 6: Regression — the SQL layer**

Writes exercise the same lock path Hive uses, so run the SQL suite from inside the HDP HiveServer2 container as the stand README documents, and report the result. If `stand-hs2-hdp` is not running, start it with `cd smoke-stand && docker compose --profile hdp up -d` and wait for health.

- [ ] **Step 7: Docs and matrix, both locales; commit everything**

README and CHANGELOG must state plainly that writes are default-catalog only and why (no writer isolation elsewhere), that the restriction is enforced on the resolved catalog so federated names cannot bypass it, and that discovery advertises the asymmetry. Add matrix rows for the write round trip and the two negatives.

```bash
git add scripts smoke-stand README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md
git commit -m "Cover Iceberg REST table writes in the stand smoke"
```

---

## Self-Review

- **Spec coverage:** client write/lock methods (T1); the gate keyed on resolved catalog including the federated trap and the rename-body case (T2); `REPORT_METRICS` explicitly excluded (T2 Step 3 + its test); reuse of `CatalogRouter.resolveDatabase` (T2 Interfaces); 403 `ForbiddenException` (T2 Step 4); inherited routing guards untouched (no task modifies them; the SQL regression in T4 Step 6 is the evidence); asymmetric endpoint advertising (T3); stand round trip, lock observation and negatives (T4); bilingual docs (T4 Step 7). No gaps.
- **Placeholder scan:** the gate-collaborator shape in T2 Step 1 is a genuine fork with both branches specified and a reporting requirement, not a deferred decision. No TBDs.
- **Type consistency:** `WriteRouteGate.check(Route, Map<String,String>, Object)` returning a nullable message is declared in T2's Interfaces and used with that shape in T2 Steps 3-4; `CatalogRouter.ResolvedNamespace.catalogName()` matches the record in `CatalogRouter.java:179`; `RoutingMetaStoreClient.create(delegate, translation)` is the existing two-arg factory used in T1's tests.
