# Iceberg REST Phase 2: Multi-Catalog Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose every configured proxy catalog through the Iceberg REST listener as its own prefix (`/v1/<catalog>/...`), read-only, per `docs/superpowers/specs/2026-07-27-iceberg-rest-phase2-multicatalog-design.md`.

**Architecture:** A per-catalog `IcebergRestService` registry built at startup; the default catalog keeps the phase-1 federated view (untranslated client), every other catalog gets a name-translating `IMetaStoreClient` layer so Iceberg responses are built from internal names and the proxy keeps seeing external ones. `GET /v1/config?warehouse=<catalog>` selects the prefix.

**Tech Stack:** Java 17, JUnit 4, existing `RecordingThriftIface` fake, Iceberg 1.5.2 vendored adapter. Build/test only with `JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19` and offline Maven (`mvn -o`).

## Global Constraints

- Java 17 compatibility; 2-space indent, explicit imports (AGENTS.md).
- No new dependencies.
- Config keys unchanged: no new `rest-catalog.*` keys in this phase.
- Docs are bilingual: every EN doc change lands with its RU counterpart in the same task.
- Commit messages in English, no Claude attribution footers.
- The user has pre-approved commits for this implementation (spec commits together with it); do NOT push without an explicit command.

---

### Task 1: CatalogNameTranslation

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/CatalogNameTranslation.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/CatalogNameTranslationTest.java`

**Interfaces:**
- Produces: `CatalogNameTranslation(String catalogName, String separator)`, `String toExternal(String internalDb)`, `String fromExternalOrNull(String externalDb)`, `List<String> internalNames(List<String> externalDbs)`.

- [ ] **Step 1: Write the failing test**

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 mvn -o -Dtest=CatalogNameTranslationTest test`
Expected: compilation FAILURE — `CatalogNameTranslation` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.List;
import java.util.Objects;

/**
 * Maps database names between a non-default catalog's internal view ("default")
 * and the proxy's external federated view ("apache__default"). The REST layer
 * shows internal names; the proxy keeps seeing external ones, so federation,
 * exposure rules and access modes stay untouched.
 */
final class CatalogNameTranslation {
  private final String externalPrefix;

  CatalogNameTranslation(String catalogName, String separator) {
    this.externalPrefix = Objects.requireNonNull(catalogName, "catalogName")
        + Objects.requireNonNull(separator, "separator");
  }

  String toExternal(String internalDb) {
    return externalPrefix + internalDb;
  }

  String fromExternalOrNull(String externalDb) {
    if (externalDb == null || !externalDb.startsWith(externalPrefix)) {
      return null;
    }
    String internal = externalDb.substring(externalPrefix.length());
    return internal.isEmpty() ? null : internal;
  }

  List<String> internalNames(List<String> externalDbs) {
    return externalDbs.stream()
        .map(this::fromExternalOrNull)
        .filter(Objects::nonNull)
        .toList();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=... mvn -o -Dtest=CatalogNameTranslationTest test`
Expected: `Tests run: 3, Failures: 0`

- [ ] **Step 5: Commit** (spec + plan + this task)

```bash
git add docs/superpowers src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/CatalogNameTranslation.java src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/CatalogNameTranslationTest.java
git commit -m "Add catalog name translation for the REST multi-catalog phase"
```

---

### Task 2: Name-translating RoutingMetaStoreClient

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClient.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClientTest.java` (add cases)

**Interfaces:**
- Consumes: `CatalogNameTranslation` from Task 1.
- Produces: `RoutingMetaStoreClient.create(ThriftHiveMetastore.Iface delegate)` (unchanged, no translation) and `RoutingMetaStoreClient.create(ThriftHiveMetastore.Iface delegate, CatalogNameTranslation translation)` (translation may be null = untranslated).

- [ ] **Step 1: Add failing tests to RoutingMetaStoreClientTest**

```java
  @Test
  public void scopedClientTranslatesDatabaseArguments() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    recording.databases.put("apache__default", RecordingThriftIface.database("apache__default"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Database db = client.getDatabase("default");
    assertEquals("default", db.getName());
    assertEquals(List.of("get_database:apache__default"), recording.calls);
  }

  @Test
  public void scopedClientFiltersAndStripsDatabaseListing() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    recording.allDatabases = List.of("default", "apache__default", "hdp__x");
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    assertEquals(List.of("default"), client.getAllDatabases());
  }

  @Test
  public void scopedClientRewritesTableDbName() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    recording.tables.put("apache__default.t1", RecordingThriftIface.table("apache__default", "t1"));
    IMetaStoreClient client = RoutingMetaStoreClient.create(
        recording.iface, new CatalogNameTranslation("apache", "__"));
    Table t = client.getTable("default", "t1");
    assertEquals("default", t.getDbName());
  }
```

(Match the existing test file's helpers: `RecordingThriftIface` keys tables by `db.name` — check its `get_table` handler and use the same key form.)

- [ ] **Step 2: Run to verify failure**

Run: `JAVA_HOME=... mvn -o -Dtest=RoutingMetaStoreClientTest test`
Expected: compilation FAILURE — no two-argument `create`.

- [ ] **Step 3: Implement translation inside the invocation handler**

In `RoutingMetaStoreClient`:

```java
  public static IMetaStoreClient create(ThriftHiveMetastore.Iface delegate) {
    return create(delegate, null);
  }

  public static IMetaStoreClient create(
      ThriftHiveMetastore.Iface delegate, CatalogNameTranslation translation) {
    Objects.requireNonNull(delegate, "delegate");
    return (IMetaStoreClient) Proxy.newProxyInstance(
        IMetaStoreClient.class.getClassLoader(),
        new Class<?>[]{IMetaStoreClient.class},
        new RoutingInvocationHandler(delegate, translation));
  }
```

`RoutingInvocationHandler` gains a nullable `translation` field. Inside `invoke`:
- helper `String db(String internal)` returns `translation == null ? internal : translation.toExternal(internal)`; apply to every first-String db argument (`getDatabase`, `getDatabases` pattern, `getAllTables`, `getTables`, `getTable`, `getTableObjectsByName`, `tableExists`).
- `getAllDatabases`: `translation == null ? delegate.get_all_databases() : translation.internalNames(delegate.get_all_databases())`.
- `getDatabases`: same wrap of `delegate.get_databases(db(pattern))`.
- `getDatabase`: result rewritten when translated: `Database copy = new Database(result); copy.setName(translation.fromExternalOrNull(result.getName()) != null ? translation.fromExternalOrNull(result.getName()) : result.getName()); return copy;` — extract a small `Database rewriteDatabase(Database)` / `Table rewriteTable(Table)` pair of private helpers (thrift copy constructors, then `setName`/`setDbName` with the internal name when `fromExternalOrNull` is non-null).
- `getTable` and each element of `getTableObjectsByName`: `rewriteTable`.

- [ ] **Step 4: Run to verify pass**

Run: `JAVA_HOME=... mvn -o -Dtest=RoutingMetaStoreClientTest test`
Expected: all existing + 3 new tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClient.java src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClientTest.java
git commit -m "Teach the REST metastore client to translate catalog-scoped names"
```

---

### Task 3: Per-catalog IcebergRestService

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java`
- Modify (call sites): `src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java` (temporarily keeps compiling — final wiring in Task 5), tests that construct the service (`IcebergRestEndpointIntegrationTest`, `RestCatalogServerTest`, `SpnegoIntegrationTest` — check with `grep -rn "new IcebergRestService" src/`).

**Interfaces:**
- Consumes: two-arg `RoutingMetaStoreClient.create` from Task 2.
- Produces: `IcebergRestService(String catalogName, ThriftHiveMetastore.Iface delegate, CatalogNameTranslation translationOrNull)`; `String catalogName()`; `supportsPrefix(String)` and `loadConfig()` now keyed on `catalogName`.

- [ ] **Step 1: Change the constructor**

```java
  private final String catalogName;

  public IcebergRestService(
      String catalogName,
      ThriftHiveMetastore.Iface delegate,
      CatalogNameTranslation translationOrNull) {
    this.catalogName = Objects.requireNonNull(catalogName, "catalogName");
    Objects.requireNonNull(delegate, "delegate");
    IMetaStoreClient client = RoutingMetaStoreClient.create(delegate, translationOrNull);
    this.catalog = new RoutingHiveCatalog(client, new Configuration());
    this.catalog.initialize(catalogName, Map.of(CatalogProperties.URI, UNUSED_URI));
    this.adapter = new RESTCatalogAdapter(catalog);
  }

  public String catalogName() {
    return catalogName;
  }
```

Drop the `ProxyConfig config` field; `supportsPrefix` compares against `catalogName`, `loadConfig()` uses `.withOverride("prefix", catalogName)`.

- [ ] **Step 2: Update every constructor call site**

Old form `new IcebergRestService(config, proxy)` becomes
`new IcebergRestService(config.defaultCatalog(), proxy, null)` — in `HmsProxyApplication` and each test found by the grep.

- [ ] **Step 3: Run the restcatalog tests**

Run: `JAVA_HOME=... mvn -o -Dtest='io.github.mmalykhin.hmsproxy.restcatalog.*' test`
Expected: all pass (behavior unchanged for the default catalog).

- [ ] **Step 4: Commit**

```bash
git add -A src/main src/test
git commit -m "Key IcebergRestService on an explicit catalog name"
```

---

### Task 4: IcebergRestServices registry

**Files:**
- Create: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServices.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServicesTest.java`

**Interfaces:**
- Consumes: `IcebergRestService` (Task 3), `ProxyConfig` (`catalogNames()`, `defaultCatalog()`, `catalogDbSeparator()`).
- Produces: `static IcebergRestServices open(ProxyConfig config, ThriftHiveMetastore.Iface delegate)`; `IcebergRestService serviceFor(String prefix)` (null when unknown); `IcebergRestService byWarehouse(String warehouseOrNull)` (null warehouse → default service, unknown → null); `String defaultPrefix()`; `close()` closes every service.

- [ ] **Step 1: Write the failing test**

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class IcebergRestServicesTest {
  @Test
  public void registryServesEveryConfiguredCatalog() throws Exception {
    RecordingThriftIface recording = new RecordingThriftIface();
    // build a two-catalog ProxyConfig the same way IcebergRestEndpointIntegrationTest.buildConfig does,
    // with catalogs "hdp" (default) and "apache"
    try (IcebergRestServices services = IcebergRestServices.open(buildTwoCatalogConfig(), recording.iface)) {
      assertEquals("hdp", services.defaultPrefix());
      assertNotNull(services.serviceFor("hdp"));
      assertNotNull(services.serviceFor("apache"));
      assertNull(services.serviceFor("nope"));
      assertEquals("hdp", services.byWarehouse(null).catalogName());
      assertEquals("apache", services.byWarehouse("apache").catalogName());
      assertNull(services.byWarehouse("nope"));
    }
  }
}
```

(Write `buildTwoCatalogConfig()` by copying `IcebergRestEndpointIntegrationTest.buildConfig()` and adding a second `CatalogConfig` entry; keep the same builder fields.)

- [ ] **Step 2: Run to verify it fails** (class missing), then implement:

```java
package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.ProxyConfig;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;

/**
 * Prefix -> per-catalog REST service registry. The default catalog keeps the
 * phase-1 federated view (no name translation); every other catalog gets a
 * clean, name-translated view. Built eagerly so a broken configuration fails
 * the proxy start, not the first REST request.
 */
public final class IcebergRestServices implements AutoCloseable {
  private final Map<String, IcebergRestService> byPrefix;
  private final String defaultPrefix;

  private IcebergRestServices(Map<String, IcebergRestService> byPrefix, String defaultPrefix) {
    this.byPrefix = byPrefix;
    this.defaultPrefix = defaultPrefix;
  }

  public static IcebergRestServices open(ProxyConfig config, ThriftHiveMetastore.Iface delegate) {
    Map<String, IcebergRestService> services = new LinkedHashMap<>();
    for (String catalog : config.catalogNames()) {
      CatalogNameTranslation translation = catalog.equals(config.defaultCatalog())
          ? null
          : new CatalogNameTranslation(catalog, config.catalogDbSeparator());
      services.put(catalog, new IcebergRestService(catalog, delegate, translation));
    }
    return new IcebergRestServices(services, config.defaultCatalog());
  }

  public IcebergRestService serviceFor(String prefix) {
    return byPrefix.get(prefix);
  }

  public IcebergRestService byWarehouse(String warehouseOrNull) {
    if (warehouseOrNull == null || warehouseOrNull.isEmpty()) {
      return byPrefix.get(defaultPrefix);
    }
    return byPrefix.get(warehouseOrNull);
  }

  public String defaultPrefix() {
    return defaultPrefix;
  }

  @Override
  public void close() throws IOException {
    for (IcebergRestService service : byPrefix.values()) {
      service.close();
    }
  }
}
```

- [ ] **Step 3: Run to verify pass**

Run: `JAVA_HOME=... mvn -o -Dtest=IcebergRestServicesTest test`

- [ ] **Step 4: Commit**

```bash
git add src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServices.java src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestServicesTest.java
git commit -m "Build a per-catalog registry of Iceberg REST services"
```

---

### Task 5: Multi-prefix HTTP routing and warehouse discovery

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RestCatalogServer.java` (`open(ProxyConfig, IcebergRestServices)`)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: `IcebergRestServices` (Task 4).
- Produces: HTTP behavior per spec — clean second prefix, federated default prefix, `?warehouse=` selection, 400 unknown warehouse, 404 unknown prefix.

- [ ] **Step 1: Extend the integration test with a second catalog**

Rework `buildConfig()` into a two-catalog config (`catalog1` default + `catalog2`, separator `__`). Seed `RecordingThriftIface.allDatabases = List.of("default", "catalog2__default")` and tables for both views. Add tests:

```java
  @Test
  public void configWithWarehouseSelectsCatalogPrefix() throws Exception {
    HttpResponse<String> response = get("/v1/config?warehouse=catalog2");
    assertEquals(200, response.statusCode());
    assertTrue(response.body().contains("\"prefix\":\"catalog2\""));
  }

  @Test
  public void configWithUnknownWarehouseReturns400() throws Exception {
    assertEquals(400, get("/v1/config?warehouse=nope").statusCode());
  }

  @Test
  public void secondPrefixShowsCleanView() throws Exception {
    HttpResponse<String> response = get("/v1/catalog2/namespaces");
    assertEquals(200, response.statusCode());
    assertTrue(response.body().contains("[\"default\"]"));
    assertFalse(response.body().contains("catalog2__default"));
  }

  @Test
  public void defaultPrefixKeepsFederatedView() throws Exception {
    HttpResponse<String> response = get("/v1/catalog1/namespaces");
    assertEquals(200, response.statusCode());
    assertTrue(response.body().contains("[\"catalog2__default\"]"));
  }
```

- [ ] **Step 2: Run to verify failures** (constructor/type mismatches first — fix wiring as part of this task).

- [ ] **Step 3: Implement**

`IcebergHttpHandler` takes `IcebergRestServices services`; `doHandle` parses the first path segment itself:
- `config` segment → `handleConfig(exchange, queryParams)`: `IcebergRestService svc = services.byWarehouse(queryParams.get("warehouse"))`; null → `writeError(exchange, 400, "BadRequestException", "Unknown warehouse: " + ...)`; else serialize `svc.loadConfig()`.
- otherwise `IcebergRestService svc = services.serviceFor(prefix)`; null → existing 404 `NoSuchCatalogException`; else dispatch with the remaining relative path exactly as today (`stripPrefixSegment` logic folds into this parse; delete `supportsPrefix` usage — keep the method on the service or drop it if unused after the change; drop it from `IcebergRestService` only if no test references remain).

`RestCatalogServer.open(ProxyConfig config, IcebergRestServices services)` — same null-check contract as today (`services == null` → skeleton `ConfigHandler`; that branch now only serves the never-enabled case, keep it).

`HmsProxyApplication` try-with-resources:

```java
        try (AdditionalFrontendThriftServers extras =
            AdditionalFrontendThriftServers.open(config, proxy, frontDoorSecurity);
             IcebergRestServices restServices =
                 config.restCatalog().enabled() ? IcebergRestServices.open(config, proxy) : null;
             RestCatalogServer restServer = RestCatalogServer.open(config, restServices)) {
```

- [ ] **Step 4: Run the full restcatalog suite**

Run: `JAVA_HOME=... mvn -o -Dtest='io.github.mmalykhin.hmsproxy.restcatalog.*' test`
Expected: all pass, including Spnego tests (they only wrap the handler in an authenticator).

- [ ] **Step 5: Full test run**

Run: `JAVA_HOME=... mvn -o test`
Expected: `BUILD SUCCESS`, 0 failures, 0 skipped.

- [ ] **Step 6: Commit**

```bash
git add -A src/main src/test
git commit -m "Serve every configured catalog as an Iceberg REST prefix"
```

---

### Task 6: Documentation (both locales)

**Files:**
- Modify: `README.md`, `README.ru.md` — Iceberg REST section: multi-catalog behavior table (`{prefix}` = any configured catalog; default prefix keeps the federated view, other prefixes are clean; `?warehouse=` discovery; 400 negative), drop the "multi-catalog is planned" caveat.
- Modify: `CHANGELOG.md`, `CHANGELOG.ru.md` — entry under 2026-07-27 describing phase 2 (multi-catalog prefixes, warehouse discovery, federated default view kept for compatibility).
- Modify: `src/main/resources/hms-proxy-example.properties` — only if it mentions single-catalog REST wording; adjust the comment.

- [ ] **Step 1: Update all files listed above** — EN and RU in the same edit batch.
- [ ] **Step 2: Commit**

```bash
git add README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md src/main/resources/hms-proxy-example.properties
git commit -m "Document multi-catalog Iceberg REST prefixes"
```

---

### Task 7: Smoke coverage and stand run

**Files:**
- Modify: `scripts/run-real-installation-smoke.sh` — in `run_rest_smoke`, after the existing checks add an optional second-prefix block driven by `HMS_SMOKE_REST_SECOND_PREFIX` (+ optional `HMS_SMOKE_REST_SECOND_ICEBERG_TABLE`):

```bash
  local second_prefix="${HMS_SMOKE_REST_SECOND_PREFIX:-}"
  if [[ -n "${second_prefix}" ]]; then
    code="$(rest_request GET "/v1/config?warehouse=${second_prefix}" "${body}")"
    [[ "${code}" == "200" ]] || fail "config?warehouse=${second_prefix} returned HTTP ${code}: $(cat "${body}")"
    grep -q "\"prefix\"[[:space:]]*:[[:space:]]*\"${second_prefix}\"" "${body}" \
      || fail "warehouse discovery did not advertise prefix '${second_prefix}': $(cat "${body}")"

    code="$(rest_request GET "/v1/config?warehouse=no_such_warehouse_smoke" "${body}")"
    [[ "${code}" == "400" ]] || fail "unknown warehouse expected HTTP 400, got ${code}: $(cat "${body}")"

    code="$(rest_request GET "/v1/${second_prefix}/namespaces" "${body}")"
    [[ "${code}" == "200" ]] || fail "GET /v1/${second_prefix}/namespaces returned HTTP ${code}: $(cat "${body}")"
    grep -q "\[\"${namespace}\"\]" "${body}" \
      || fail "namespace '${namespace}' missing under prefix '${second_prefix}': $(cat "${body}")"
    if grep -q "${second_prefix}__" "${body}"; then
      fail "external names leaked into the clean view of '${second_prefix}': $(cat "${body}")"
    fi

    if [[ -n "${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE:-}" ]]; then
      code="$(rest_request GET "/v1/${second_prefix}/namespaces/${namespace}/tables/${HMS_SMOKE_REST_SECOND_ICEBERG_TABLE}" "${body}")"
      [[ "${code}" == "200" ]] || fail "REST load under '${second_prefix}' returned HTTP ${code}: $(cat "${body}")"
      grep -q '"metadata-location"' "${body}" \
        || fail "second-prefix load carries no metadata-location: $(cat "${body}")"
    fi
  fi
```

  Also document both variables in the usage text next to the other `HMS_SMOKE_REST_*` lines.
- Modify: `scripts/hms-real-installation-smoke.simple.env.example` — add the two commented keys.
- Modify: `smoke-stand/env/simple.env` — set `HMS_SMOKE_REST_SECOND_PREFIX=apache`, `HMS_SMOKE_REST_SECOND_ICEBERG_TABLE=smoke_iceberg_tbl_ap`.
- Modify: `smoke-stand/README.md` + `smoke-stand/README.ru.md` — extend the REST section: the second Iceberg table lives in the `apache` catalog on `namenode-b` (`hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap`), registered the same way as the first one.
- Modify after the run: `smoke-stand/TEST-MATRIX.md` + `.ru.md` — extend section G (multi-catalog rows G8+: warehouse discovery, clean second view, 400 negative, second-prefix load) and the revalidation log.

- [ ] **Step 1: Implement the runner/env/README changes; `bash -n` the runner.**
- [ ] **Step 2: Bring the stand up** (`cd smoke-stand && docker compose up -d`; containers were only stopped, state is preserved). Rebuild + restage the fat jar first: `JAVA_HOME=... mvn -o -DskipTests package && cd smoke-stand && ./prepare.sh`, then `docker compose up -d --build` to pick the new jar.
- [ ] **Step 3: Register the second Iceberg table** — metadata.json (copy of the first with `location`/`table-uuid` adjusted, e.g. uuid `7a9d0e3f-5c8b-4d2e-af40-3b6c9d8e7f20`) onto `stand-namenode-b` under `/warehouse/apache/smoke_iceberg_tbl_ap/metadata/00000-smoke.metadata.json`, then Beeline `create external table if not exists apache__default.smoke_iceberg_tbl_ap ... location 'hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap' tblproperties ('table_type'='ICEBERG','metadata_location'='hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap/metadata/00000-smoke.metadata.json')`.
- [ ] **Step 4: Run** `scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario rest`, then `--scenario all`. Expected: `completed successfully` both times.
- [ ] **Step 5: Update TEST-MATRIX (EN+RU) with the observed results; commit everything.**

```bash
git add scripts smoke-stand
git commit -m "Cover multi-catalog REST prefixes in the stand smoke"
```

---

## Self-Review

- Spec coverage: prefixes/registry (T4, T5), federated default view (T2 null-translation + T5 test), clean views (T1, T2, T5), warehouse discovery + 400 (T5), unknown prefix 404 (unchanged, asserted in T5 suite), read-only unchanged (no write-path edits anywhere), no new config keys (none added), unit/integration/smoke/docs (T1–T7). No gaps found.
- Placeholders: none — every code step carries concrete code.
- Type consistency: `CatalogNameTranslation` (T1) used in T2/T4; two-arg `create` (T2) used in T3; `IcebergRestService(String, Iface, CatalogNameTranslation)` (T3) used in T4; `IcebergRestServices` (T4) used in T5. Names match.
