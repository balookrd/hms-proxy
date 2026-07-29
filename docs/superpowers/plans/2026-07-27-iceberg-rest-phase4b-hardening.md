# Iceberg REST Phase 4b: Read-Path Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the remaining wire-behavior defects of the Iceberg REST front door per `docs/superpowers/specs/2026-07-27-iceberg-rest-phase4b-hardening-design.md`: no stack traces in error bodies, `400` instead of `500` for unparseable bodies, `/v1/config` advertising the endpoints actually served, and unit coverage for the exists routes.

**Architecture:** All changes live in `IcebergHttpHandler` plus a small addition to `IcebergRestService` for the config response. Upstream's `configureResponseFromException` stays the source of truth for exception→status mapping; only the `stack` field is dropped. No new catalog functionality — the endpoint stays read-only.

**Tech Stack:** Java 17 (`JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19`), Maven offline (`mvn -o`), JUnit 4, Iceberg 1.9.2, the local docker-compose smoke stand.

## Global Constraints

- Java 17; 2-space indent; explicit imports; no new dependencies.
- The endpoint stays read-only: no route gains write capability, and the advertised endpoint list must contain only routes actually served.
- Exactly two deliberate status changes: adapter-mapped errors keep their code but lose the `stack` field; unparseable request bodies move from `500` to `400`. No other status may change.
- Do not hand-roll the exception→status mapping; keep `RESTCatalogAdapter.configureResponseFromException` as the source of truth and blank the stack afterwards.
- Phase-3 per-request metrics bookkeeping (`RequestOutcome`, the `finally` recording) must keep working unchanged.
- Bilingual docs: every EN doc change lands with its RU counterpart in the same commit.
- English commit messages, no attribution footers. Commits pre-approved; do NOT push.
- Never run `docker compose down` on the stand — its HDFS data and registered Iceberg tables live in volumes.

---

### Task 1: Stack-free error responses and 400 for unparseable bodies

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Produces: no new public API; behavior only.

- [ ] **Step 1: Write the failing tests**

Add to the integration test, following the file's existing style (it starts a real `RestCatalogServer` over a `RecordingThriftIface` and drives it with an HTTP client):

```java
  @Test
  public void errorResponsesCarryNoStackTrace() throws Exception {
    HttpResponse<String> response = get("/v1/catalog1/namespaces/no_such_ns_probe");
    assertEquals(404, response.statusCode());
    assertTrue(response.body().contains("\"type\""));
    assertFalse("error body must not leak a server stack trace: " + response.body(),
        response.body().contains("\"stack\":[\""));
  }

  @Test
  public void unparseableRequestBodyReturns400() throws Exception {
    HttpResponse<String> response = post(
        "/v1/catalog1/namespaces/default/tables/t1/metrics", "not json at all");
    assertEquals(400, response.statusCode());
    assertTrue(response.body().contains("BadRequestException"));
  }
```

The file currently has only a `get(String)` helper; add a sibling `post(String path, String body)` built the same way (same `HTTP_TIMEOUT`, same base URI, `.POST(HttpRequest.BodyPublishers.ofString(body))`, `Content-Type: application/json`).

- [ ] **Step 2: Run to verify they fail**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
```

Expected: `errorResponsesCarryNoStackTrace` fails because the body contains a populated `stack` array; `unparseableRequestBodyReturns400` fails with `500`.

- [ ] **Step 3: Blank the stack in the exception mapping**

In the `catch (RuntimeException e)` block that currently builds the error response:

```java
      } catch (RuntimeException e) {
        ErrorResponse.Builder builder = ErrorResponse.builder();
        RESTCatalogAdapter.configureResponseFromException(e, builder);
        // Upstream's helper attaches the server stack trace; it is fine in a test
        // harness but this listener answers real clients, so keep the mapped code,
        // type and message and drop the trace.
        builder.withStackTrace(List.of());
        writeErrorResponse(exchange, outcome, builder.build());
        return;
      }
```

Check the rest of the class for any other place that builds an `ErrorResponse` from an exception and give it the same treatment; `writeError(...)`, which builds responses from literal strings, already carries no stack and stays as is.

- [ ] **Step 4: Return 400 for a body that will not parse**

`readBody` currently lets deserialization failures escape to the catch-all. Give it a targeted failure signal — throw a small private exception type or return a sentinel — and answer `400` at the call site:

```java
      Object body;
      try {
        body = readBody(exchange, route);
      } catch (IOException | RuntimeException e) {
        writeError(exchange, outcome, 400, "BadRequestException",
            "Malformed request body: " + e.getMessage());
        return;
      }
```

Keep `readBody`'s own signature and the `drain(...)` path for bodyless routes unchanged. Make sure the `outcome.route` set for this path is the resolved route (the request did match a route), and that `outcome.status` ends up `400` so metrics record it.

- [ ] **Step 5: Run to verify they pass**

```bash
mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
```

Expected: all green, including the pre-existing tests.

- [ ] **Step 6: Run the restcatalog package**

```bash
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

Expected: all green.

- [ ] **Step 7: Commit** (include the phase-4b spec and this plan, both untracked)

```bash
git add src/main src/test docs/superpowers
git commit -m "Stop leaking server stack traces and answer 400 for unparseable bodies"
```

---

### Task 2: Advertise the served endpoints in /v1/config

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java` (`loadConfig`)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java` (route `/v1/{prefix}/config` to the same handler)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `IcebergRestService.loadConfig()` keeps its signature and now returns a `ConfigResponse` carrying the endpoint list.

- [ ] **Step 1: Write the failing tests**

```java
  @Test
  public void configAdvertisesOnlyServedEndpoints() throws Exception {
    HttpResponse<String> response = get("/v1/config");
    assertEquals(200, response.statusCode());
    String body = response.body();
    assertTrue(body, body.contains("GET /v1/{prefix}/namespaces"));
    assertTrue(body, body.contains("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
    assertFalse("read-only endpoint must not advertise writes: " + body,
        body.contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
    assertFalse("read-only endpoint must not advertise deletes: " + body,
        body.contains("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"));
  }

  @Test
  public void prefixedConfigAnswersLikeConfigWithWarehouse() throws Exception {
    HttpResponse<String> response = get("/v1/catalog2/config");
    assertEquals(200, response.statusCode());
    assertTrue(response.body(), response.body().contains("\"prefix\":\"catalog2\""));
    assertFalse("prefixed config must not advertise writes either: " + response.body(),
        response.body().contains("POST /v1/{prefix}/namespaces/{namespace}/tables"));
  }

  @Test
  public void prefixedConfigForUnknownCatalogReturns404() throws Exception {
    assertEquals(404, get("/v1/no_such_catalog_probe/config").statusCode());
  }
```

Before writing the assertions, confirm how `Endpoint` serializes by printing one: it renders as `"<METHOD> <path>"` (for example `GET /v1/{prefix}/namespaces`). If the exact rendering differs, use the real form in the assertions rather than these strings.

- [ ] **Step 2: Run to verify they fail.**

```bash
mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
```

Expected: the first two fail (no `endpoints` field today; `/v1/catalog2/config` currently reaches the adapter and advertises everything).

- [ ] **Step 3: Build the endpoint list in `loadConfig`**

```java
  private static final List<Endpoint> SERVED_ENDPOINTS = List.of(
      Endpoint.V1_LIST_NAMESPACES,
      Endpoint.V1_LOAD_NAMESPACE,
      Endpoint.V1_NAMESPACE_EXISTS,
      Endpoint.V1_LIST_TABLES,
      Endpoint.V1_LOAD_TABLE,
      Endpoint.V1_TABLE_EXISTS,
      Endpoint.V1_LIST_VIEWS,
      Endpoint.V1_LOAD_VIEW,
      Endpoint.V1_VIEW_EXISTS);

  public ConfigResponse loadConfig() {
    return ConfigResponse.builder()
        .withOverride("prefix", catalogName)
        .withEndpoints(SERVED_ENDPOINTS)
        .build();
  }
```

Verify each constant exists in Iceberg 1.9.2's `org.apache.iceberg.rest.Endpoint` before using it (`V1_VIEW_EXISTS` in particular); drop any that does not and say so in the report.

- [ ] **Step 4: Route the prefixed config form**

In `IcebergHttpHandler.doHandle`, the first path segment is resolved to a service before the route lookup. Add: when the remainder after a **known** prefix is exactly `config`, answer from that service's `loadConfig()` — the same writer path `/v1/config` uses, with `outcome.route` set to `config` and the prefix recorded as that catalog. An unknown first segment keeps returning the existing `404` unchanged.

- [ ] **Step 5: Run to verify green, then the whole package**

```bash
mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

- [ ] **Step 6: Commit**

```bash
git add src/main src/test
git commit -m "Advertise the endpoints the REST front door actually serves"
```

---

### Task 3: Unit coverage for the exists routes

**Files:**
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java`

**Interfaces:**
- Consumes: the `head(String path)` helper this task adds.

- [ ] **Step 1: Write the tests**

Add a `head(String path)` helper beside `get(...)` (`.method("HEAD", HttpRequest.BodyPublishers.noBody())`), then:

```java
  @Test
  public void headOnExistingNamespaceReturns204() throws Exception {
    assertEquals(204, head("/v1/catalog1/namespaces/default").statusCode());
  }

  @Test
  public void headOnMissingNamespaceReturns404() throws Exception {
    assertEquals(404, head("/v1/catalog1/namespaces/no_such_ns_probe").statusCode());
  }

  @Test
  public void headOnExistingTableReturns204() throws Exception {
    assertEquals(204, head("/v1/catalog1/namespaces/default/tables/t1").statusCode());
  }

  @Test
  public void headOnMissingTableReturns404() throws Exception {
    assertEquals(404, head("/v1/catalog1/namespaces/default/tables/no_such_table_probe").statusCode());
  }

  @Test
  public void headOnTableUnderSecondPrefixReturns204() throws Exception {
    assertEquals(204, head("/v1/catalog2/namespaces/default/tables/t2").statusCode());
  }
```

Use the namespace and table names the fixture already seeds — read the test's `setUp` and the `RecordingThriftIface` seeding first and substitute the real names for `default`, `t1` and `t2`; seed an extra table only if the fixture has none under the second prefix.

- [ ] **Step 2: Run — these should pass immediately** (the routes are already live; this task is coverage, not a fix):

```bash
mvn -o -Dtest=IcebergRestEndpointIntegrationTest test
```

If any fails, that is a real defect: report it rather than adjusting the assertion to match.

- [ ] **Step 3: Full suite**

```bash
mvn -o test
```

Expected: `BUILD SUCCESS`, 0 failures, 0 skipped.

- [ ] **Step 4: Commit**

```bash
git add src/test
git commit -m "Cover the Iceberg REST exists routes with unit tests"
```

---

### Task 4: Smoke assertions, stand run, documentation

**Files:**
- Modify: `scripts/run-real-installation-smoke.sh` — three assertions inside `run_rest_smoke`, after the existing single-prefix checks:

```bash
  code="$(rest_request GET "/v1/${prefix}/namespaces/no_such_ns_smoke" "${body}")"
  [[ "${code}" == "404" ]] || fail "missing namespace expected HTTP 404, got ${code}: $(cat "${body}")"
  if grep -q '"stack":\["' "${body}"; then
    fail "error response leaks a server stack trace: $(cat "${body}")"
  fi

  code="$(curl -sS -o "${body}" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
    --data 'not json at all' \
    "${HMS_SMOKE_REST_URL}/v1/${prefix}/namespaces/${namespace}/tables/${iceberg_table}/metrics")"
  [[ "${code}" == "400" ]] || fail "unparseable body expected HTTP 400, got ${code}: $(cat "${body}")"

  code="$(rest_request GET "/v1/config" "${body}")"
  [[ "${code}" == "200" ]] || fail "GET /v1/config returned HTTP ${code}: $(cat "${body}")"
  grep -q '"endpoints"' "${body}" \
    || fail "config does not advertise an endpoint list: $(cat "${body}")"
```

  Guard the metrics assertion with `[[ -n "${iceberg_table}" ]]` so it is skipped when no Iceberg table is configured.
- Modify: `README.md`, `README.ru.md` — the Iceberg REST section: error responses carry code, type and message but no stack trace; `/v1/config` advertises the served endpoints and works in the `/v1/{prefix}/config` form too; an unparseable body answers 400.
- Modify: `CHANGELOG.md`, `CHANGELOG.ru.md` — a bullet under `## 2026-07-27` in `### Fixed` for the stack-trace leak and the 400, and one under `### Added` for the endpoint advertising.
- Modify after the run: `smoke-stand/TEST-MATRIX.md`, `smoke-stand/TEST-MATRIX.ru.md` — three new section-G rows (stack-free errors, 400 on unparseable body, config advertises endpoints) and a sentence in the 2026-07-27 revalidation-log entry.

- [ ] **Step 1: Implement the runner and doc changes; `bash -n scripts/run-real-installation-smoke.sh` passes.**
- [ ] **Step 2: Rebuild and restage the stand**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -DskipTests package
cd smoke-stand && ./prepare.sh && docker compose up -d --build
```

Poll `docker compose ps` until every container is healthy (up to ~8 minutes, `sleep 20` between polls).

- [ ] **Step 3: Run the REST scenario, then the full one**

```bash
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario rest
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario all
```

Expected: both end `completed successfully`.

- [ ] **Step 4: Capture the new config response verbatim** for the changelog and report:

```bash
curl -s http://localhost:19183/v1/config
curl -s http://localhost:19183/v1/apache/config
```

- [ ] **Step 5: Update TEST-MATRIX in both locales with what the run showed; commit everything.**

```bash
git add scripts smoke-stand README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md
git commit -m "Cover the hardened REST error and config behavior in the stand smoke"
```

---

## Self-Review

- **Spec coverage:** stack-free errors (T1); `400` for unparseable bodies (T1); `endpoints` advertising plus the prefixed config form and its `404` (T2); exists-route unit coverage (T3); smoke, matrix and bilingual docs (T4). The spec's "no other status may change" is enforced by running the full suite in T3 and both stand scenarios in T4. No gaps.
- **Placeholder scan:** none. The one conditional instruction (verify `Endpoint.V1_*` constants exist, use the real `Endpoint` rendering in assertions) names exactly what to check and what to do about it.
- **Type consistency:** `loadConfig()` keeps its no-arg signature returning `ConfigResponse` across T2 and its callers; `head(String)` is defined in T3 and used only there; `rest_request GET|POST` matches the helper the runner already defines.
