# Iceberg REST Phase 3: Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** REST traffic visible in the proxy's Prometheus metrics per `docs/superpowers/specs/2026-07-27-iceberg-rest-phase3-observability-design.md`.

**Architecture:** Three new series in the hand-rolled `PrometheusMetrics`; `IcebergHttpHandler` records prefix/route/status/duration around `doHandle`; the listener info gauge is set on successful bind. No audit changes, no response-behavior changes.

**Tech Stack:** Java 17, JUnit 4. `JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19`, offline Maven (`mvn -o`). No new dependencies.

## Global Constraints

- Java 17; 2-space indent; explicit imports; no new dependencies.
- Metric label rules from the spec verbatim: `route` = lower-case adapter `Route` enum name or pseudo-routes `unknown_prefix` / `unknown_route` / `bad_request`; `prefix` = catalog prefix or `unknown`; `status` = numeric HTTP status written. Never a raw URL in a label.
- Metrics recording must never fail or alter a response; record after the response is written.
- Bilingual docs: every EN doc change lands with its RU counterpart in the same commit.
- English commit messages, no attribution footers. Commits pre-approved; do NOT push.

---

### Task 1: PrometheusMetrics REST series

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/observability/PrometheusMetrics.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/observability/PrometheusMetricsTest.java` (add cases; if the class does not exist, check for the existing metrics test class with `grep -rn 'class PrometheusMetrics' src/test` and extend that)

**Interfaces:**
- Produces: `void recordRestRequest(String prefix, String route, int status, double durationSeconds)`; `void setRestListenerInfo(String bindHost, int port)`.

- [ ] **Step 1: Write failing tests** — after `recordRestRequest("hdp", "load_table", 200, 0.05)` the `render()` output contains `hms_proxy_rest_requests_total{prefix="hdp",route="load_table",status="200"} 1` and a `hms_proxy_rest_request_duration_seconds` histogram series for those labels; after `setRestListenerInfo("0.0.0.0", 9183)` render contains `hms_proxy_rest_listener_info{bind_host="0.0.0.0",port="9183"} 1`. Follow the existing test class's assertion style (substring checks on `render()`).
- [ ] **Step 2: Run** `mvn -o -Dtest=<metrics test class> test` — expect compile failure.
- [ ] **Step 3: Implement** — declare the three series next to the existing ones, same builder/registration style, duration histogram reusing the same bucket array as `hms_proxy_request_duration_seconds`; `recordRestRequest` increments the counter with `String.valueOf(status)` and observes the histogram; `setRestListenerInfo` sets the gauge to 1.
- [ ] **Step 4: Run to green.**
- [ ] **Step 5: Commit** (include `docs/superpowers/specs/...phase3...md` and this plan file): `git commit -m "Add REST listener series to the Prometheus metrics"`.

---

### Task 2: Handler instrumentation and wiring

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RestCatalogServer.java` (`open(ProxyConfig, IcebergRestServices, PrometheusMetrics)`)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java` (pass `observability.metrics()`)
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestEndpointIntegrationTest.java` (+ update every `RestCatalogServer.open` call site: `grep -rn 'RestCatalogServer.open' src/`)

**Interfaces:**
- Consumes: `recordRestRequest` / `setRestListenerInfo` from Task 1.
- Produces: instrumented handler; `RestCatalogServer.open` requires non-null metrics.

- [ ] **Step 1: Add failing integration tests** — after `get("/v1/catalog1/namespaces")` (200), `get("/v1/nope/namespaces")` (404) and `get("/v1/config?warehouse=nope")` (400), the shared `PrometheusMetrics` instance's `render()` contains: a `route="list_namespaces",status="200"` series with `prefix="catalog1"`; a `route="unknown_prefix",prefix="unknown",status="404"` series; a `route="bad_request",prefix="unknown",status="400"` series; and `hms_proxy_rest_listener_info`.
- [ ] **Step 2: Implement** — handler constructor takes `PrometheusMetrics`; `handle()` wraps `doHandle` with `System.nanoTime()`; the handler records in `finally` using fields captured during dispatch: resolved prefix (or `unknown`), normalized route (lower-case `Route` enum name via the parsed route; `config` for the discovery path; pseudo-routes for refusals), and the status passed to the write helpers (track it where the response is written). `RestCatalogServer.open` requires metrics (`Objects.requireNonNull`) and calls `setRestListenerInfo(bindHost, boundPort)` after `server.start()`.
- [ ] **Step 3: Run the restcatalog suite** (`mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test`), then the FULL suite (`mvn -o test`) — 0 failures, 0 skipped.
- [ ] **Step 4: Commit**: `git commit -m "Record REST request metrics in the Iceberg HTTP handler"`.

---

### Task 3: Smoke step, stand run, docs

**Files:**
- Modify: `scripts/run-real-installation-smoke.sh` — at the end of `run_rest_smoke`, before the final log line:

```bash
  local metrics_url="${HMS_SMOKE_REST_METRICS_URL:-}"
  if [[ -n "${metrics_url}" ]]; then
    code="$(rest_request GET "" "${body}")" # placeholder removed below; use curl directly:
    curl -sS -o "${body}" "${metrics_url}" || fail "cannot fetch metrics from ${metrics_url}"
    grep -q 'hms_proxy_rest_requests_total{' "${body}" \
      || fail "metrics endpoint carries no hms_proxy_rest_requests_total series"
    grep -q 'hms_proxy_rest_listener_info{' "${body}" \
      || fail "metrics endpoint carries no hms_proxy_rest_listener_info series"
  fi
```

  (Use the curl call only — do not call `rest_request` for the metrics URL; drop the placeholder line. Document `HMS_SMOKE_REST_METRICS_URL` in usage next to the other REST vars.)
- Modify: `smoke-stand/env/simple.env` — `HMS_SMOKE_REST_METRICS_URL=http://localhost:19090/metrics`.
- Modify: `scripts/hms-real-installation-smoke.simple.env.example` — commented key + one-line comment.
- Modify: `README.md` + `README.ru.md` — extend the metrics table with the three new series; one sentence in the Iceberg REST section that the listener is covered by metrics. Same content both locales.
- Modify: `CHANGELOG.md` + `CHANGELOG.ru.md` — bullet under 2026-07-27 Added.
- Modify after the run: `smoke-stand/TEST-MATRIX.md` + `.ru.md` — row G17 (REST metrics visible on the management endpoint) + a revalidation-log sentence.

- [ ] **Step 1: Implement runner/env/docs; `bash -n` passes.**
- [ ] **Step 2: Rebuild + restage + restart the stand proxy** (`mvn -o -DskipTests package`, `cd smoke-stand && ./prepare.sh`, `docker compose up -d --build`), wait healthy.
- [ ] **Step 3: Run** `--scenario rest` then `--scenario all` — both `completed successfully`.
- [ ] **Step 4: Update TEST-MATRIX both locales; commit everything**: `git commit -m "Cover REST metrics in the stand smoke"`.

---

## Self-Review

- Spec coverage: three series (T1), label rules + wiring + non-null metrics (T2), never-fail recording (T2 finally-after-write), smoke + docs (T3). No audit/readiness changes anywhere — matches out-of-scope.
- Placeholders: the T3 snippet's stray `rest_request` line is explicitly instructed to be dropped; no TBDs.
- Type consistency: `recordRestRequest(String,String,int,double)` and `setRestListenerInfo(String,int)` used identically in T1/T2.
