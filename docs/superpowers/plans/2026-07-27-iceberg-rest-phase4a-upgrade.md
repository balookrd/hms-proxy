# Iceberg REST Phase 4a: Upgrade to Iceberg 1.9.2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the REST front door from Iceberg `1.5.2` to `1.9.2` per `docs/superpowers/specs/2026-07-27-iceberg-rest-phase4a-upgrade-design.md`, keeping every existing behavior intact.

**Architecture:** Bump the version, pin Jackson to the version Iceberg expects, re-vendor `RESTCatalogAdapter` from `1.9.2`, and migrate our dispatch from the removed `execute(...)` overload to the public `handleRequest(...)` plus exception-based error mapping. No new REST features — exists routes and `endpoints` advertising are phase 4b.

**Tech Stack:** Java 17 (`JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19`), Maven, JUnit 4, Iceberg 1.9.2, the local docker-compose smoke stand.

## Global Constraints

- Java 17; 2-space indent; explicit imports; no new dependencies beyond the Iceberg version move and the Jackson pin.
- Target version `1.9.2`. Fallback if the stand shows unfixable Hive breakage: `1.8.1`; if that also fails, abandon the upgrade and report back. Record the outcome in the changelog.
- Jackson: pin `jackson-core` and `jackson-databind` to `2.18.3` in `dependencyManagement`.
- `org.slf4j:slf4j-api` must resolve to exactly one version, the project's `1.7.36`; no `log4j:log4j` anywhere in the tree.
- Behavior must be unchanged except the deltas the spec allows: view routes start returning real data, and `/v1/config` may carry new optional fields.
- Bilingual docs: every EN doc change lands with its RU counterpart in the same commit.
- English commit messages, no attribution footers. Commits are pre-approved; do NOT push.
- Offline Maven (`mvn -o`) is the norm, but the first build after the version bump needs the network to fetch `1.9.2` — use plain `mvn` for that one fetch, then `mvn -o` afterwards.

---

### Task 1: Version bump, Jackson pin, re-vendored adapter, dispatch migration

This is one task on purpose: the tree does not compile between the version bump and the dispatch migration, so there is no intermediate state a reviewer could accept.

**Files:**
- Modify: `pom.xml` (`iceberg.version`, `dependencyManagement`, slf4j exclusions on the three Iceberg dependencies)
- Modify: `src/main/java/org/apache/iceberg/rest/RESTCatalogAdapter.java` (replace wholesale with the `1.9.2` source)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergRestService.java` (dispatch signature)
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java` (imports, route dispatch, error mapping)
- Possibly modify: `src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/RoutingMetaStoreClient.java` (only if a read path needs a method the proxy lacks)

**Interfaces:**
- Produces: `IcebergRestService.dispatch(Route route, Map<String, String> vars, Object body, Class<T> responseType)` returning `T`, throwing catalog exceptions instead of reporting through a callback.

- [ ] **Step 1: Fetch the 1.9.2 adapter source**

The vendored file must come from the matching upstream tag, not be hand-edited:

```bash
curl -sS -o /tmp/RESTCatalogAdapter-1.9.2.java \
  https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.9.2/core/src/test/java/org/apache/iceberg/rest/RESTCatalogAdapter.java
head -30 /tmp/RESTCatalogAdapter-1.9.2.java
```

Expected: the Apache licence header followed by `package org.apache.iceberg.rest;`. If the tag path 404s, list the tags with `curl -sS https://api.github.com/repos/apache/iceberg/tags | grep -o 'apache-iceberg-1\.9\.[0-9]*' | head` and use the exact `1.9.2` tag name.

- [ ] **Step 2: Install the file, preserving our vendoring header**

Copy the fetched source over `src/main/java/org/apache/iceberg/rest/RESTCatalogAdapter.java`, then re-add the two-comment header that currently sits between the licence and the `package` line, with the version updated:

```java
// Vendored verbatim from apache-iceberg-1.9.2 (core/src/test/java/org/apache/iceberg/rest).
// In 1.9.x this class lives in test sources of iceberg-core; the copy here lets the
// proxy use it without depending on the tests classifier. When bumping the Iceberg
// version, diff this file against the matching upstream tag.
```

- [ ] **Step 3: Update pom.xml**

Set the version property:

```xml
<iceberg.version>1.9.2</iceberg.version>
```

Add to the existing `<dependencyManagement><dependencies>` block (which currently pins only curator):

```xml
      <!-- Iceberg 1.9.2 is compiled against Jackson 2.18; Hive 3.1.3 drags databind 2.12.
           Without this pin the tree resolves core 2.18.3 next to databind 2.12.0 and
           TableMetadataParser - the path that reads metadata.json - breaks. -->
      <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-core</artifactId>
        <version>2.18.3</version>
      </dependency>
      <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.18.3</version>
      </dependency>
```

Add this exclusion inside the `<exclusions>` of **each** of the three Iceberg dependencies (`iceberg-core`, `iceberg-bundled-guava`, `iceberg-hive-metastore`); `iceberg-bundled-guava` has no `<exclusions>` element today, so add one:

```xml
        <exclusion>
          <groupId>org.slf4j</groupId>
          <artifactId>slf4j-api</artifactId>
        </exclusion>
```

- [ ] **Step 4: Migrate the dispatch layer**

In `IcebergRestService`, replace the `dispatch(...)` method with:

```java
  public <T extends RESTResponse> T dispatch(
      RESTCatalogAdapter.Route route,
      Map<String, String> vars,
      Object body,
      Class<T> responseType) {
    return adapter.handleRequest(route, vars, body, responseType);
  }
```

Adjust its imports: drop `HTTPMethod`, `ErrorResponse` and `Consumer` if they become unused; keep `RESTResponse`.

In `IcebergHttpHandler`:
- change the import `org.apache.iceberg.rest.RESTCatalogAdapter.HTTPMethod` to `org.apache.iceberg.rest.HTTPRequest.HTTPMethod` (the `Route` import stays as it is);
- replace the `capturedError` callback scheme around the dispatch call with a catch that maps the exception:

```java
      try {
        response = service.dispatch(route, routeAndVars.second(), body, effectiveResponseType);
      } catch (RuntimeException e) {
        ErrorResponse.Builder builder = ErrorResponse.builder();
        RESTCatalogAdapter.configureResponseFromException(e, builder);
        writeErrorResponse(exchange, outcome, builder.build());
        return;
      }
```

Keep the existing `writeErrorResponse` helper and the per-request `RequestOutcome` bookkeeping exactly as they are — metrics recording must keep working unchanged. Import `org.apache.iceberg.rest.RESTCatalogAdapter` for the static helper.

- [ ] **Step 5: Build (online, one time) and fix what the compiler reports**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -DskipTests compile
```

Expected: `BUILD SUCCESS`. Compilation errors here are the migration surface — resolve them inside the files listed above; do not widen the change.

- [ ] **Step 6: Run the restcatalog suite**

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -Dtest='io/github/mmalykhin/hmsproxy/restcatalog/*' test
```

Expected: all green. A failure naming `UnsupportedOperationException: HMS proxy REST gateway does not support IMetaStoreClient.<method>` means `1.9.2` read paths call a method the proxy lacks: add that branch to `RoutingMetaStoreClient`'s switch, applying the same name translation the neighbouring branches use, and re-run.

- [ ] **Step 7: Run the full suite**

```bash
mvn -o test
```

Expected: `BUILD SUCCESS`, 0 failures, 0 skipped.

- [ ] **Step 8: Verify the dependency tree**

```bash
mvn -o dependency:tree > /tmp/tree-4a.txt 2>&1
grep -E 'slf4j-api|log4j:log4j|jackson-core:jar|jackson-databind:jar' /tmp/tree-4a.txt
```

Expected: exactly one `org.slf4j:slf4j-api:jar:1.7.36`, no `log4j:log4j`, and `jackson-core` and `jackson-databind` both at `2.18.3`.

- [ ] **Step 9: Build the fat jar and check the shade report**

```bash
mvn -o -DskipTests package 2>&1 | grep -E 'overlapping classes|BUILD'
```

Expected: `BUILD SUCCESS` and no new `overlapping classes` warning that names an `iceberg-` jar (resource overlaps such as LICENSE/NOTICE are pre-existing and fine).

- [ ] **Step 10: Commit** (include the spec and this plan, both currently untracked)

```bash
git add pom.xml src/main docs/superpowers
git commit -m "Move the Iceberg REST front door to Iceberg 1.9.2"
```

---

### Task 2: Stand validation

**Files:**
- No source changes expected. If the stand exposes a defect, fix it in the files from Task 1 and note it in the report.

- [ ] **Step 1: Rebuild, restage and restart the stand**

The stand containers are running with the previous jar.

```bash
export JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19
mvn -o -DskipTests package
cd smoke-stand && ./prepare.sh && docker compose up -d --build
```

Then poll until healthy (up to ~8 minutes), e.g. `docker compose ps` in a loop with `sleep 20`. Never run `docker compose down` — the stand's HDFS data and registered Iceberg tables live in its volumes.

- [ ] **Step 2: Run the REST scenario**

```bash
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario rest
```

Expected: `scenario 'rest' completed successfully`.

- [ ] **Step 3: Run the full scenario**

```bash
JAVA_HOME=~/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario all
```

Expected: `scenario 'all' completed successfully`.

- [ ] **Step 4: Run the SQL layer — the Jackson-regression detector**

This is the acceptance criterion for "Hive still works" after the Jackson pin. It runs from inside the HDP HiveServer2 container:

```bash
docker cp scripts/run-real-installation-smoke.sh stand-hs2-hdp:/opt/hs2-hdp/run-smoke.sh
docker exec stand-hs2-hdp /opt/hs2-hdp/run-smoke.sh --env-file /opt/hs2-hdp/sql.env --scenario sql
```

Expected: `scenario 'sql' completed successfully`. If `stand-hs2-hdp` is not running, start it with `cd smoke-stand && docker compose --profile hdp up -d` and wait for health (it runs emulated and is slow to start). If `/opt/hs2-hdp/sql.env` is missing, recreate it from `smoke-stand/env/simple.env` plus the SQL keys documented in that file's trailing comment block, and copy in the fat jar as `/opt/hs2-hdp/hms-proxy.jar` with `HMS_SMOKE_FAT_JAR` pointing at it.

A failure here that names Jackson (`NoSuchMethodError`, `ClassNotFoundException` under `com.fasterxml.jackson`) is the fallback trigger: report it and stop rather than improvising.

- [ ] **Step 5: Verify client-visible behavior held**

```bash
curl -s http://localhost:19183/v1/hdp/namespaces/default/tables/smoke_iceberg_tbl \
  | python3 -c "import sys,json; m=json.load(sys.stdin)['metadata']; print('format-version:', m['format-version']); print('fields:', len(m))"
curl -s http://localhost:19183/v1/hdp/namespaces | head -c 200; echo
curl -s http://localhost:19183/v1/apache/namespaces | head -c 200; echo
curl -s 'http://localhost:19183/v1/config?warehouse=apache'; echo
curl -s http://localhost:19090/metrics | grep -c 'hms_proxy_rest_requests_total{'
```

Expected: `format-version: 2` (the upgrade must not promote the table); the federated view under `hdp` still lists `default` and `apache__default`; the clean view under `apache` still lists only `default`; discovery still advertises `prefix":"apache"`; the metrics grep returns a non-zero count.

- [ ] **Step 6: Record the view-route delta**

```bash
curl -s -w '\n[%{http_code}]\n' http://localhost:19183/v1/hdp/namespaces/default/views
```

Before the upgrade this answered `204` with an empty body. Note the new status and body verbatim in the task report — it is the spec's expected delta and feeds the changelog wording in Task 3.

- [ ] **Step 7: Commit only if something needed fixing**

If Steps 1-6 required no source change, there is nothing to commit; say so in the report.

---

### Task 3: Documentation

**Files:**
- Modify: `README.md`, `README.ru.md` — the Iceberg REST section's note that pins `RoutingHiveCatalog` to Iceberg `1.5.2` (find it with `grep -n '1\.5\.2' README.md`) must name `1.9.2`, and the sentence about running `RoutingHiveCatalogTest` after a bump stays.
- Modify: `CHANGELOG.md`, `CHANGELOG.ru.md` — a bullet under `## 2026-07-27` / `### Changed` (create the subsection if the date section has none).
- Modify: `smoke-stand/TEST-MATRIX.md`, `smoke-stand/TEST-MATRIX.ru.md` — one sentence appended to the `2026-07-27` revalidation-log entry recording that sections A-D and G were re-run on the upgraded jar.

- [ ] **Step 1: Write the changelog bullet**

It must state: the REST front door moved from Iceberg `1.5.2` to `1.9.2`; Jackson is pinned to `2.18.3` because Iceberg expects `2.18` while Hive `3.1.3` brings `2.12`; view routes now return real data instead of an empty `204` because `HiveCatalog` became a `ViewCatalog`; the dispatch layer moved to `handleRequest` with exception-based error mapping; client compatibility is unaffected because the REST endpoint is a wire protocol and a v2 table still loads as v2. Mirror it in Russian, keeping terms like `prefix`, `route`, `front door` in Latin script.

- [ ] **Step 2: Update the READMEs and TEST-MATRIX files, both locales.**

- [ ] **Step 3: Commit**

```bash
git add README.md README.ru.md CHANGELOG.md CHANGELOG.ru.md smoke-stand/TEST-MATRIX.md smoke-stand/TEST-MATRIX.ru.md
git commit -m "Document the Iceberg 1.9.2 upgrade"
```

---

## Self-Review

- **Spec coverage:** version bump + Jackson pin + slf4j exclusion (T1 S3); re-vendored adapter (T1 S1-S2); adapter API migration incl. `configureResponseFromException` (T1 S4); `RoutingMetaStoreClient` gap handling (T1 S6); dependency-tree and shade acceptance (T1 S8-S9); stand + SQL-layer Jackson detector (T2 S2-S4); v2-format client-compatibility assertion (T2 S5); view-route delta recorded (T2 S6); docs both locales incl. the README `1.5.2` pin (T3). Fallback trigger is stated in the Global Constraints and at T2 S4. No gaps.
- **Placeholder scan:** none — every step carries the exact command or code, including the conditional `RoutingMetaStoreClient` branch instructions.
- **Type consistency:** `dispatch(Route, Map<String, String>, Object, Class<T>)` is declared in T1's Interfaces block and used with exactly that shape in T1 S4; `RESTCatalogAdapter.Route` stays the nested type throughout; `configureResponseFromException(Exception, ErrorResponse.Builder)` matches the verified upstream signature.
