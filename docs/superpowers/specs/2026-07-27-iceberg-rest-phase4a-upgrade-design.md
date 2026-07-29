# Iceberg REST frontend, phase 4a: Iceberg upgrade — design

Date: 2026-07-27
Branch: `feature/iceberg-rest-fe-phase1`
Status: approved

## Goal

Move the REST front door off Iceberg `1.5.2` onto `1.9.2`. This sub-project is the
dependency and API-migration work only: everything that worked before must still work.
The read-path features the upgrade unlocks (exists routes, `endpoints` advertising) are
phase 4b.

Phase 4 was split in two because the upgrade carries dependency risk that deserves its
own acceptance gate: if it proves unworkable, it can be abandoned without losing 4b.

## Why 1.9.2

Verified against the published artifacts before choosing, across every candidate:

| version | exists routes | native views | `Route` | adapter entry point | jackson |
| --- | --- | --- | --- | --- | --- |
| 1.5.2 (today) | no | no | nested | `execute(...)`, public | 2.14 / 2.12 |
| 1.8.1 | yes | yes | nested | `handleRequest(route, vars, body, type)` | ~2.18 |
| **1.9.2** | **yes** | **yes** | **nested** | **`handleRequest(route, vars, body, type)`** | **2.18.3** |
| 1.10.2 | yes | yes | nested | same | 2.21.2 |
| 1.11.0 | yes | yes | top-level | `handleRequest` over `HTTPRequest` | 2.21.3 |

Exists routes (`NAMESPACE_EXISTS`, `TABLE_EXISTS`, `VIEW_EXISTS`) arrive in `1.8.x` and
are what phase 4b needs; `CatalogHandlers` in the main jar carries the matching
`namespaceExists` / `tableExists` / `viewExists`. Everything above `1.9.2` adds only
scan-planning routes this project does not want, while `1.10.2` and `1.11.0` cost a
wider Jackson jump and `1.11.0` additionally restructures the adapter into three
vendored files. `1.9.2` is therefore the newest release that buys every feature at the
mildest dependency and API cost.

Also verified for `1.9.2`:

- `HiveCatalog` extends `BaseMetastoreViewCatalog`, so view read paths become real
  instead of the current empty `204`.
- The private `clients` field survives with the same type, so the reflection inject in
  `RoutingHiveCatalog` still applies.
- Bytecode major version 55 (Java 11) — no new runtime requirement.
- No slf4j 2.x fluent-API call sites, so Iceberg runs on the project's slf4j `1.7.36`.
- `iceberg-hive-metastore` is published at `1.9.2`, and its pom declares no Hive
  dependency (upstream keeps Hive `compileOnly`), so Hive `3.1.3` keeps providing
  `IMetaStoreClient`.

## The Jackson problem

Measured on the real tree by temporarily setting `iceberg.version=1.9.2` and running
`mvn dependency:tree`:

| | jackson-core | jackson-databind |
| --- | --- | --- |
| today (1.5.2) | 2.14.2 | 2.12.0 (Hive) |
| with 1.9.2 | 2.18.3 (Iceberg) | 2.12.0 (Hive) |

Iceberg wins `jackson-core`, Hive wins `jackson-databind`, and the split widens from two
minor versions to six. Iceberg `1.9.2` is compiled against databind `2.18`; served `2.12`
it would break in `TableMetadataParser` — the exact path that reads `metadata.json`.

Resolution: pin `jackson-core` and `jackson-databind` to `2.18.3` through
`dependencyManagement`. This hands the newer Jackson to the Hive/Hadoop paths too, which
AGENTS.md flags as fragile — so "Hive still works" becomes an acceptance criterion
measured on the stand, not an assumption.

The rest of the measured tree is undramatic: `slf4j-api` stays at the project's `1.7.36`,
`avro` stays at Hive's `1.7.4`, `caffeine` stays at `2.9.3`, and `httpclient5` moves
`5.3.1` → `5.4.3`.

**Fallback.** If the stand shows Hive breakage that Jackson alignment cannot fix, step
down to `1.8.1` — the only lower rung that still carries exists routes and native views.
If that fails too, abandon the upgrade and take phase 4b on `1.5.2`, losing native views
and implementing exists checks by hand. Record the outcome in the changelog.

## Adapter API migration

The upgrade is not import-only: the `execute(HTTPMethod, path, queryParams, body,
responseType, headers, errorHandler)` overload that `IcebergRestService.dispatch`
currently calls is gone in every candidate version. `1.9.2` replaces it with the public
`handleRequest(Route route, Map<String, String> vars, Object body, Class<T> responseType)`.

Three consequences, all inside `IcebergRestService` and `IcebergHttpHandler`:

- The route and its path variables are already parsed by the handler
  (`Route.from(...)` returns `Pair<Route, Map<String, String>>`), so they are passed
  straight into `handleRequest`.
- `HTTPMethod` moved out of the adapter: the import becomes
  `org.apache.iceberg.rest.HTTPRequest.HTTPMethod`. `Route` itself stays nested in
  `RESTCatalogAdapter`, so that import is unchanged and only one file is vendored.
- `handleRequest` takes no error-handler callback; catalog exceptions propagate instead.
  The handler maps them with the adapter's public
  `RESTCatalogAdapter.configureResponseFromException(Exception, ErrorResponse.Builder)`,
  which fills in both the message and the response code, replacing today's
  captured-callback scheme.

## Scope

- `iceberg.version` `1.5.2` → `1.9.2`; Jackson pinned to `2.18.3` via
  `dependencyManagement`; an explicit `org.slf4j:slf4j-api` exclusion on the Iceberg
  dependencies so the transitive `2.0.17` can never win. Existing exclusions stay.
- Re-vendor `RESTCatalogAdapter` from `1.9.2` test sources — still one file, since
  `Route` remains nested. The vendoring header comment names the new version.
- Migrate `IcebergRestService.dispatch` and `IcebergHttpHandler` to `handleRequest` and
  exception-based error mapping, as described above.
- Extend `RoutingMetaStoreClient` if `1.9.2` read paths call `IMetaStoreClient` methods
  the proxy's dynamic proxy does not implement. The proxy throws
  `UnsupportedOperationException` with the method signature, so any gap surfaces as a
  clear test failure; new branches must apply the same name translation as the existing
  ones.

Out of scope: enabling exists routes, advertising `endpoints`, view smoke coverage,
pagination (`CatalogHandlers` still returns whole lists, and HMS does too).

## Expected behavior deltas

Strict neutrality is not achievable and is not claimed:

- View routes (`GET .../views`, `GET .../views/{name}`) start returning real data instead
  of the current empty `204`, because `HiveCatalog` is a `ViewCatalog` from `1.7` on.
- Exists routes become reachable in the adapter. Wiring them into the handler is 4b;
  until then they behave as they do today.
- `/v1/config` may carry the new optional `endpoints` / `idempotency-key-lifetime`
  fields. Populating `endpoints` is 4b.

Everything else — namespace and table listings, table load, multi-catalog prefixes, name
translation, metrics, SPNEGO — must be unchanged.

## Client compatibility

The REST front door is a wire protocol, so a client's own Iceberg version is independent
of ours; unlike the Thrift front door, one endpoint serves every client version. Two
properties make the upgrade safe for existing clients, and both are asserted rather than
assumed:

- **The proxy re-serializes table metadata rather than streaming the file.** Verified on
  the stand: the raw `metadata.json` in HDFS carries 18 top-level fields while the served
  response carries 21 (`refs`, `statistics`, `partition-statistics` are added by the
  parser). The emitted field set therefore follows our library version. Older clients
  tolerate this because Iceberg's JSON parsers read fields by name and ignore unknown
  ones.
- **Table format version comes from the data, not from our library.** A `format-version: 2`
  table stays v2 after the upgrade; the parser does not silently promote tables. What
  limits an old client is the format version of the tables in HMS, not the proxy.

Acceptance therefore includes: after the upgrade a v2 table still loads with
`"format-version": 2` and keeps the fields the v2 spec requires.

## Testing

- Full `mvn -o test` green.
- `mvn -o dependency:tree`: exactly one `org.slf4j:slf4j-api` (the project's `1.7.36`),
  no `log4j:log4j`, and matching `jackson-core` / `jackson-databind` versions.
- Fat jar builds with no new overlapping *classes* in the shade report.
- Stand, plain profile: `--scenario rest` and `--scenario all` green, plus the **SQL
  layer** through both HiveServer2 instances — that pass is the Jackson-regression
  detector for the Hive paths.
- A stand assertion that the served v2 table still reports `"format-version": 2`.
- Docs: README and CHANGELOG in both locales; the README note pinning `RoutingHiveCatalog`
  to Iceberg `1.5.2` must name `1.9.2` instead.
