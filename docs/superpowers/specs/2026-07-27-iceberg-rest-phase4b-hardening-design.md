# Iceberg REST frontend, phase 4b: read-path hardening — design

Date: 2026-07-27
Branch: `feature/iceberg-rest-fe-phase1`
Status: approved

## Goal

Close the wire-behavior defects the REST front door still carries after the Iceberg
`1.9.2` upgrade, and make the endpoint honest about what it serves. No new catalog
functionality: the endpoint stays read-only, writes remain phase 5.

## Why this is smaller than originally scoped

Phase 4a was expected to leave HEAD exists routes, view support and pagination for this
phase. The upgrade delivered all three:

- Exists routes went live because `IcebergHttpHandler` forwards any route
  `Route.from(...)` resolves without an allowlist. Verified on the stand: existing
  namespace and table answer `204`, missing ones `404`, a plain Hive table `404`, and a
  table under a non-default prefix `204`.
- `HiveCatalog` became a `ViewCatalog`, so `LIST_VIEWS` returns a real listing and
  `LOAD_VIEW` a real `404 NoSuchViewException`.
- Pagination works, once 4a fixed the `pageSize`-without-`pageToken` crash.

What remains is hardening.

## 1. Stack traces out of error responses

`RESTCatalogAdapter.configureResponseFromException` always calls `withStackTrace(exc)`,
so every adapter-mapped error hands the client the full server stack — internal package
structure, file names and line numbers — on a listener that may be unauthenticated. This
predates the upgrade; `1.5.2` behaved identically.

The exception-to-code table (`EXCEPTION_ERROR_CODES`) is private to the adapter, so
writing our own mapping would duplicate it and drift on the next upgrade. Instead the
upstream helper stays the source of truth for code, type and message, and the stack is
overwritten with an empty list before `build()`. `message` and `type` remain: they are
useful to clients and disclose nothing.

## 2. Malformed request body answers 400

`readBody` deserializes the request body outside any targeted catch, so a body that fails
to parse reaches the handler's catch-all and becomes `500`. Measured: a `ReportMetricsRequest`
missing `table-name`, and plain non-JSON, both return `500` today.

Deserialization failures become `400 BadRequestException`. This covers every route that
takes a body, not just metrics.

Note: a *valid* metrics report already answers `204` — the no-op path works and needs no
change.

## 3. `/v1/config` advertises the endpoints actually served

Iceberg `1.9.2` added `endpoints` to `ConfigResponse`. A server that stays silent tells a
modern client nothing, and the client assumes the full endpoint set including writes,
then discovers the refusals one request at a time. The response will list exactly what
this front door serves, built from the `Endpoint.V1_*` constants: list and load namespace
plus namespace-exists, list and load table plus table-exists, list and load view plus
view-exists. Older clients ignore the field.

`/v1/{prefix}/config` currently falls through to the adapter's own `CONFIG` case, which
advertises every route the adapter knows — writes included — and omits the `prefix`
override. It is routed through the same handler as `/v1/config`, with one difference that
follows from the URL: the path segment names the catalog, so the response carries
`overrides.prefix` for that catalog, exactly as `/v1/config?warehouse=<catalog>` does. An
unknown catalog in the path stays a `404`, matching every other prefixed route.

## 4. Unit coverage for the exists routes

The exists routes are covered only by stand row G18. Focused tests join the restcatalog
package: existing namespace and table `204`, missing `404`, and the same under a
non-default prefix, so a future upgrade cannot silently drop them.

## Error handling

No route gains or loses a status other than the two deliberate changes: adapter-mapped
errors keep their code but lose the `stack` field, and unparseable bodies move from `500`
to `400`.

## Testing

- Unit: stack-free error responses; `400` on an unparseable body; `/v1/config` and
  `/v1/{prefix}/config` both carrying `endpoints` and the `prefix` override; the exists
  routes.
- Smoke (`--scenario rest`): an error response carries no `stack`, an unparseable body
  answers `400`, and `/v1/config` advertises `endpoints`.
- Stand matrix: new section-G rows for the three smoke assertions, recorded after the run.
- Docs: README and CHANGELOG in both locales.

## Out of scope

Writes, commits and transactions (phase 5); OAuth; scan planning; the pre-existing
`/v1/{prefix}/oauth/tokens` route inherited from the vendored adapter.
