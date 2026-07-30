# Iceberg REST purge policy

Design for putting an explicit, configurable boundary around the only proxy call that irreversibly
deletes data: `DELETE /v1/{prefix}/namespaces/{ns}/tables/{tbl}?purgeRequested=true`.

## Problem

The REST front door serves `purgeRequested=true` as a real purge, the way Iceberg's own REST
catalog does: `CatalogHandlers.purgeTable` calls `catalog.dropTable(ident, true)`, which drops the
table in the metastore and then walks the table's manifests deleting its data and metadata files.
That deletion runs inside the proxy's JVM, under the proxy's own credentials (under Kerberos, the
outbound keytab), synchronously, before the `204`.

Unlike the Thrift path's external-table purge (`FileSystemExternalTableDropPurger`) it has neither
an allowlist of permitted prefixes nor a `BEST_EFFORT`/`DISABLED` switch. The only boundary is
`WriteRouteGate`, which answers a different question - "does this namespace resolve to the default
catalog?" - and says nothing about *which paths* may be deleted. Meanwhile the REST listener can
be, and on the stand is, deployed without authentication.

Two properties make this worse than "an unauthenticated client can delete its own tables":

1. **The file list comes from the client.** In the REST protocol the server never writes manifests;
   the client writes data files, manifest files and the manifest list itself, and the commit
   carries only metadata updates pointing at them. A client can therefore commit a snapshot whose
   manifest references arbitrary paths and then ask for a purge, and the proxy will delete those
   paths with its own credentials. This is an unauthenticated arbitrary-delete primitive, not a
   hypothetical one.
2. **A table's data need not live under its `LOCATION`.** `write.data.path` and `register_table`
   both put referenced files outside the table directory, so a check on the table location alone
   cannot bound what a purge touches.

## Decision

**Add a dedicated switch, `rest-catalog.purge.mode`, with its own allowlist key, defaulting to
today's behaviour.** The alternatives were weighed as follows.

**(a) Leave it as is.** The honest argument: the client asked through the Iceberg REST spec's own
parameter, and Iceberg catalogs behave this way. But a real Iceberg REST catalog sits behind
authentication and hands the deletion to a FileIO with credentials scoped to that table's storage.
Here the deletion runs with the proxy's cluster-wide credentials, the request may be
unauthenticated, and - per point 1 above - the set of deleted paths is client-controlled. Keeping
this as the only available behaviour means shipping an arbitrary-delete primitive with no way for
an operator to turn it off. Rejected as the *only* behaviour; kept as the **default**, because it
is what current deployments, the test suite and the stand rely on.

**(b) Reuse `federation.external-table-drop-purge.mode` plus
`catalog.<name>.conf.hms.proxy.external-table-drop-purge.allowed-prefixes`.** Attractive as "one
place that says what may be deleted", but the two lists describe different trees: the Thrift list
covers external tables (on the stand, `hdfs://namenode:8020/external/`) while Iceberg tables live
in the warehouse. Reusing it means enabling the mode silently breaks purge where it works today,
and repairing that means widening a list that also governs Thrift-side deletion - each key's blast
radius grows to cover the other path. `federation.external-table-drop-purge.mode` also gates a
different mechanism (recursive delete of a `LOCATION`, Apache `3.1.3` backends only, asynchronous,
best-effort), so a shared mode value would have to mean two different things. Rejected.

**(c) A dedicated `rest-catalog.purge.mode` with `rest-catalog.purge.allowed-prefixes`.**
Chosen. The two paths stay independently configurable, the new keys sit with the rest of the
`rest-catalog.*` surface they belong to, and the default preserves existing behaviour exactly.
The allowlist is global rather than per-catalog because a purge only ever reaches the default
catalog - `WriteRouteGate` refuses `DROP_TABLE` anywhere else - so a per-catalog dimension would
be configuration that can never take effect.

### Modes

| Mode | Behaviour |
| --- | --- |
| `ALLOW` (default) | Exactly today's path. No policy object is consulted, no FileIO is wrapped, no extra call is made. |
| `ALLOWLIST` | Purge proceeds only within `rest-catalog.purge.allowed-prefixes`, enforced twice (below). |
| `REFUSE` | `purgeRequested=true` is refused with `403`; the table is not dropped and no file is touched. `DELETE` without the parameter is unaffected. |

### Refusal semantics

A purge the policy does not permit answers `403` (`ForbiddenException`) **before the table is
dropped**, matching `WriteRouteGate`: the client learns what happened and can retry without
`purgeRequested`. The rejected alternative was degrading to a plain drop and answering `204`,
the way the Thrift path's `BEST_EFFORT` leaves files behind. That would quietly do something
other than what was asked and normalise orphaned files; the project's rule is explicit safe
failures. `DROP TABLE ... PURGE` from Spark fails loudly under `REFUSE`, which is the point.

### Two lines of enforcement in `ALLOWLIST`

**Pre-flight.** Before anything is dropped, the table's `TableMetadata.location()` and
`metadataFileLocation()` are qualified against the catalog's Hadoop `Configuration` and matched
against the allowlist. Either outside it → `403`, nothing dropped, nothing deleted. This is the
normal-operations check: it catches a table that simply lives in the wrong tree, and it answers
loudly.

**Per-path.** The deletion itself runs through a FileIO decorator that qualifies and checks every
path Iceberg asks it to delete - data files, delete files, manifests, manifest lists, metadata
files. A path outside the allowlist is **not deleted** and is logged at WARN. This is the line
that actually closes point 1 of the problem: a manifest referencing another tenant's files makes
the purge skip them rather than delete them. Pre-flight alone cannot do this, because those paths
are only discovered while walking the manifests, and by then the table is already dropped -
throwing there would leave a half-purged table and a `500`.

The decorator constrains only the delete methods; everything else delegates unchanged, so write
paths (`newOutputFile`, `newInputFile`) behave identically. In `ALLOW` the decorator is never
constructed, so the default path keeps today's `HadoopFileIO`, including any bulk-delete
capability it advertises.

## Configuration

Two keys, parsed strictly like the rest of the configuration surface - `ConfigParsing.parseEnum`
for the mode (case-insensitive, message listing the valid constants, no silent fallback):

| Key | Default | Meaning |
| --- | --- | --- |
| `rest-catalog.purge.mode` | `ALLOW` | `ALLOW` / `ALLOWLIST` / `REFUSE` |
| `rest-catalog.purge.allowed-prefixes` | *(empty)* | Comma-separated qualified path prefixes a purge may delete under; only meaningful with `ALLOWLIST` |

Combinations that cancel each other out fail at startup, per the project's parsing rule:

- `ALLOWLIST` with no non-blank prefix - an allowlist that permits nothing would make every purge
  a `403`, which is `REFUSE` spelled confusingly.
- A non-empty `allowed-prefixes` with `ALLOW` or `REFUSE` - the operator wrote a boundary that
  would never be applied.

Both checks run regardless of whether the REST listener is enabled: they are pure configuration
contradictions, and catching them only when the listener happens to be on would let a typo hide
until the listener is turned on in production.

Prefix matching reuses the Thrift path's semantics exactly: an exact match, or a match on the
prefix with a `/` boundary appended, so `hdfs://ns/warehouse/db` never matches
`hdfs://ns/warehouse/dbx`. That logic currently lives privately in
`FileSystemExternalTableDropPurger`; it moves to `util/PathPrefixAllowlist` and both callers use
it, so the two paths cannot drift apart in how they read a prefix.

## Components

- `config/restcatalog/RestCatalogPurgeMode.java` - the enum.
- `RestCatalogConfig` / `RestCatalogConfigParser` - two new fields, the parse and the two
  contradiction checks.
- `restcatalog/IcebergPurgePolicy.java` - the single place that answers "may this purge run?"
  (`refusalFor(TableMetadata)`) and "may this path be deleted?" (`guard(FileIO)`). Deliberately
  separate from `WriteRouteGate`, which answers the different question of catalog ownership; both
  facts get an `AGENTS.md` note so no third class grows its own copy.
- `restcatalog/PrefixGuardedFileIO.java` - the delete-only decorator.
- `RoutingHiveCatalog.dropTable(identifier, purge)` - the single override. `ALLOW` and
  `purge == false` delegate straight to `super`. Otherwise the policy is consulted, and on
  approval the catalog performs the same two steps Iceberg does, in the same order:
  `super.dropTable(identifier, false)` for the metastore drop, then
  `CatalogUtil.dropTableData(guardedIo, lastMetadata)` for the files.

`ForbiddenException` needs no new mapping: the vendored `RESTCatalogAdapter` already maps it to
`403`, which is how `WriteRouteGate` refusals reach the client.

## Failure handling

- A table whose metadata cannot be read (`ops.current()` is null - the table exists in the
  metastore but its pointer is gone) is dropped without a purge, as Iceberg already does; there
  is nothing to check and nothing to delete.
- A refused purge is logged at WARN as well as answered with the `403`: it is the only request the
  proxy answers that would have destroyed data, so the refusal belongs in the operator's log and
  not only in the client's response.
- A skipped path in `ALLOWLIST` never fails the request: the purge continues and the response is
  `204`. The WARN line names the table and the path.
- A delete that fails for an allowed path keeps Iceberg's own behaviour (logged, purge continues).

## Tests

The three existing purge tests in `IcebergRestEndpointIntegrationTest`
(`dropTableWithPurgeDeletesDataFiles`, `dropTableWithoutPurgeKeepsDataFiles`,
`dropTableWithPurgeUnderFederatedNamespaceDeletesNothing`) and
`dropTableWithPurgeReadsManifestsOfATableAskingForSnappy` stay untouched and keep passing on the
default `ALLOW` - that is the compatibility assertion for the default.

New endpoint tests, each starting a listener with its own mode:

- `REFUSE`: a purge answers `403`, the data file survives, and the table is still there
  (no `drop_table` on the fake delegate); a `DELETE` without `purgeRequested` on the same listener
  still succeeds.
- `ALLOWLIST`, table inside the list: purge deletes the data files, as `ALLOW` does.
- `ALLOWLIST`, table location outside the list: `403`, data file survives, no `drop_table`.
- `ALLOWLIST`, table inside the list but with a data file committed **outside** it: the table is
  dropped, the in-list files are gone, the out-of-list file survives. This is the manifest-borne
  case, and it is the test that would fail if the FileIO decorator were dropped in favour of
  pre-flight alone.

Config parsing tests next to the other parser tests: default is `ALLOW` with an empty list; an
unknown mode value is rejected with the valid constants named; `ALLOWLIST` without prefixes is
rejected; prefixes with `ALLOW` and with `REFUSE` are rejected; a lowercase mode value is accepted.

`PathPrefixAllowlist` gets its own test for the boundary cases (exact match, `/` boundary,
`dbx` non-match), and the existing `RoutingMetaStoreProxyDropPurgeTest` must keep passing
unchanged after the extraction.

## Stand verification

The point of the default is that nothing changes, so it is verified rather than asserted:
`smoke-stand/run-iceberg-interop-smoke.sh` is run with the default configuration, and its closing
purge check - no data, manifest or metadata files left - must still pass.

## Documentation

`README.md` with `README.ru.md` (the Iceberg REST purge section, which currently states there is
no allowlist and no switch, plus the configuration reference),
`src/main/resources/hms-proxy-example.properties`, `CHANGELOG.md` with `CHANGELOG.ru.md`, and an
`AGENTS.md` note in the `restcatalog` paragraph recording where the purge decision lives and that
the default is unchanged.

## Out of scope

- View drops. `DROP VIEW` has no purge parameter and deletes no data files.
- The Thrift path's external-table purge. Its keys, mode and asynchronous execution are unchanged.
- Authentication for the REST listener. A purge policy bounds what an authorised - or
  unauthenticated - client can destroy; it does not decide who the client is. SPNEGO already
  exists for deployments that need identity, and this design does not change when it applies.
