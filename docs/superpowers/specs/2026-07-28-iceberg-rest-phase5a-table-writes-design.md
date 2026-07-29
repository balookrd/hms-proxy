# Iceberg REST frontend, phase 5a: table writes — design

Date: 2026-07-28
Branch: `feature/iceberg-rest-fe-phase1`
Status: approved

## Goal

Let Iceberg REST clients create, commit to, rename, register and drop tables — in the
proxy's default catalog only. Namespace DDL, view writes and multi-table transactions are
later sub-projects.

## Why default catalog only

Iceberg commits atomically one of two ways. `NoLock` relies on the metastore performing a
compare-and-swap on the table's `metadata_location` through `alter_table`'s expected-parameter
check. Neither backend supports it: `expected_parameter` appears nowhere in
`HiveAlterHandler` of `hive-standalone-metastore-3.1.3.jar` or of the Hortonworks
`3.1.0.3.1.0.0-78` jar. So safe commits require `MetastoreLock`, which takes a real HMS lock
through `lock` / `checkLock` / `unlock` / `showLocks`.

Only the default catalog has real locks: it owns the TxnHandler. Every other catalog is served
by the synthetic lock shim, which — as the repo states in several places — "grants locks
without any conflict checking: a non-default catalog gives no writer isolation." An Iceberg
commit there would take an EXCLUSIVE lock that is always granted, believe it holds the table
exclusively, and race a concurrent writer into lost updates — silent metadata corruption.
Tolerable for reads, not for writes. Real isolation would need a distributed lock manager,
which is far beyond this phase.

## The gate, and the federated-name trap

Restricting writes to "the default catalog's prefix" is **not** sufficient. The default
prefix exposes the federated view, in which other catalogs' databases appear as
`<catalog><separator><db>`. A write to `/v1/hdp/namespaces/apache__default/tables/x` would
route by name into the `apache` catalog — straight into the shim, through the back door.

The gate therefore keys on **the catalog the namespace resolves to**, not on the URL prefix.
A write route whose target namespace resolves to any catalog other than
`routing.default-catalog` is refused before dispatch, with a message naming the reason
(no writer isolation outside the default catalog) rather than a bare refusal.

Resolution reuses the proxy's existing namespace logic rather than re-parsing
`<catalog><separator><db>` in the REST layer — a second parser would drift from the first and
this one is safety-critical.

## Client extension

`RoutingMetaStoreClient` implements read methods only today; every write throws
`UnsupportedOperationException`. The write path needs these added, each applying the same
name translation as the existing branches:

- `createTable` — used by `HiveOperationsBase` for both create and register.
- `dropTable`.
- `alter_table_with_environmentContext` — carries both the commit (updating
  `metadata_location`) and renames. Iceberg reaches it through
  `MetastoreUtil.alterTable`, which resolves the method reflectively via `DynMethods`,
  trying `alter_table_with_environmentContext` and then `alter_table`; our client is itself a
  `java.lang.reflect.Proxy` over `IMetaStoreClient`, so the lookup lands in the invocation
  handler as usual.
- `lock`, `checkLock`, `unlock`, `showLocks`, `heartbeat` — `MetastoreLock`'s commit locking.

## Guards inherited, not reimplemented

`CatalogAccessModeGuard` lives in the routing layer, shared by both front doors, so REST
writes inherit `READ_ONLY` refusal and `write-db-whitelist` enforcement automatically — a
REST client cannot bypass what a Thrift client faces. The transactional-DDL guard is checked
for false positives: Iceberg tables are not Hive-transactional, but the guard watches
`create_table_with_environment_context`, so the interaction is verified rather than assumed.

## Endpoint advertising becomes asymmetric

`/v1/config` under the default catalog's prefix advertises the write endpoints alongside the
reads; every other prefix keeps advertising reads only. Clients learn the asymmetry from
discovery instead of from refusals — the payoff of phase 4b's endpoint work.

## Error handling

A refused write returns the same shape as every other REST error: mapped status, `type`,
`message`, no stack. The gate refuses with `403 ForbiddenException` — the write route exists
and the caller is understood, the proxy declines to serve it — and the message names the
cause. Refusals from the inherited routing guards (`READ_ONLY`, whitelist) keep whatever
status the existing exception mapping already produces for them; this phase adds no new
status codes and changes none.

## Testing

- Unit: each new client branch with its name translation; the gate, including a write to a
  federated name under the default prefix and a write under a non-default prefix; the
  asymmetric endpoint list.
- Stand: a full round trip through REST — create a table, commit a snapshot, read it back,
  rename it, drop it — plus the two negatives above. The lock request Iceberg issues is
  inspected on the stand to confirm it reaches the real backend rather than the synthetic
  shim, since `LockHandler` has its own routing rules.
- Regression: `--scenario all` and the SQL layer through both HiveServer2 instances, because
  writes exercise the same lock path Hive uses.
- Docs: README and CHANGELOG in both locales; stand matrix rows after the run.

## Out of scope

Namespace DDL, view writes, multi-table transactions (`commitTransaction`), and any attempt
to give non-default catalogs real writer isolation.
