# Iceberg REST frontend, phase 5b: completing the write surface — design

Date: 2026-07-28
Branch: `feature/iceberg-rest-fe-phase1`
Status: approved

## Goal

Finish the write surface of the Iceberg REST front door: implement namespace DDL, and make
the view writes and multi-table transaction commits that phase 5a inadvertently enabled
official — advertised, tested and documented. The default-catalog restriction and its gate
are unchanged.

## What phase 5a actually shipped

The roadmap assumed views, namespaces and transactions were all unimplemented. Probing the
running stand under Kerberos shows otherwise:

| Route group | State after 5a |
| --- | --- |
| Namespace DDL | **Not implemented** — `HiveCatalog` needs `createDatabase`, `dropDatabase`, `alterDatabase`, none of which `RoutingMetaStoreClient` answers |
| View writes | **Working** — `POST .../views` returned 200 with a real `metadata-location`, the view listed, `DELETE` returned 204 |
| Multi-table transactions | **Working** — `POST .../transactions/commit` returned 204 and advanced the table's metadata from `00000-…` to `00001-…`, proving a real commit |

The cause is the same mechanism that handed phase 4a its exists routes for free: the handler
dispatches on the whole `Route` enum with no allowlist, and the client plumbing 5a added for
tables — `createTable`, `MetastoreUtil.alterTable`, the lock family — is exactly what
`HiveViewOperations` and the transaction commit path use. `HiveViewOperations` implements
`HiveOperationsBase` and takes the same `HiveLock`.

## The problem this creates

`/v1/config` advertises nine reads and five table writes. View writes and transaction
commits work and are **not** advertised, so a spec-compliant client reading `endpoints`
concludes they are unsupported and will not use them. That is the same discovery lie phase 4b
set out to remove, pointing the other way — under-advertising rather than over-advertising.

Worse, working functionality has no unit tests, no smoke coverage and no documentation. It
holds together only because nothing has disturbed it.

## Scope

1. **Namespace DDL.** Add `createDatabase`, `dropDatabase` and `alterDatabase` to
   `RoutingMetaStoreClient`, each applying the same name translation as the existing
   branches. This is the only genuinely missing implementation.
2. **Legitimise what works.** Advertise the view-write and transaction endpoints — and the
   namespace-DDL ones once they work — in the default catalog's `endpoints` list, alongside
   the table writes. Non-default catalogs keep advertising reads only.
3. **Cover it.** Unit tests for the new client branches and for the widened endpoint list;
   smoke coverage for a view write round trip, a namespace DDL round trip, and a transaction
   commit, each asserting the effect rather than only the status code.
4. **Document it**, both locales, including the honest note that views and transactions were
   already reachable before this phase made them official.

## Safety

Unchanged and non-negotiable: writes are permitted only where the target namespace resolves
to `routing.default-catalog`, enforced by `WriteRouteGate` on the resolved catalog rather
than the URL prefix. The gate already covers all thirteen write routes, including the ones
this phase legitimises, and phase 5a added the drift-guard test that fails when a new route
appears unclassified. Nothing here relaxes that.

Namespace DDL raises one question the table work did not: `CREATE_NAMESPACE` names a
namespace that does not exist yet, so "the catalog it resolves to" is decided purely by the
name's own prefix. A request to create `apache__foo` under the default prefix must be refused
exactly as a write into an existing federated namespace is — the gate already does this, and
the phase adds a test pinning it.

## Error handling

No new status codes. Namespace DDL failures surface through the same
`configureResponseFromException` mapping as every other route, with the stack stripped.

## Testing

- Unit: the three new client branches with translation, mirroring the existing write-method
  tests; the widened endpoint list per catalog.
- Smoke (`--scenario rest`, in the existing write-guarded block): a namespace create →
  properties update → drop round trip; a view create → list → drop round trip; a transaction
  commit asserting the table's `metadata-location` advances. Plus the gate negative for
  creating a federated namespace, which already exists and stays.
- Stand: plain profile for the round trips; the Kerberos profile is re-run for the write
  paths, since phase 5a's Kerberos pass is what exposed the missing `HiveConf` wiring.
- Docs: README and CHANGELOG both locales; TEST-MATRIX rows after the run.

## Out of scope

Relaxing the default-catalog restriction; giving non-default catalogs real writer isolation;
scan planning; OAuth.
