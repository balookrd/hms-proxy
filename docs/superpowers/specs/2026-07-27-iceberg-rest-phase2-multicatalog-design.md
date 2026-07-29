# Iceberg REST frontend, phase 2: multi-catalog — design

Date: 2026-07-27
Branch: `feature/iceberg-rest-fe-phase1`
Status: approved

## Goal

Expose every configured proxy catalog through the Iceberg REST listener as its own
prefix (`/v1/<catalog>/...`), instead of serving only `routing.default-catalog`.
The endpoint stays read-only (writes are phase 5).

## Behavior

- **Prefixes.** Every catalog name from the proxy configuration is a valid prefix.
  An unknown prefix keeps returning 404 `NoSuchCatalogException`.
- **Default catalog keeps the federated view.** Under the default catalog's prefix
  the listing stays exactly what phase 1 shows: the proxy's external names — the
  default catalog's own databases plus `<catalog><separator><db>` names of the other
  catalogs. Phase 1 clients see no change.
- **Non-default catalogs get clean views.** Under `/v1/<catalog>/` only that
  catalog's databases are visible, under their internal names (`default`, not
  `apache__default`). Tables of other catalogs do not exist under that prefix by
  construction.
- **Discovery.** `GET /v1/config?warehouse=<catalog>` answers
  `overrides.prefix=<catalog>`. No `warehouse` parameter → the default catalog
  (phase 1 compatible). Unknown `warehouse` → 400 `BadRequestException`.
- **Reads only.** Write routes keep failing exactly as in phase 1
  (`UnsupportedOperationException` behind the adapter, non-2xx on the wire).

## Architecture

- `RestCatalogServer`, SPNEGO, and the `rest-catalog.*` config keys do not change.
  No new configuration keys: all configured catalogs are exposed — they are already
  visible through the Thrift federation, and the endpoint is read-only.
- A registry (prefix → per-catalog `IcebergRestService`) is built at startup from
  `config.catalogNames()`, so a broken configuration fails the start, not the first
  request. `IcebergHttpHandler` resolves the prefix segment against the registry;
  `/v1/config` gains the `warehouse` lookup.
- The default catalog's service is built on the existing untranslated
  `RoutingMetaStoreClient` — the federated view needs no name mapping.
- Each non-default catalog's service wraps the shared `ThriftHiveMetastore.Iface`
  in a **name-translating client layer**:
  - db arguments: internal → external (`default` → `apache__default`) before the call;
  - `get_all_databases`: filter to the catalog's own external names, strip the prefix;
  - `Database.name` / `Table.dbName` in results: external → internal.
  The proxy itself keeps seeing ordinary external names, so routing, exposure rules
  and catalog access modes apply unchanged, with no edits to the federation layer.

## Error handling

- Unknown prefix → 404 `NoSuchCatalogException` (unchanged).
- Unknown `warehouse` in `/v1/config` → 400 `BadRequestException`.
- A table of another catalog under a clean prefix → the ordinary 404
  `NoSuchTableException` path (the name simply does not resolve).

## Testing

- **Unit**: the translating layer — both mapping directions, listing filtration,
  result-name rewriting — against the existing `RecordingThriftIface`.
- **Integration**: extend `IcebergRestEndpointIntegrationTest` — clean view under a
  second prefix, federated view under the default prefix, `warehouse` discovery,
  400 on unknown warehouse, 404 on unknown prefix.
- **Smoke**: `--scenario rest` learns a second prefix
  (`HMS_SMOKE_REST_SECOND_PREFIX`), the stand registers an Iceberg table in the
  `apache` catalog too; run on the local stand after implementation.
- **Docs**: README/CHANGELOG in both locales; stand README and TEST-MATRIX after
  the stand run.

## Out of scope

- Writes, commits, view and transaction endpoints (phase 5 and later).
- Pagination and the remaining read endpoints of the REST spec.
- Kerberos changes: SPNEGO stays as shipped in phase 1.
