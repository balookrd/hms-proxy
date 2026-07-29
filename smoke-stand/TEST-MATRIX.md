# Smoke test matrix

What has actually been run on this stand, and what has not. Every ✅ below was observed on the
configuration described here — not inferred from a similar case passing.

**Configuration under test**

| Component | Version / role |
| --- | --- |
| Proxy | the fat jar from `target/`, three front doors: 9083 `APACHE_3_1_3`, 9084 `HORTONWORKS_3_1_0_3_1_0_78`, 9085 `APACHE_4_1_0` (the last one only where a config declares it) |
| `hms-hdp` | Hortonworks standalone metastore `3.1.0.3.1.0.0-78` — default catalog in the base config, owns ACID/txn state |
| `hms-apache` | Apache standalone metastore `3.1.3` — non-default catalog, and the default one under `.env.apache` |
| `hms-hive4` | Apache Hive standalone metastore `4.1.0` (official image) — default catalog under `.env.hive4`, compose profile `hive4` |
| `hs2` | Apache HiveServer2 `3.1.3` → Apache front door |
| `hs2-hdp` | vendor HDP HiveServer2 `3.1.0.3.1.0.0-78` → Hortonworks front door |
| `hs2-hive4` | Apache HiveServer2 `4.1.0` (official image, Tez local mode) → Hive 4 front door, compose profile `hive4fe` |
| Storage | **two** Apache Hadoop `3.1.3` clusters: `namenode` (catalogs `hdp` and `hive4`), `namenode-b` (catalog `apache`) |
| Auth | profile `plain` (no SASL) and profile `kerberos` (realm `SMOKE.LOCAL`, one realm for both clusters) |

Legend: ✅ passed · ❌ fails by design · — not run · n/a not applicable.

## A. Direct HMS smoke CLI — `--scenario all`

Driven by `scripts/run-real-installation-smoke.sh`; both profiles ended with
`scenario 'all' completed successfully`.

| # | Scenario | RPCs exercised through the proxy | plain | kerberos |
| --- | --- | --- | --- | --- |
| A1 | `txn` | `open_txns` → `allocate_table_write_ids` → `lock` → `check_lock` → `get_valid_write_ids` → `commit_txn` | ✅ | ✅ |
| A2 | Non-default catalog lock | `lock` SHARED_READ + DB + NO_TXN → `check_lock` → `heartbeat` → `unlock` → `abort_txn` | ✅ | ✅ |
| A3 | Partition lock | same, EXCLUSIVE + PARTITION + NO_TXN | ✅ | ✅ |
| A4 | Cross-catalog lock | one `lock` whose components name two catalogs (`--second-db`) | ✅ | ✅ |
| A5 | Notification, positive | `add_write_notification_log`, HDP front door → HDP backend | ✅ | ✅ |
| A6 | Notification, negative | same call against an Apache backend — must be refused | ✅ refused | ✅ refused |

## B. SQL through the **Apache** HiveServer2 (front door 9083)

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| B1 | Name federation — `show databases` → `default`, `apache__default` | ✅ | ✅ |
| B2 | Reads from both catalogs | ✅ | ✅ |
| B3 | DDL + `describe formatted` (location on the catalog's own HDFS) | ✅ | ✅ |
| B4 | Partitions: create → insert → `show partitions` → rename → count | ✅ | ✅ |
| B5 | External table: create → alter → insert → describe → drop | ✅ | ✅ |
| B6 | `INSERT` via local MapReduce | ✅ | ✅ |
| B7 | Views + cross-catalog view rewrite | ✅ | ✅ |
| B8 | Cross-catalog JOIN in one statement | ✅ | ✅ |
| B9 | JOIN across two databases of one catalog | ✅ | ✅ |
| B10 | Permanent UDF (`UDFReverse` → `yxorp`) | ✅ | ✅ |

## C. SQL through the vendor **HDP** HiveServer2 (front door 9084)

An HDP client cannot use the Apache listener — Thrift has no version negotiation — so this is the
only path that covers the Hortonworks front door with a real client.

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| C1 | Everything in section B | ✅ | ✅ |
| C2 | Transactional (ACID) tables: create → insert → count | ✅ | ✅ |
| C3 | `add_write_notification_log` sent by **Hive itself** after an ACID write, with real delta paths and checksums | ✅ | ✅ |
| C4 | `allocate_table_write_ids` / `get_valid_write_ids` through federation | ✅ | ✅ |
| C5 | Materialized view with rewrite enabled (`show materialized views` → `Yes`) | ✅ | ✅ |

## D. Two HDFS clusters

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| D1 | Tables of the two catalogs land on **different** namenodes | ✅ | ✅ |
| D2 | Physical check: no cross-cluster files | ✅ | ✅ |
| D3 | Writes into both clusters | ✅ | ✅ |
| D4 | Cross-cluster JOIN — one MapReduce job reads from both filesystems | ✅ | ✅ |
| D5 | Unqualified `LOCATION` rewritten onto the owning catalog's filesystem | ✅ | ✅ |
| D6 | `LOCATION` naming the *other* cluster rewritten onto the owning one | ✅ | ✅ |
| D7 | `DROP ... PURGE` deletes data on the catalog's own cluster | ✅ | ✅ |

## E. Individual checks

| # | Check | Result |
| --- | --- | --- |
| E1 | Transactional-DDL guard on `create_table_with_environment_context` | ✅ blocks a transactional table, lets a plain one through |
| E2 | Readiness probe does not disturb SASL (15 × `/readyz`, then a Kerberos smoke run) | ✅ |
| E3 | `hms_proxy_lock_request_split_total{catalog}` counts lock-request splits | ✅ |

## G. Iceberg REST catalog front door (host port 19183)

Driven by `--scenario rest` with curl from the host (plain) or from inside `stand-proxy`
(kerberos - the KDC and the `proxy` hostname only resolve in-network, and the container's curl
is GSS-capable). The loaded table is the hand-registered `smoke_iceberg_tbl` (see the stand
README). The Kerberos profile carried the listener disabled through phase 5a because SPNEGO
needed a GSS-capable curl inside the network; once that stopped being true the listener was
turned on there too (`rest-catalog.kerberos.principal=HTTP/proxy@SMOKE.LOCAL`, same keytab as
the Thrift front door). Since 2026-07-29 the kerberos column is driven by the smoke script
itself (`HMS_SMOKE_REST_CURL_OPTS=--negotiate -u :` in `env/kerberos.env`, after a kinit inside
the container), so both columns run the identical check set; only G18 (HEAD requests, never part
of the script) stays hand-driven.

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| G1 | `GET /v1/config` advertises `prefix=hdp` (the default catalog) | ✅ | ✅ |
| G2 | Namespace list and load (`default`) | ✅ | ✅ |
| G3 | Table listing shows the Iceberg table and hides plain Hive tables of the same database | ✅ | ✅ |
| G4 | Table load returns `metadata-location` and full metadata read from HDFS by the proxy itself | ✅ | ✅ |
| G5 | Unknown prefix → clean 404 `NoSuchCatalogException` | ✅ | ✅ |
| G6 | Unknown table → clean 404 | ✅ | ✅ |
| G7 | `DELETE` of a non-existent table answers a clean 404, not a silent 2xx | ✅ | ✅ |
| G8 | `GET /v1/config?warehouse=apache` advertises `prefix=apache` | ✅ | ✅ |
| G9 | Unknown warehouse (`GET /v1/config?warehouse=no_such_warehouse_smoke`) → clean 400 | ✅ | ✅ |
| G10 | Clean namespace view under the `apache` prefix lists `default` with no `apache__`-prefixed external names | ✅ | ✅ |
| G11 | Table load under the `apache` prefix (`smoke_iceberg_tbl_ap`, second HDFS cluster) returns `metadata-location` | ✅ | ✅ |
| G12 | Federated namespace `apache__default` stays visible under the default prefix | ✅ | ✅ |
| G13 | Listing and load of `smoke_iceberg_tbl_ap` through the federated `apache__default` name under the default prefix | ✅ | ✅ |
| G14 | A default-catalog table under the `apache` prefix → clean 404 | ✅ | ✅ |
| G15 | The external name `apache__default` used as a namespace under the `apache` prefix → clean 404 | ✅ | ✅ |
| G16 | The second catalog's plain Hive table (`smoke_read_ap`) stays invisible in the `apache` listing | ✅ | ✅ |
| G17 | REST metrics (`requests_total`, `listener_info`) visible on the management `/metrics` endpoint | ✅ | ✅ |
| G18 | `HEAD` on namespaces/tables answers `204` when present and `404` when absent, including under the non-default `apache` prefix and for a plain Hive table (`smoke_read_hdp`) | ✅ | n/a |
| G19 | Error response for a missing namespace carries the mapped `404`, `type` and `message` but no `"stack":[...]` server trace | ✅ | ✅ |
| G20 | An unparseable `POST .../metrics` body answers `400` (`BadRequestException`), not a `500` | ✅ | ✅ |
| G21 | `GET /v1/config` and `GET /v1/{prefix}/config` (both resolving to the default catalog) advertise the table-create and table-drop write routes, on top of the namespaces read route | ✅ | ✅ |
| G22 | `GET /v1/{second-prefix}/config` (non-default catalog) advertises the namespaces read route and carries no write route - proves discovery advertises the write/read asymmetry, not only the default side | ✅ | ✅ |
| G23 | Table write round trip on the default catalog: `POST` create (`200`), `GET` load (`metadata-location` present), `DELETE` drop (`2xx`) | ✅ | ✅ |
| G24 | Direct `POST` create under the non-default `apache` prefix refused with `403` (`ForbiddenException`) | ✅ | ✅ |
| G25 | `POST` create under the federated `apache__default` namespace, reached through the default prefix, refused with `403` - proves the write gate is enforced on the *resolved* catalog, not the request's own prefix | ✅ | ✅ |
| G26 | Real `POST` commit against the just-created table (`assert-table-uuid` requirement + `set-properties` update) answers `200` and the returned `metadata-location` differs from create's - proof a new metadata file was actually written through `HiveTableOperations.commit`, not a silent no-op | ✅ | ✅ |
| G27 | `POST /v1/{prefix}/tables/rename` answers `204`, and `GET` on the new name answers `200` | ✅ | ✅ |
| G28 | `POST /v1/{prefix}/transactions/commit` naming a table in the federated `apache__default` namespace refused with `403` | ✅ | ✅ |
| G29 | `POST /v1/{prefix}/namespaces` with a federated name (`apache__zzz_smoke`) refused with `403` | ✅ | ✅ |
| G30 | `POST /v1/{prefix}/tables/rename` with a federated *destination* namespace (source table still under its current name) refused with `403` - proves the destination side of the gate, not just the source | ✅ | ✅ |
| G31 | A request without `--negotiate` is rejected `401` with a `WWW-Authenticate: Negotiate` challenge and an empty body | n/a | ✅ |
| G32 | Namespace DDL round trip: `POST .../namespaces` create (`200`), `GET` load (`200`), `POST .../properties` update (`200`) with a follow-up `GET` confirming the property is actually present, `DELETE` (`204`), `GET` afterward (`404`) - genuinely new: `RoutingMetaStoreClient` did not implement `createDatabase`/`alterDatabase`/`dropDatabase` before this phase, so this is the first time namespace DDL reached a real metastore | ✅ | ✅ |
| G33 | View write round trip: `POST .../views` create answers `200` with a real `metadata-location`, `GET .../views` lists the new view, `POST .../views/{view}` update (`assert-view-uuid` requirement + `set-properties`) answers `200` with a follow-up `GET` confirming the property is actually present, `POST /v1/{prefix}/views/rename` answers `204` and the view loads back `200` under the new name while the old name answers `404` - the pair that proves the rename moved it rather than copying it, `DELETE` answers `204` | ✅ | ✅ |
| G34 | `POST /v1/{prefix}/transactions/commit` against a freshly created table: answers `204`, and the table's `metadata-location` afterward differs from create's - proof the multi-table commit path actually wrote a new metadata file, not a silent no-op | ✅ | ✅ |
| G35 | `POST .../views` (CREATE_VIEW, full valid view body) into the federated `apache__default` namespace refused with `403` - a minimal body instead gets `400` because it fails to parse before the gate is even consulted, so `400` here would mean the request is malformed, not that the gate let it through | ✅ | ✅ |
| G36 | `DELETE .../views/{view}` (DROP_VIEW) under the federated `apache__default` namespace refused with `403` | ✅ | ✅ |
| G37 | `DELETE /v1/{prefix}/namespaces/{ns}` (DROP_NAMESPACE) of the federated `apache__default` namespace refused with `403` | ✅ | ✅ |
| G38 | `POST .../properties` (UPDATE_NAMESPACE) of the federated `apache__default` namespace refused with `403` | ✅ | ✅ |
| G39 | REGISTER_TABLE round trip: create a table, `DELETE` it WITHOUT purge (the metadata file survives on HDFS, and a `GET` confirms `404`), `POST .../register` re-registers it from that metadata file (`200`, `metadata-location` present), `GET` loads it back (`200`), `DELETE` drops it - the last advertised write route without a positive proof | ✅ | ✅ |
| G40 | `POST .../tables/{table}` (UPDATE_TABLE, per-table commit) under the federated `apache__default` namespace refused with `403` - the table named need not exist, proving the gate answers before the lookup | ✅ | ✅ |
| G41 | `DELETE .../tables/{table}` (DROP_TABLE) under the federated `apache__default` namespace refused with `403` | ✅ | ✅ |
| G42 | `POST .../register` (REGISTER_TABLE, deliberately bogus `metadata-location`) under the federated `apache__default` namespace refused with `403` before anything tries to read the metadata file | ✅ | ✅ |
| G43 | `POST .../views/{view}` (UPDATE_VIEW) under the federated `apache__default` namespace refused with `403` | ✅ | ✅ |
| G44 | `POST /v1/{prefix}/views/rename` (RENAME_VIEW) with a federated *destination* namespace refused with `403` - the view-side counterpart of G30 | ✅ | ✅ |

With G39-G44 every one of the thirteen `WriteRouteGate` write routes now has both a positive
round trip (where the route is genuinely served) and a gate negative against a federated
namespace.

## H. Iceberg interop across every backend and front-door dialect

Driven by `smoke-stand/run-iceberg-interop-smoke.sh` (stand-local: every step is a docker exec
into the engine's own container). One Iceberg table crosses **all three front-door dialects plus
REST**, and the whole scenario is repeated with each of the stand's three metastores as the
default catalog — writes are gated to it, so the default catalog *is* the backend under test:

| Backend under test | Runtime profile | Storage | How |
| --- | --- | --- | --- |
| Hortonworks `3.1.0.3.1.0.0-78` (`hms-hdp`) | `HORTONWORKS_3_1_0_3_1_0_78` | `namenode` | default config, `--prefix hdp` |
| Apache `3.1.3` (`hms-apache`) | `APACHE_3_1_3` | `namenode-b` | `.env.apache`, `--prefix apache` |
| Apache Hive `4.1.0` (`hms-hive4`) | `APACHE_4_1_0` | `namenode` | `.env.hive4`, `--prefix hive4` |

The Iceberg REST writer (`smoke-stand/iceberg-rest-writer`, the client half of the REST protocol
curl cannot play) runs inside `stand-proxy`; both 3.1-dialect HiveServer2 instances carry
`iceberg-hive-runtime` 1.6.1 - the last release with a Hive 3 runtime, Iceberg 1.7 dropped it -
while the Hive 4 HiveServer2 (`hs2-hive4`, official image, Tez local mode) has Iceberg built in.

Every cell below was observed on all three backends unless the row says otherwise.

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| H1 | REST writes real data: the writer creates the table through the REST front door, writes Parquet files into HDFS and commits them as a snapshot through REST; its own scan reads the 2 rows back | ✅ | ✅ |
| H2 | The vendor HDP HiveServer2 (Hortonworks front door, 9084) reads the REST-written rows (`count=2`), appends one with `INSERT`, reads back `count=3` | ✅ | ✅ |
| H3 | The Apache HiveServer2 (Apache front door, 9083) appends one more and reads back `count=4` | ✅ | ✅ |
| H4 | The Hive 4 HiveServer2 (**Hive 4 front door, 9085** - the `APACHE_4_1_0` dialect, the only listener a Hive 4 client can use) reads everything the two 3.1-era engines wrote (`count=4`), appends its own row and reads back `count=5` | ✅ | ✅ |
| H5 | A REST-side full scan sees every SQL engine's commit (`rows=5`) - metadata and data round-trip through all four access paths | ✅ | ✅ |
| H6 | REST `DELETE` drops the table: `GET` answers `404`, `show tables` through SQL no longer lists it | ✅ | ✅ |
| H7 | Kerberos end to end: the writer authenticates REST with per-request SPNEGO tokens (custom Iceberg `AuthManager`) and writes HDFS as `smoke-user` from its keytab; all three HS2 passes run over SASL | n/a | ✅ |
| H8 | The same table is written through a 3.1-line backend on the second HDFS cluster (`--prefix apache`), which is what puts `APACHE_3_1_3` on the REST write path - the runtime profile no other layout can reach, since writes only go to the default catalog | ✅ | ✅ |

### H9-H12. Which front door creates the table (`--origin`)

The rows above have REST create the table and SQL take it over. `--origin` rotates that role, so
each front door in turn is the one that creates and writes first while the other three modify
what it made. Run on the `hive4` backend:

| # | Origin (creates + writes 2 rows) | Modified afterwards by | plain | kerberos |
| --- | --- | --- | --- | --- |
| H9 | REST front door (Iceberg catalog `createTable`) | HDP, Apache, Hive 4 → 5 rows | ✅ | ✅ |
| H10 | HDP HiveServer2 (`STORED BY 'HiveIcebergStorageHandler'`) | REST, Apache, Hive 4 → 5 rows | ✅ | ✅ |
| H11 | Apache HiveServer2 (same DDL) | REST, HDP, Hive 4 → 5 rows | ✅ | ✅ |
| H12 | Hive 4 HiveServer2 (`STORED BY ICEBERG`) | REST only → 3 rows; the 3.1-line engines **cannot** read it, see below | ✅ | ✅ |

Every participant reads the running total *before* its own append, so each hand-off across the
front-door boundary is proven rather than assumed, and a final round has all participants
confirm the same count.

**The one asymmetry, and it is Hive's, not the proxy's.** A Hive 4-created Iceberg table is
unreadable by the 3.1 line: `STORED BY ICEBERG` leaves the StorageDescriptor's `inputFormat` as
the abstract `org.apache.hadoop.mapred.FileInputFormat` (spelling the handler class out
explicitly leaves it `null` instead), because Hive 4 resolves the real format through the
storage handler at plan time. Hive 3.1 instantiates whatever the descriptor names and fails with
`Cannot create an instance of InputFormat class org.apache.hadoop.mapred.FileInputFormat`.
Tables written by Iceberg's own `HiveTableOperations` - the REST path, and the 3.1 storage
handler itself - carry the concrete `HiveIcebergInputFormat`, which is why every other origin is
readable everywhere, Hive 4 included. The proxy passes the descriptor through unchanged in both
directions; nothing here is a routing or compatibility decision it could make differently.

What building this surfaced (all found by the scenario, not by review):

- The `APACHE_4_1_0` backend runtime could not open a live Thrift connection at all - its client
  is generated against libthrift 0.16 while the fat jar carries 0.9.3, and the unit tests mocked
  the invocation layer. Fixed in the proxy: the Hive 4 isolated runtime now loads companion jars
  (`libthrift-0.16.0`, `libfb303-0.9.3`, `hive-storage-api-4.1.0`, vendored in
  `hive-metastore/`) child-first, and `ThriftValueConverter` converts structs and thrift
  infrastructure exceptions across the loader boundary; pinned by `Hive4IsolatedRuntimeTest`.
- The `APACHE_4_1_0` **front door** could not serve a write either: Hive 4 added a fourth
  `LockType` constant, `EXCL_WRITE`, which Apache 3.1.3 - the shape every request is converted
  into before routing - does not have. The value vanished in conversion, the required field
  failed validation, and a Hive 4 client's `INSERT` got a bare "Internal error processing lock"
  and retried forever. Fixed in `Hive4FrontendBridge`: EXCL_WRITE is downgraded to EXCLUSIVE
  (never SHARED_WRITE - a downgrade must not grant concurrency the client asked to exclude);
  pinned by two round-trip tests in `FrontendBridgeThriftSerializationTest`.
- `scheduled_query_poll` is refused with a clean `UNKNOWN_METHOD` every few seconds: the Hive 4
  HiveServer2 polls for scheduled queries, a Hive 4-only feature with no Apache 3.1.3 mapping.
  Log noise by design, not a scenario failure.
- `DELETE .../tables/{table}?purgeRequested=true` answers 500: the server-side purge reads
  manifests through Avro, and the fat jar's avro 1.7 lacks `org.apache.avro.Conversion`. Open
  gap, tracked separately; the scenario uses a plain `DELETE` and removes the files explicitly.
- After a stand rebuild the HiveServer2 JVMs can keep a stale DNS resolution and talk to the
  wrong namenode ("File does not exist" for files that exist); restarting the HS2 containers
  after the network settles is the cure - same class of stale-session issue the 2026-07-27
  rerun already hit.

## F. Not covered, and why

| Area | Reason |
| --- | --- |
| ACID on a non-default catalog | The proxy refuses `allocate_table_write_ids` outside the default catalog **by design** — there is nothing to pass |
| YARN / Tez, distributed execution | The stand runs local MapReduce only; nothing here says how the proxy behaves under a real cluster's concurrency |
| Ranger, Atlas, HA | Out of the stand's scope |
| Cross-realm Kerberos trust | Both clusters share one realm on purpose; cross-realm would test the KDC, not the proxy |
| Concurrency / load | Every scenario is single-client. The synthetic lock shim in particular grants locks without checking conflicts, so nothing here validates writer isolation |

## Revalidation log

Full-matrix reruns after the table above was first filled in. Only what a rerun actually
executed is claimed; a row not listed was not repeated and its ✅ stands on the earlier run.

- **2026-07-27**, jar `1.0.4-38128c8b` (branch `feature/iceberg-rest-fe-phase1` rebased onto
  `main`; the Iceberg REST listener stays disabled, so the Thrift path is what was under test).
  Rerun and green: all of section A on both profiles, sections B and C on both profiles through
  both HiveServer2 instances — except the steps their env flags keep off by default (B9
  cross-database join, C2/C3 ACID SQL, C5 materialized view). Sections D and E were not repeated.
  The rerun surfaced three stand/runner defects, all fixed on `main` the same day: the SQL pass
  exhausted `server.max-worker-threads=64` (each HiveServer2 async-exec thread owns one
  metastore connection — the limit is now 256), the B10 assertion relied on
  `show functions like` matching a bare name that Hive 3.1.3 registers qualified, and the
  runner's cleanup `RETURN` trap re-fired in the enclosing function after a two-pass run and
  killed it under `set -u` after every assertion had already passed.
  Later the same day the branch's Iceberg REST listener was enabled on the plain profile and
  section G was run for the first time (`--scenario rest`, and again as the REST step of a
  full green `--scenario all`).
  The same day, after the `apache` catalog's second Iceberg table (`smoke_iceberg_tbl_ap`) was
  registered on its own cluster (`namenode-b`), the new multi-catalog REST rows (G8-G11) were
  run too, in the same `--scenario rest` and `--scenario all` passes. A follow-up run the same
  day added and passed the federation/isolation rows G12-G16: the federated name under the
  default prefix (listing and load included) and clean 404s for every cross-catalog shape.
  Later still, jar `1.0.20-eec20f1a` added row G17: with `HMS_SMOKE_REST_METRICS_URL` set to the
  stand's management endpoint, both `--scenario rest` and `--scenario all` fetched it with curl
  and confirmed the `hms_proxy_rest_requests_total` and `hms_proxy_rest_listener_info` series were
  present and populated after the REST checks ran.
  Later still, jar `1.0.23-613b7a1e` (the Iceberg 1.9.2 upgrade, Jackson pinned to `2.18.3`)
  re-ran sections A-D and G green, including the SQL layer through both HiveServer2 instances as
  the Jackson-regression detector for the pin.
  Later still, jar `1.0.33-01704804` (the stack-free error, 400-on-unparseable-body and
  endpoint-advertising hardening) added rows G19-G21 and re-ran `--scenario rest` and
  `--scenario all` green; `GET /v1/config` and `GET /v1/apache/config` were fetched with curl and
  both carried the nine-route `endpoints` list, and `docker logs stand-proxy` showed no
  `stream closed` WARN noise from the HEAD checks in G18.
  Later still, jar `1.0.34-5397bb81` strengthened the G21 assertion: the runner used to only
  `grep` for the `"endpoints"` key's presence, which cannot distinguish a read-only listing from
  one that also advertised a write route. It now checks both `GET /v1/config` and
  `GET /v1/{prefix}/config` for the `GET /v1/{prefix}/namespaces` read entry and for the absence
  of any `POST /v1/{prefix}/namespaces` or `DELETE` entry. `--scenario rest` re-ran green against
  the rebuilt jar; the strengthened assertion was proven to discriminate by temporarily pointing
  it at a route name the server does not serve and confirming the runner failed with
  "config does not advertise the namespaces read route" before restoring it.
  Later still, jar `1.0.41-931b78d4` (phase 5a: table writes for the default
  catalog, the write gate, and asymmetric endpoint advertising; a Hadoop
  `hadoop-hdfs`/`hadoop-common` version-alignment fix and the widened
  `Throwable` catch-all in `IcebergHttpHandler` landed on top of it) added
  rows G22-G25 and updated G7, G21. `--scenario rest` and `--scenario all`
  both re-ran green: `GET /v1/config` and `GET /v1/{prefix}/config` (default
  catalog) were confirmed to carry the table-create and table-drop write
  routes; `GET /v1/apache/config` was confirmed to carry neither. A table
  created through `POST /v1/hdp/namespaces/default/tables` loaded back with a
  `metadata-location` and dropped with `204`; a direct create under
  `/v1/apache/namespaces/default/tables` and a create under
  `/v1/hdp/namespaces/apache__default/tables` both answered `403`. The SQL
  layer (sections B and C, both HiveServer2 instances) was re-run as the
  regression check for the Hadoop dependency change, since table writes and
  Hive's own ACID commits now share the same lock path; it passed, with
  `stand-hs2-hdp` restarted first (its HiveServer2 session had gone stale
  after the stand rebuild - a fresh session opened cleanly against the same,
  otherwise-untouched HDFS state) and `HMS_SMOKE_SQL_HDP_SESSION_INIT=set
  hive.execution.engine=mr;` supplied for the Hortonworks pass, as documented
  in `smoke-stand/env/simple.env`.

- **2026-07-28**, jar `1.0.43-c4685ef7` (unchanged on the stand; only the smoke script grew new
  checks against it). Added rows G26-G30: the write round trip now includes a REAL commit against
  the just-created table and a rename round trip, not just create/load/drop, and the gate
  negatives now cover COMMIT_TRANSACTION, CREATE_NAMESPACE and rename-with-federated-destination
  on top of the existing CREATE_TABLE pair - COMMIT_TRANSACTION in particular was a critical
  bypass found during phase 5a and had until now only been pinned down by unit tests. `--scenario
  rest` and `--scenario all` both re-ran green: the create response's `metadata-location` (ending
  `00000-...`) differed from the commit response's (`00001-...`), the renamed table loaded back
  with `200`, and all three new negatives answered `403`. The G26 assertion was proven to
  discriminate by temporarily requiring the commit's `metadata-location` to equal create's
  (i.e. asserting a no-op commit); the runner failed with "did not write a new metadata file",
  confirming the check would catch a silently no-opped commit; the assertion was restored and
  both scenarios re-ran green.

- **2026-07-28** (second entry), the Iceberg REST listener was turned on for the first time in
  the Kerberos profile: the KDC gained an `HTTP/proxy@SMOKE.LOCAL` principal in the same keytab
  the Thrift front door already uses, and `hms-proxy-kerberos.properties` gained a
  `rest-catalog.*` block pointing at it, on the same port 19183 the plain profile uses. Bringing
  the stand up this way surfaced a real bug, not just a missing config row: `IcebergRestService`
  built its own bare `Configuration` instead of reusing the catalog's Kerberos-aware `HiveConf`,
  so every REST write failed with "Failed to specify server's Kerberos principal name" once the
  NameNode RPC was reached; fixed by threading `CatalogBackend.hiveConf()` through
  `IcebergRestServices.open(...)`. A second, stand-only gap followed once the NameNode RPC
  itself worked: the per-catalog Hadoop conf was missing `dfs.data.transfer.protection`, so a
  create's actual block write to the datanode reset the connection ("could only be written to 0
  of the 1 minReplication nodes") even though a plain NameNode-only RPC (the existing purge-path
  delete) had never needed it; added `catalog.hdp.conf.dfs.data.transfer.protection=authentication`
  and the same key for `catalog.apache` to `hms-proxy-kerberos.properties`, matching what
  `hdfs/hadoop-kerberos*.env` already requires of the datanodes. With both fixed, first
  `docker exec stand-proxy /opt/hms-proxy/scripts/run-real-installation-smoke-kerberos.sh
  --scenario all` was re-run to confirm the Hadoop dependency bump the REST feature travelled in
  on (`hadoop-hdfs` 2.2.0 -> 2.6.0) had not regressed the existing kerberized Thrift/lock paths -
  it completed with `scenario 'all' completed successfully` (the notification-negative check's
  `TApplicationException` is the documented libthrift 0.9.3 behavior for exception-less RPCs, not
  a failure). Then, from inside `stand-proxy` after `kinit -kt smoke-user.keytab`, curl
  `--negotiate` drove rows G1, G23-G26 and the new G31 (below): an unauthenticated request got a
  clean `401`/`WWW-Authenticate: Negotiate`; `GET /v1/config` advertised `prefix=hdp` with the
  write routes; a table was created (`200`), loaded back (`200`), committed for real (`200`,
  `metadata-location` moved from a `00000-...` file to a `00001-...` one), refused with `403`
  both directly under the `apache` prefix and via the federated `apache__default` namespace
  under the default prefix, and dropped (`204`). `docker logs stand-proxy` traced the create's
  and commit's `lock`/`unlock` to `catalog=hdp, backend=hdp` with small sequential lock IDs (387,
  388 - the real backend's scheme, not the synthetic shim's), and
  `logs/hms-proxy-audit.log` carried `"authenticatedUser":"smoke-user@SMOKE.LOCAL"` on every one
  of those entries. The remaining Kerberos-column read-only rows (G2-G22, G27-G30) were not
  re-run and stay `n/a`.

- **2026-07-28** (third entry), jar `1.0.49-2b778592` (phase 5b: namespace DDL in
  `RoutingMetaStoreClient` and the full-write-surface `GET /v1/config` advertising). Before this
  run the stand was still on a pre-phase jar, and probing it directly showed `POST
  /v1/{prefix}/namespaces` answering `406` ("does not support `IMetaStoreClient.createDatabase`")
  - namespace DDL had never actually been validated against a real metastore. Added rows G32-G34
  for the three new round trips (namespace DDL, view writes, transaction commit via
  `POST /v1/{prefix}/transactions/commit`); the per-table commit route (G26) was already covered
  and stayed green, unaffected by this phase's changes.
  After rebuilding the fat jar and restaging (`./prepare.sh && docker compose up -d --build`,
  plain profile), `--scenario rest` and `--scenario all` both re-ran green, this time actually
  exercising namespace DDL for the first time: `POST /v1/hdp/namespaces` created
  `smoke_rest_ns` (`200`), `GET` loaded it back, `POST .../properties` set `smoke=yes` (`200`)
  and a follow-up `GET` confirmed the property was actually present, `DELETE` answered `204` and
  a final `GET` answered `404`. The view round trip created `smoke_rest_view` (`200`, a real
  `metadata-location`), listed it, and dropped it (`204`). The transaction round trip created a
  table, committed it through `POST /v1/hdp/transactions/commit` (`204`), and confirmed the
  table's `metadata-location` moved from a `00000-...` file to a `00001-...` one on reload -
  manual curl round trips against the running stand captured the same verbatim responses outside
  the smoke script, for the record.
  Step 4 of the task proved the new transaction assertion actually discriminates: the check was
  temporarily inverted to demand the `metadata-location` stay unchanged, `--scenario rest` was
  rerun and failed with "did not write a new metadata file: metadata-location is still
  '...00001-...'" as expected, then the assertion was restored and both `--scenario rest` and
  `--scenario all` re-ran green.
  The stand was then switched to the Kerberos profile
  (`docker compose --env-file .env.kerberos --profile kerberos up -d --build`) and, from inside
  `stand-proxy` after `kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL`, curl
  `--negotiate` drove G32 (namespace DDL) and G33 (view writes) by hand per the task brief, which
  scoped the Kerberos re-run to those two round trips only. Both passed identically to the plain
  profile - same status codes, same effects - and `hms-proxy-audit.log` showed genuine
  `create_database`/`alter_database`/`drop_database` entries with
  `"authenticatedUser":"smoke-user@SMOKE.LOCAL"`, confirming namespace DDL reached the real HDP
  backend under Kerberos too. G34 (transaction commit) was not re-run under Kerberos and stays
  `n/a`, matching the task's scope.
  Later still (same jar, script-only change, back on the plain profile): the view round trip
  (G33) was extended to drive the two advertised view routes it had never exercised - update
  (`assert-view-uuid` requirement, `POST .../views/{view}`) and rename (`POST
  /v1/{prefix}/views/rename`) - and four more `WriteRouteGate` negatives were added (G35-G38:
  CREATE_VIEW, DROP_VIEW, DROP_NAMESPACE and UPDATE_NAMESPACE, all against the federated
  `apache__default` namespace under the default prefix). `--scenario rest` and `--scenario all`
  both re-ran green: the view update's property reload confirmed `"smoke":"updated"` actually
  stuck, the rename answered `204` and the view loaded back `200` under the new name while the
  old name answered `404`, and all four new negatives answered `403` (the CREATE_VIEW one with
  the full valid view body, since a stub body had earlier answered `400` before the gate was even
  reached). The new rename-effect assertion was proven to discriminate by temporarily flipping its
  expected status from `404` to `200` (i.e. asserting the old view name is still reachable after
  the rename); the rerun failed - the pre-rename name still answered its real `404`, which the
  inverted assertion now rejected - confirming the check would catch a rename that copies the view
  instead of moving it; the assertion was restored and both `--scenario rest` and `--scenario all`
  re-ran green.

- **2026-07-29**, jar `1.0.4-14af4def` (post-merge `main`; includes the fail-closed unresolved-
  namespace gate hardening of `5f84d4e`). The smoke script grew the six checks that completed the
  write-surface coverage - a REGISTER_TABLE round trip (G39: create, non-purge drop, re-register
  from the surviving metadata file, load back, drop) and one gate negative per still-uncovered
  write route (G40-G44: UPDATE_TABLE, DROP_TABLE, REGISTER_TABLE, UPDATE_VIEW and RENAME_VIEW
  with a federated destination) - so every one of the thirteen gated write routes now has both a
  positive and a negative. On the plain profile `--scenario all` ran green twice (before and
  after the SPNEGO refactor below) and `--scenario rest` green in between; the register assertion
  was proven to discriminate by temporarily expecting `403` instead of `200` - the run failed
  with "REST register ... returned HTTP 200" and a full metadata body read back from HDFS,
  confirming both that the check bites and that register genuinely works; the assertion was
  restored and the scenario re-ran green.
  The same day the REST smoke gained `HMS_SMOKE_REST_CURL_OPTS` (extra curl options for every
  REST request, e.g. `--negotiate -u :`) plus an automated version of G31: when the options are
  set, a request WITHOUT them must be rejected `401` with a `WWW-Authenticate: Negotiate`
  challenge and an empty body. `env/kerberos.env` gained the full REST block (in-network URL
  `http://proxy:9183`, both prefixes, the write table/namespace/view and the management metrics
  URL `http://proxy:9090/metrics`), so the Kerberos REST column is now driven by the script
  itself instead of hand-typed curl. The stand was then switched to the Kerberos profile and
  `docker exec stand-proxy /opt/hms-proxy/scripts/run-real-installation-smoke-kerberos.sh
  --env-file /opt/hms-proxy/smoke-env/kerberos.env --scenario all` (after a kinit and a
  `docker cp` of the updated `scripts/` and env file) completed with `scenario 'all' completed
  successfully` - the first scripted full REST pass under Kerberos. That run turned the kerberos
  column of G2-G17, G19-G22, G27-G30 and G34-G38 from `n/a` to observed-green and covered the
  new G39-G44 on both profiles; G18 (HEAD requests) remains hand-driven and stays `n/a` under
  Kerberos. Sections B-D (SQL/HDFS layers) were not re-run - the changes are REST-smoke-only,
  the jar's Java-side delta since the last full SQL pass is the write-gate hardening, and the
  A-section CLI scenarios (txn, locks, notification) re-ran green on both profiles as part of
  the two `--scenario all` passes.

- **2026-07-29** (second entry), section H added: the Iceberg interop scenario over a Hive 4.1.0
  backend, run green on both profiles the same day it was built. New stand pieces: the
  `hms-hive4` container (official `apache/hive:4.1.0`, thin wrapper in `smoke-stand/hms-hive4/` -
  the official image's own conf-symlink mechanism silently does nothing because the image lacks
  `find`, so the wrapper writes conf files directly), the `hms-proxy-hive4[-kerberos].properties`
  configs (default catalog `hive4`, `runtime-profile=APACHE_4_1_0`), the
  `iceberg-rest-writer` client (Iceberg 1.9.2, Parquet into HDFS + REST commits, per-request
  SPNEGO via a custom `AuthManager` under Kerberos), `iceberg-hive-runtime` 1.6.1 in both
  HiveServer2 images, and the `hive/hms-hive4@SMOKE.LOCAL` principal in the KDC. The proxy fix
  the scenario forced (companion jars + child-first thrift for the Hive 4 isolated runtime,
  cross-loader `ThriftValueConverter`) re-ran the full unit suite green (641 tests). A fresh
  HDFS needs `/warehouse/hive4` created alongside the other warehouse dirs - the REST create
  path does not mkdir it. The purge-drop 500 (H notes) remains open.

- **2026-07-29** (third entry), the Hive 4 **front door** was added to the same profile and
  section H grew its H4 row: `additional-frontends.hive4fe` on 9085 (`APACHE_4_1_0`) plus
  `hs2-hive4`, a Hive 4.1.0 HiveServer2 from the official image (Tez local mode, Iceberg built
  in), and the `hive/hs2-hive4@SMOKE.LOCAL` principal. The scenario now proves one Iceberg table
  is readable and writable through all three Thrift dialects and REST at once; both profiles
  re-ran green (`rows=5`: 2 rest + 1 hdp + 1 apache + 1 hive4).
  Turning the front door on found the compatibility bug described in the H notes (EXCL_WRITE),
  and the Kerberos pass needed two stand-side settings the 3.1 HiveServer2 images already had in
  some form: `yarn.resourcemanager.principal` + `mapreduce.job.hdfs-servers` in *core-site.xml*
  (without them the INSERT dies with "Can't get Master Kerberos principal for use as renewer"),
  and `tez.local.mode.without.network=true` - Tez local mode otherwise talks to its in-process AM
  over Hadoop RPC, which under Kerberos demands SASL it has no principal for ("Client cannot
  authenticate via:[TOKEN, KERBEROS]" → "TezSession has already shutdown"). The official image
  also ships no Kerberos client, so the wrapper installs `krb5-workstation` for the beeline the
  smoke runs inside it. The stale-DNS trap bit twice more: `docker compose up --build <service>`
  recreates the whole depends_on chain including HDFS, after which the 3.1 HiveServer2 pair must
  be restarted (and a run started during that recreation fails its HDFS write outright).

- **2026-07-29** (fourth entry), the interop scenario stopped being hive4-only: `--prefix` now
  names whichever catalog the running config makes default, `hs2-hive4` moved to its own compose
  profile (`hive4fe`) so the Hive 4 dialect can be driven against any backend, the Hive 4 front
  door was added to `hms-proxy.properties`/`hms-proxy-kerberos.properties`, and a new
  `hms-proxy-apache[-kerberos].properties` pair (plus `.env.apache[-kerberos]`) swaps the roles
  of the two 3.1-line metastores so the Apache 3.1.3 one becomes default. That last layout is
  the only way to put `APACHE_3_1_3` on the REST write path at all - writes are gated to the
  default catalog - and it also moves the whole scenario onto the second HDFS cluster. Section H
  was then run green six times, once per backend and profile: `hdp` plain and kerberos, `apache`
  plain and kerberos, `hive4` plain and kerberos (the last two re-run after the refactor, so no
  cell rests on the pre-refactor script). Each run ended with `rows=5` and the table gone. No new
  proxy defect surfaced: the EXCL_WRITE downgrade found earlier is what already made the Hive 4
  dialect work over a 3.1 backend, which is the `hive4_frontdoor_to_apache_backend_downgrade`
  capability being driven by a real Hive 4 client for the first time.

- **2026-07-29** (fifth entry), `--origin` was added so each front door in turn creates the table
  while the other three modify it (rows H9-H12), and the scenario grew a read *before* every
  append plus a final all-participants round, so each hand-off is proven rather than assumed.
  Eight runs on the `hive4` backend, plain and Kerberos for each of the four origins, all green.
  The Hive 4 origin is the one that does not reach the 3.1 line, for a reason outside the proxy:
  `STORED BY ICEBERG` writes no concrete `inputFormat` into the StorageDescriptor. That was
  confirmed by hand before being written down - the descriptor the proxy relayed carried
  `org.apache.hadoop.mapred.FileInputFormat`, and naming the handler class explicitly in the DDL
  produced `inputFormat: null` instead. The reverse direction works: tables created by the 3.1
  storage handler carry `HiveIcebergInputFormat` and Hive 4 reads and appends to them happily.

## Two caveats on faithfulness

- The Kerberos profile is complete end to end — client → HiveServer2 → proxy → metastores → HDFS,
  with no service falling back to simple auth. But the HDP HiveServer2 only starts with
  `hive.in.test=true`, which is what lets a session switch the engine to `mr`; Hortonworks builds
  without MapReduce. The *metadata* path is unaffected, query execution is not what an HDP cluster
  would do.
- All SQL runs as local MapReduce, so timings and concurrency behaviour say nothing about
  production.
