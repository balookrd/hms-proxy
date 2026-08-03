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
| C8 | **The paired topology** - each front door with its own metastore as the default catalog, the other as a remote one: Hortonworks front door while `hms-hdp` is default, Apache front door while `hms-apache` is (`.env.apache`). Both pass the whole of sections B and C **including the ACID block**, with `allocate_table_write_ids` in the proxy log and the transactional table created. The Apache pairing sends no `add_write_notification_log` at all, which is why C7 does not arise there | ✅ both pairings | ✅ both pairings |
| C6 | **Cross pairing, outside the paired topology of C8.** ACID is a property of the **front door**, not just the catalog: the same `create` + `insert` on a transactional table succeeds here and fails through the Apache front door, where the insert lands and the stats update is then refused with `Cannot change stats state for a transactional table without providing the transactional write state for verification (new write ID -1, valid write IDs null)`, surfacing as a failing `StatsTask`. **The proxy is not losing the write ID** - the two clients issue different RPCs. The vendor build never calls `set_aggr_stats_for` at all: it goes `get_valid_write_ids` → `alter_table_with_environment_context` → `commit_txn`. The Apache 3.1.3 client instead ends with `set_aggr_stats_for`, which carries no transactional write state, and the Hortonworks backend refuses it, after which the client aborts the txn. Ruled out along the way: federation is not the trigger (a non-federated `default` database fails identically), stand configuration is not (both HiveServer2 instances carry the same `hive.support.concurrency`/`hive.txn.manager`), and field loss in the proxy is not (`NamespaceInternalizer` deep-copies the struct). What this stand cannot settle is whether an Apache 3.1.3 client would hit the same rule against an Apache metastore: with that metastore as the default catalog the statement dies earlier, at C7 | ✅ refused as described | — |
| C7 | **Cross pairing, outside the paired topology of C8.** With the **Apache 3.1.3 metastore as the default catalog** (`.env.apache`) there is no ACID path at all: Hive issues `add_write_notification_log` itself after an ACID write, and the proxy refuses the Hortonworks-shaped call whenever the backend is not a Hortonworks runtime (row A6 records that refusal as correct). The statement dies in `MoveTask` with a bare `Internal error processing add_write_notification_log`. Everything non-ACID in sections B and C passes on that layout | ✅ | — |

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
| H12 | Hive 4 HiveServer2 (`STORED BY ICEBERG`) | REST, HDP, Apache → 5 rows; it takes two proxy-side fixes, see below | ✅ | ✅ |

Every participant reads the running total *before* its own append, so each hand-off across the
front-door boundary is proven rather than assumed, and a final round has all participants
confirm the same count.

**H12 in detail: who is allowed to keep the Hive-engine descriptor.** This row used to read
"a Hive 4-created table is unreadable by the 3.1 line", on the belief that `STORED BY ICEBERG`
leaves the StorageDescriptor's `inputFormat` as the abstract
`org.apache.hadoop.mapred.FileInputFormat`. **That explanation was wrong, and it was wrong in a
way the scenario could not notice**, because `--origin hive4` carved the two 3.1 engines out of
the run and asserted the limitation instead of testing it. Measured again on 2026-07-31: Hive 4
creates the table with the concrete `HiveIcebergInputFormat`, and both 3.1 engines read it. What
is real is a *write*-side defect, and there are two of them, in two different processes.

Both come from the same fork in Iceberg's `HiveTableOperations`. Every commit rebuilds the
StorageDescriptor, and it writes one of two shapes: with the Hive engine enabled,
`storage_handler` plus the concrete `HiveIcebergInputFormat`/`OutputFormat`/`SerDe`; with it
disabled, the abstract `FileInputFormat`/`FileOutputFormat`/`LazySimpleSerDe`, and
`storage_handler` *removed*. Which one it picks comes from the table's own
`engine.hive.enabled`, and, when the table does not set it, from `iceberg.engine.hive.enabled`
in the Hadoop configuration of whatever process is committing. A table created by Hive 4's
`STORED BY ICEBERG` sets no `engine.hive.enabled` at all - verified by reading its
`metadata.json` - so every later committer decides this for itself:

- **The proxy's own REST commits** used to fall on the disabled side, because the REST front
  door built its Iceberg client without the flag. One REST append rewrote a Hive-created table
  into the plain-files shape and the 3.1 engines could no longer open it. Fixed by
  `rest-catalog.hive-engine-descriptor` (default `true`), applied to a copy of each catalog's
  Hadoop `Configuration` in `IcebergRestServices.open`.
- **A 3.1 HiveServer2's own commits** fall on the disabled side too - `iceberg-hive-runtime`
  1.6.1 inside `hs2-hdp`/`hs2` reads the flag from *its* `hive-site.xml`, which does not set it,
  and no proxy setting can reach that JVM. So HDP's own `INSERT` onto a Hive 4-created table
  degraded the descriptor a step later, and the request carrying it is a perfectly legitimate
  forward commit that `IcebergTablePointerGuard` had no pointer-related reason to touch. Fixed
  in that same guard: having read the record anyway, it now also keeps the Hive-engine
  descriptor the record holds (`routing.iceberg-pointer-guard.hive-engine-descriptor`, default
  `true`, counted as the `hive_descriptor_kept` outcome). It only ever *keeps* - a table the
  metastore records without a storage handler is never given one.

So the proxy is not a bystander here after all: it is the one place both the REST writer and
every SQL engine pass through, and therefore the only place where a table created by one engine
can be protected from another engine's idea of whether Hive should be able to read it. Setting
`iceberg.engine.hive.enabled=true` in every engine's own `hive-site.xml` would fix the second
half at the source, and on a real cluster that is worth doing; the stand deliberately does not,
so that this scenario keeps testing the proxy rather than the workaround.

### H13-H20. Row-level DML: `DELETE` and `UPDATE`

Everything above only ever appends, so nothing in it produces a delete file. This block does.
Driven by `smoke-stand/run-iceberg-rowlevel-smoke.sh`: Hive 4 - the only engine on the stand with
native row-level DML over Iceberg - deletes and updates rows in a v2 table the REST front door
created, and the other three front doors then have to read what it left behind. Run on the
`hive4` backend, once per `write.delete.mode`/`write.update.mode` value:

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| H13 | Hive 4 `DELETE FROM ... WHERE` removes rows from a REST-created v2 table, and REST sees it: its own scan drops from 5 rows to 3 and no longer finds the deleted id | ✅ | ✅ |
| H14 | Hive 4 `UPDATE ... SET` changes a value in place, and REST sees the new one: still 3 rows, `src=updated` matches exactly 1 and `src=rest` the other 2 | ✅ | ✅ |
| H15 | `merge-on-read` really is merge-on-read: after the delete the planned scan is 1 data file **plus 1 delete file** - the original five-row data file is untouched and the rows come out at read time | ✅ | ✅ |
| H16 | **Both 3.1 engines read the merge-on-read result correctly** - a full row scan through each returns exactly the surviving rows, so `iceberg-hive-runtime` 1.6.1 does apply position delete files | ✅ | ✅ |
| H17 | The HDP 3.1 engine still `INSERT`s onto a table Hive 4 has row-level modified, and all four front doors then agree on the 4 rows | ✅ | ✅ |
| H18 | Neither 3.1 engine can do row-level DML of its own: `DELETE` and `UPDATE` are refused at compile time with `SemanticException [Error 10297]: Attempt to do update or delete on table default.smoke_iceberg_rowlevel that is not transactional`, and the table's contents are unchanged afterwards | ✅ | ✅ |
| H19 | `copy-on-write` really is copy-on-write: the same delete leaves **0 delete files** because Hive 4 rewrites the data file instead, and every engine reads the same result | ✅ | ✅ |
| H20 | The purge-drop still cleans up a v2 table that has delete files: no parquet, avro or `metadata.json` survives under the table directory | ✅ | ✅ |

**The boundary is on the write side, not the read side** - the same way round as H12 above. A 3.1
HiveServer2 carrying `iceberg-hive-runtime` 1.6.1 plans a scan of a format-version 2 table with
position deletes and applies them; what it cannot do is *produce* them, because the Hive 3
storage handler registers no ACID-capable table and the semantic analyzer stops the statement
before a plan exists. So a Hive 4 writer and a 3.1 reader can share a row-level-modified table,
and a 3.1 client that tries to modify one fails loudly and early instead of half-writing. Unlike
H12, this one really is not a proxy decision: the statement never reaches the metastore at all.

Two things the scenario is careful about, because either would make it pass vacuously:

- Every read assertion is a full `select id, src` row scan, never `select count(*)`. Hive can
  answer a count from the Iceberg summary it keeps as table stats, so a reader that cannot apply
  delete files would still report the right number.
- The mode is asserted from the table's file shape rather than trusted as a setting, and the two
  values are what make each other meaningful: the same assertion sees 1 delete file under
  `merge-on-read` and 0 under `copy-on-write`, so a Hive 4 that ignored the property would fail
  one of the two runs.

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
- `DELETE .../tables/{table}?purgeRequested=true` answered 500: the purge walks the table's
  manifests through Avro, and Maven had resolved avro 1.7.4 (from `hadoop-mapreduce-client-core`,
  same tree depth, earlier declaration) over the 1.12.0 `iceberg-core` is compiled against.
  Fixed by pinning avro; the scenario now ends with a real purge and asserts no data, manifest or
  metadata file survives it.
- After a stand rebuild the JVMs can keep a stale DNS resolution and talk to the wrong namenode
  ("File does not exist" for files that exist, or a failed HDFS write straight after start);
  restarting the affected container once the network settles is the cure. It bites the proxy and
  all three HiveServer2 instances alike, and `docker compose up --build <service>` is enough to
  trigger it, because that recreates the whole depends_on chain including HDFS - same class of
  stale-session issue the 2026-07-27 rerun already hit.

## I. Writer isolation

The write gate lets writes into the default catalog only, on the argument that just that
catalog's commits take a real Hive lock while the rest are served by a shim that grants locks
without checking conflicts. Both halves of that argument are now pinned.

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| I1 | The synthetic shim grants two conflicting EXCLUSIVE locks on the same partition at once - the unsafety the write gate exists to contain, pinned as a unit test so making the shim conflict-aware has to be deliberate (`RoutingMetaStoreProxySyntheticReadLocksTest#syntheticShimGrantsConflictingExclusiveLocksOnTheSameObject`) | n/a | n/a |
| I2 | 5 concurrent REST writers appending to one table on the default catalog: all 5 commit, the table holds exactly 6 rows (1 baseline + 5) - no lost update | ✅ | ✅ Hive 4 backend |
| I3 | 8 concurrent writers: the row count equals the writers that reported success plus the baseline, and a writer that is refused is refused with `CommitFailedException: branch main has changed` - contention is resolved by rejecting a stale writer, never by silently overwriting one | ✅ 7 commit, 1 refused | ✅ Hive 4 backend; how many are refused varies run to run - 7 commits and 1 refusal in one run, 8 and none in another |
| I4 | **Across front doors**: REST appends and Hive `INSERT`s (Hortonworks front door) commit to the same table with overlapping commit windows | ✅ | ✅ 12/12 on the 3.1 backend, against 1 loss in 12 before the repair took the Iceberg table lock |
| I5 | **Multi-table transaction under contention**: a two-table `POST /v1/{prefix}/transactions/commit` whose requirement for the second table was invalidated by a competing writer is refused `409 CommitFailedException: Requirement failed: branch main has changed`, **neither** table is left carrying the update, and the competing writer's rows survive | ✅ | ✅ |
| I6 | The same route is **not** atomic when a commit fails rather than a requirement: requirements are all validated up front, then the tables are committed one by one with no rollback, so a failure partway leaves the earlier tables committed and the request answers `500 CommitStateUnknownException` (`IcebergRestEndpointIntegrationTest#multiTableTransactionMustNotReportSuccessWhenTheSecondCommitFails`, confirmed on the stand's real Hive 4 metastore by starving the ddl rate-limit class) | — | ✅ |

Driven by `smoke-stand/run-iceberg-concurrency-smoke.sh`, which counts the writers that exited 0
and requires the row count to match them exactly. A writer that fails loudly is correct
behaviour and does not fail the run; a writer that reports success while its rows are missing
does.

For I4 the row count alone would prove nothing: a beeline `INSERT` spends tens of seconds in
MapReduce before it commits, while a REST append commits a second after it starts, so firing
both at once just runs them in sequence. The scenario therefore keeps issuing REST appends in
rounds for as long as any SQL writer is alive, and then **asserts the overlap**: each Iceberg
commit ends in an `alter_table` the proxy logs with its thread, REST requests on
`hms-proxy-rest-*` and Thrift ones on `pool-*-thread-*`, and the two windows have to intersect.
The detector was checked against a run that did *not* overlap (REST finished 4 s before the
first SQL commit) and reports it as such, so a vacuous pass fails the run instead.

I5 and I6 answer the same question from the two sides a client can hit. Driven by
`smoke-stand/run-iceberg-txn-contention-smoke.sh`, whose contention is real rather than injected:
a second writer appends to one of the two tables through the same front door, which advances that
table's `main` ref, and the transaction then arrives carrying the snapshot id it read before that
append - the shape a losing racer would have sent, with no timing games needed. The scenario ends
with a **positive control** - the same transaction with the current snapshot id must be accepted
and applied to both tables - because without it a malformed body, a wrong prefix or a
non-writable table would refuse the transaction just as convincingly. Requirement contention is
therefore all-or-nothing while a mid-transaction commit failure is not, and a client must not read
"transaction" as "atomic under every failure".

What the run does **not** show: no `check_lock` and no `WAITING` appeared in the proxy log, so
what rejected the stale writer was Iceberg's own requirement check on the branch's snapshot id,
not a lock wait. The Hive lock still matters - it is what makes the read-verify-then-`alter_table`
window atomic - but this run did not have to exercise the blocking path to protect the data.

### I4 in detail: mixing REST and SQL writers loses rows

On the plain profile the cross-path run passed repeatedly (13 of 14 writers commit, one REST
writer refused with "branch main has changed", 14 rows - exactly right). On the Kerberos profile
the same scenario **loses a committed row about half the time**, and the loss is on the REST
side: with 4 REST writers per round against 2 Hive `INSERT`s, a run ended with 14 writers
reporting success and 14 rows instead of 15, and the per-marker breakdown named the victim -
`baseline, sql901, sql902, w1..w7, w9..w12`, with **w8 missing**.

That writer's process exited 0, which means `newAppend().commit()` returned normally: Iceberg
told it the commit had landed. A commit that returns success and then vanishes is data loss, not
contention - a stale writer is supposed to be refused with `CommitFailedException`, which is
exactly what happens to the writers that *do* fail here.

REST-only runs (5 and 8 concurrent writers, both profiles) have never lost a row, so the
suspicion is the mix: the proxy commits through Iceberg 1.9.2 while HiveServer2 commits through
`iceberg-hive-runtime` 1.6.1 inside its own JVM, and the two only meet at the metastore lock.
Whether the fault is a lock the Hive side does not take, one the proxy does not hold long
enough, or something else is unproven - that is the first thing to settle. Reproduce with:

```bash
smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hive4 --writers 4 --sql-writers 2 --sql-engine hdp --kerberos
```

Not seen on plain yet, but nothing about the mechanism looks auth-specific; the Kerberos runs are
simply slower, which widens the window.

**The cause, and how far the fix goes.** A HiveServer2 `INSERT` opens with an
`alter_table_with_environment_context` carrying `alterTableOpType=DROPPROPS` and the `Table` it
snapshotted when the query was compiled. The metastore applies those parameters wholesale, so
every Iceberg key the record holds and the request omits is erased - `metadata_location` first of
all, but also `table_type`, `storage_handler`, `previous_metadata_location` and the
`current-snapshot-*` set - and the call travels outside the Iceberg lock, so nothing serializes
it. `IcebergTablePointerGuard` now merges such an alter over the record the metastore currently
holds (the record's parameters as the base, the client's on top, both pointers forced back), and
tells a genuine commit apart by its `previous_metadata_location` (a request whose base is the
current pointer is moving forward and is passed through untouched; anything else carries a stale
copy).

**Keying the guard off the request was a no-op; it is now keyed off the metastore record.**
Verified on the wire: the `alter_table` HiveServer2 sends carries `params={EXTERNAL, numFiles,
numRows, totalSize, transient_lastDdlTime}` and **no `metadata_location` at all**, so the first
version of the guard - which looked for a stale pointer *in the request* - returned on its first
check. The six clean runs it was credited with proved nothing: the WARN it logs when it repairs a
pointer never appeared once, and at the observed one-in-eight loss rate a six-run clean streak
happens about 45% of the time anyway. Whether the target is an Iceberg table is now read from the
metastore.

**Measured, with the counter that makes a green run mean something.** Ten consecutive runs of the
command above, all green *and* all with `hms_proxy_iceberg_pointer_guard_events_total{outcome=
"repaired"}` incremented by exactly 2 per run - one per SQL writer, the DROPPROPS alter that
opens each `INSERT`. Row counts: 11/11, 15/15, 15/15, 10/10, 11/11, 11/11, 11/11, 10/10, 15/15,
15/15 (rows vs. 1 baseline + successful writers); two of the ten refused one REST writer loudly,
which is correct behaviour. `outcome="forward_commit"` ran 10-15 per run - the REST commits, recognised
and left alone. For comparison, the eight runs recorded here earlier were runs with the guard
silently doing nothing, and one of them lost a row.

The first run also caught a defect the unit tests could not: the guard read the record by raw
method name, and Hive 4 has no positional `get_table` in its IDL, so all 13 reads of that run
failed with `NoSuchMethodException` (`outcome="read_failed"`) and nothing was repaired - on
exactly the backend line whose compare-and-swap the guard depends on. The read now goes through
the backend adapter, which upgrades it to `get_table_req`.

**What the extra read costs.** Same stand, both HiveServer2 instances, `create table` plus five
`INSERT`s each - 15 `alter_table` either way, none of them on an Iceberg table:

| `table-cache-ttl-ms` | reads (`not_iceberg`) | no read (`cache_suppressed`) | mean `alter_table` |
| --- | --- | --- | --- |
| `30000` (default) | 2 | 13 | 11.1 ms (0.166 s / 15) |
| `0` (cache off) | 15 | 0 | 12.4 ms (0.185 s / 15) |

So the negative cache removed 87 % of the added round trips, and even with every alter reading,
the read is about 1.3 ms on an `alter_table` that already costs ~11 ms. Iceberg tables are never
cached - their pointer must be read fresh - which is why the concurrency runs above show reads on
every alter of the table under test.

**The rest of the race, and how it was closed.** Reading the pointer and applying the alter were
two separate calls, so a commit landing between them was still overwritten. On Hive 4 backends the
repaired alter's `expected_parameter_key`/`expected_parameter_value` turned that into a loud
failure; the 3.1 line ignores both keys, so there the window stayed open. It is now closed by
holding the table lock Iceberg itself takes across the repair.

**What the stand showed about the locks, before any code was written.** One SQL `INSERT` into an
Iceberg table on this profile, from the proxy trace log:

| time | call | lock |
| --- | --- | --- |
| `08,045` | Hive locks for its own transaction (txnid 957) | `LockComponent(db=_dummy_database, table=_dummy_table)` and nothing else |
| `08,249` | the `DROPPROPS` alter the guard repairs | no lock held on the table |
| `12,982` | HiveServer2's Iceberg commit takes its lock | `LockRequest(txnid=0, components=[LockComponent(db=default, table=<table>)])` |
| `13,033` | that commit's `alter_table` | **inside** that lock |
| `13,097` | `unlock` | held for 115 ms |

Two facts decided the design. Hive takes **no** lock on the target table of an `INSERT`, so a lock
the guard takes while serving that `INSERT`'s alter cannot queue behind the statement it serves.
And a genuine Iceberg commit sends its `alter_table` from **inside** the table lock, so acquiring
that lock before deciding what the alter is would block on a lock held by the caller waiting for
the answer - a self-deadlock on every honest commit. The guard therefore reads unlocked first and
locks **only to repair**, then re-reads under the lock and merges over what it finds. The request
shape is copied from `org.apache.iceberg.hive.MetastoreLock`, which is identical in Iceberg 1.6.1
(inside HiveServer2) and 1.9.2 (the proxy's REST path): one EXCLUSIVE, table-level component with
the backend database name, no `txnid`.

**Measured, both before and after.** Twelve runs of the command above with `--prefix hdp` (the 3.1
backend, where the metastore ignores the compare-and-swap), first on the unchanged jar:

| | runs | lost updates | `repaired` | under the lock |
| --- | --- | --- | --- | --- |
| before (guard, no lock) | 12 | **1** - run 12 held 10 rows for 10 successful writers, and the missing marker was `sql901` | 2 per run | n/a |
| after (guard holds the lock) | 12 | **0** - every run matched rows to successful writers | 2 per run | 2 per run |

The loss rate on the 3.1 line had never been measured before this - the earlier figures are all
from `hive4`, whose compare-and-swap already turns a lost update into a loud failure. Across the
twelve runs after the change, `repair_locked` equalled `repaired` exactly (24 of each), and
`repair_lock_timeout`, `repair_lock_failed` and `lock_release_failed` stayed at zero: every repair
was atomic, and no lock was stranded. That last one matters on this stand in particular - its
metastore runs with `metastore.compactor.initiator.on=false` and no housekeeping threads, so a
leaked lock would never be reaped and would block every later commit on the table.

**What the lock costs.** Sections B and C through both HiveServer2 instances, and separately five
SQL `INSERT`s into one Iceberg table (each one repair plus one forward commit):

| workload | `lock-enabled` | `alter_table` | mean | locks taken |
| --- | --- | --- | --- | --- |
| sections B + C | `true` | 14 | 20.7 ms | **0** |
| sections B + C | `false` | 14 | 13.9 ms | 0 |
| 5 `INSERT`s, Iceberg table | `true` | 10 | 14.7 ms | 5 (`repair_locked`) |
| 5 `INSERT`s, Iceberg table | `false` | 10 | 15.7 ms | 0 (`repair_lock_skipped`) |

The SQL sections take **no lock at all** in either configuration - none of their tables is an
Iceberg table, so no repair fires - which makes the 20.7-vs-13.9 ms gap pure run-to-run variance
and, incidentally, the resolution limit of this measurement: about 7 ms on 14 samples. On the path
that does lock, three added RPCs (`lock`, the second `get_table`, `unlock`) land inside that same
noise - the locked runs came out 1 ms *faster* than the unlocked ones on 5 repairs each.

**What is still open.** A lock that is not granted within `lock-acquire-timeout-ms` (10 s by
default) leaves the repair unprotected rather than refusing the write, and a backend whose ACID
housekeeping does reap timed-out locks could reap this one out from under an `alter_table` that
takes longer than `hive.txn.timeout`. Both are counted (`repair_lock_timeout`, and the WARN that
goes with it) rather than assumed away; neither occurred in these runs.

## F. Not covered, and why

| Area | Reason |
| --- | --- |
| ACID on a non-default catalog | The proxy refuses `allocate_table_write_ids` outside the default catalog **by design** — there is nothing to pass |
| YARN / Tez, distributed execution | The stand runs local MapReduce only; nothing here says how the proxy behaves under a real cluster's concurrency |
| Ranger, Atlas, HA | Out of the stand's scope |
| Cross-realm Kerberos trust | Both clusters share one realm on purpose; cross-realm would test the KDC, not the proxy |
| Sustained load | Section I covers concurrent REST commits to a single table (I2, I3), REST mixed with SQL writers across front doors (I4) and a two-table transaction under contention (I5, I6). What is still missing is duration: every run is a burst of a handful of writers, never sustained load, and nothing measures throughput or latency under it |
| Iceberg partitioned tables, schema evolution, `MERGE INTO` | H13-H20 cover row-level `DELETE`/`UPDATE` on an unpartitioned table with a fixed schema. Partition specs (and their evolution), added/renamed/dropped columns and `MERGE INTO` have never been run through the proxy |

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

- **2026-07-29** (sixth entry), the purge fix landed and the scenario stopped working around it.
  `DELETE ...?purgeRequested=true` had answered 500 because Maven resolved avro 1.7.4 over the
  1.12.0 `iceberg-core` is compiled against; with the pin in place the purge was driven by hand
  against the stand first - a table with two rows, five files under it (parquet, manifest,
  manifest list, two metadata JSONs), `204` back, zero files left and a `404` on reload - and the
  interop scenario now ends with that same purge plus an assertion that nothing survives it. The
  proxy turned out to cache stale namenode DNS the same way the HiveServer2 JVMs do: the first
  create after an HDFS recreation failed its write until the proxy container was restarted.

- **2026-07-29** (seventh entry), section I - writer isolation - was added and then extended
  across front doors. The stand runs: 5 REST writers (all commit, 6 rows), 8 REST writers (7
  commit, 1 refused with "branch main has changed", 8 rows), and REST against Hive `INSERT`s on
  the Hortonworks front door (13 of 14 commit, 14 rows). The cross-path row count was worthless
  at first - the REST side finished four seconds before the first SQL commit, which the proxy log
  showed plainly - so the scenario was rebuilt to issue REST appends in rounds while the SQL side
  runs, and to assert the commit windows intersect. That detector was then checked against the
  original non-overlapping log and correctly calls it a non-overlap, so the assertion cannot pass
  vacuously.

- **2026-07-30**, repo at `074526b` on `main`; **no proxy code was under test** - this change is a
  new runner (`run-iceberg-rowlevel-smoke.sh`) plus three additions to the REST writer
  (`--properties` on `create`, `--where` on `count`, and a `files` command that reports the
  planned scan's data- and delete-file counts). The stand ran the fat jar already staged in
  `smoke-stand/proxy/`, unchanged across all four passes. Section H gained rows H13-H20, which
  close the row-level gap the interop scenario left: it only ever appends, so until now no delete
  file had ever existed on the stand.
  Four runs, all green, all on the `hive4` backend: `--mode merge-on-read` and
  `--mode copy-on-write`, each on the plain profile (`.env.hive4`, profiles `hive4`+`hive4fe`+
  `hdp`) and then on Kerberos (`.env.hive4-kerberos`, `--kerberos`). Each pass is the same
  sequence - REST creates a format-version 2 table and appends 5 rows, all three SQL engines read
  it as a control, Hive 4 deletes two rows and updates one, the REST client verifies the effect
  (3 rows, `src=updated` exactly 1, `src=rest` exactly 2), all three engines read the result, the
  HDP engine appends onto it, both 3.1 engines are refused their own `DELETE`/`UPDATE`, and REST
  purge-drops the table with the no-leftovers assertion. The writer was then refactored (methods
  moved, one log line) and the Kerberos merge-on-read pass re-run green against the rebuilt jar,
  so no green cell rests on a jar that no longer exists.
  The headline result is a **negative** finding, recorded as such: the 3.1 line reads
  merge-on-read fine. The expectation going in was that `iceberg-hive-runtime` 1.6.1 would not
  apply position deletes and that H16 would end up as another "Hive 4 wrote it, 3.1 cannot read
  it" limitation next to H12. It does apply them - both 3.1 engines returned exactly
  `1:rest, 3:rest, 5:updated` - so the limitation is only that the 3.1 line cannot *write*
  row-level changes (H18, refused at compile time with SemanticException 10297).
  No assertion had to be inverted to prove it discriminates: `merge-on-read` and `copy-on-write`
  run the identical file-shape check and produced 1 delete file and 0 respectively, so a Hive 4
  that silently ignored `write.delete.mode` would have failed one of the two runs. The row scans
  are deliberately `select id, src` and not `select count(*)`, since a count can come from Hive's
  cached Iceberg stats without reading a delete file at all.
  Not run: the `hdp` and `apache` backends (`--prefix hdp` / `--prefix apache`) - the row-level
  rows rest on the `hive4` backend only, since what they exercise is a Hive-side capability and
  not a backend runtime profile; also untouched were partitioned tables and schema evolution,
  which stay uncovered (see section F), and `MERGE INTO`. No other section was re-run.
  Stand notes for a repeat: the runner is host-side, so its `sed` is BSD `sed` - the first
  version used a `\|` alternation that silently matched nothing and the run failed on an empty
  file-shape reading. Switching the stand between `.env.hive4` and `.env.hive4-kerberos`
  recreates the HDFS chain, and the usual stale-DNS restart applies.

- **2026-07-30**, jar `1.0.19-f4cbeea7` built from `f4cbeea` and staged into the stand, so the
  proxy code under test is what is committed - the pointer guard's repair now takes Iceberg's
  table lock, and `rest-catalog.purge.mode` exists. Closed the last two open cells of section I
  and added rows I5 and I6.
  I2 and I3 got their Kerberos column (`--prefix hive4 --kerberos`, `--writers 5` and
  `--writers 8`, `--sql-writers 0`): 6 rows for 1 baseline + 5, and for 8 writers the row count
  equal to the writers that reported success. Run twice at eight writers on purpose, and that is
  what corrected the table: one run refused a writer (7 + 1), the next refused none (8 + 0). Both
  are correct, so I3's wording - and the stand README's claim that eight writers "reliably"
  produce a refusal - were overstating a run-to-run variable. The invariant the scenario asserts
  is the row count.
  I5 and I6 come from the new `run-iceberg-txn-contention-smoke.sh`, run on the `hive4` backend on
  both profiles (Kerberos three times, plain once after bringing the whole stand up under
  `.env.hive4`): the two-table transaction with a stale `assert-ref-snapshot-id` is refused `409
  CommitFailedException`, neither table keeps the update, the competing writer's 5 rows are intact,
  and the positive control at the current snapshot id is accepted and applied to both tables.
  Not run: I6 on the plain profile - it is pinned by
  `IcebergRestEndpointIntegrationTest#multiTableTransactionMustNotReportSuccessWhenTheSecondCommitFails`
  and was confirmed on the stand under Kerberos by starving the ddl rate-limit class; reproducing
  that on the plain profile needs the same config change and adds nothing the unit test does not
  decide deterministically. No other section was re-run, and `stand-hs2-hdp` was left on its
  Kerberos container throughout (it is in the `hdp` profile, which these runs do not use).
  Stand notes for a repeat: the runner is host-side, so `sed` is BSD `sed` - `\?` is **not** read
  as "optional" there, and a GNU-style BRE silently matched nothing, returning each JSON field
  with its own name still attached (`grep` does honour `\?`, which is what masked it). Under
  Kerberos `curl` runs inside `stand-proxy`, so neither `-o` nor `--data @file` may name a host
  path - the body has to come back on stdout. A recreated `stand-proxy` has no ticket cache, so
  the scenario does its own `kinit` rather than expecting one. Switching only the proxy between
  profiles does not work: the metastores keep their own auth and answer `500`, so the whole stand
  has to come up on the other env file.

- **2026-07-31**, jar built from this change and staged into the stand (byte count of
  `/opt/hms-proxy/hms-proxy.jar` compared against the built fat jar, equal), **H12 changed from a
  documented limitation to a passing row** - and the limitation it documented turned out never to
  have existed. The old text said a Hive 4-created Iceberg table is unreadable by the 3.1 line
  because `STORED BY ICEBERG` leaves an abstract `inputFormat`. It does not: the table is created
  with the concrete `HiveIcebergInputFormat`, and both 3.1 engines read it. The old wording
  survived because `--origin hive4` carved those two engines out of the run, so the scenario
  asserted the limitation instead of testing it. With the carve-out gone, two real *write*-side
  defects appeared one after the other, both from the `engine.hive.enabled` fork in Iceberg's
  `HiveTableOperations` (see "H12 in detail"): the proxy's own REST commits stripped the
  Hive-engine descriptor, fixed earlier by `rest-catalog.hive-engine-descriptor`; and then a 3.1
  HiveServer2's own `INSERT` stripped it, which no proxy setting could prevent at the source
  because the flag is read inside that engine's JVM. Fixed by teaching
  `IcebergTablePointerGuard` to keep the descriptor the record holds
  (`routing.iceberg-pointer-guard.hive-engine-descriptor`, new `hive_descriptor_kept` outcome).
  Root-caused rather than guessed: the `hiveEngineEnabled`/`storageDescriptor` fork was read out
  of `iceberg-hive-runtime-1.6.1.jar` with `javap`, the absent `engine.hive.enabled` was read out
  of the table's own `metadata.json` in HDFS, and `hs2-hdp`'s generated `hive-site.xml` was
  checked for the flag before any code was changed.
  Runs, all green: the isolated probe (Hive 4 creates, HDP inserts, descriptor intact afterwards
  and the rows readable through HDP, with the metric showing exactly one `hive_descriptor_kept`);
  `run-iceberg-interop-smoke.sh --prefix hive4` for **all four origins** on plain, and
  `--origin hive4` and `--origin rest` on Kerberos; `run-iceberg-rowlevel-smoke.sh --prefix hive4`
  and `run-iceberg-concurrency-smoke.sh --prefix hive4` on plain as regression cover for a guard
  change that now touches every Iceberg `alter_table`. Unit suite: 714 tests, 0 failures, 0
  skipped, on Java 17.
  The stand deliberately still does **not** set `iceberg.engine.hive.enabled` in either 3.1
  HiveServer2. Setting it there would fix the second defect at its source and make the scenario
  green without the proxy - which is exactly why it is not set: the row would stop testing the
  fix. On a real cluster, setting it is worth doing anyway.
  Not run: the `hdp` and `apache` backends (`--prefix hdp` / `--prefix apache`), and the Kerberos
  column of the row-level and concurrency sections. The proxy change is in the routing path shared
  by all three backends and is covered by unit tests on both runtime profiles, but the other two
  backends were not re-run on the stand for this change.

- **2026-08-03**, jar `1.0.27-d14e85c2`, the SQL layer of the stand driven from repository files for
  the first time. Until now every SQL setting in `smoke-stand/env/*.env` was commented out, so
  `--scenario all` logged "skipping beeline SQL smoke" and `--scenario sql` refused to start;
  sections B and C rested on hand-edited runs nobody could reproduce. Four env files now cover the
  paired topology - `sql.env`, `sql-apache.env` and their `-kerberos` counterparts - and the runner
  selects passes with `HMS_SMOKE_SQL_FRONT_DOORS` and the ACID blocks with
  `HMS_SMOKE_TRANSACTIONAL_SQL_FRONT_DOORS`.
  Four runs, all green: Hortonworks front door over the Hortonworks default catalog and Apache over
  the Apache one, each on the plain and the Kerberos profile. The ACID block ran for the first time
  and was verified rather than inferred - `allocate_table_write_ids` in the proxy log and the
  transactional table created in each pairing - and the Kerberos runs were confirmed Kerberized the
  same way (`security.mode=KERBEROS`, 169 calls from `hive/hs2@SMOKE.LOCAL`, SASL in the log)
  rather than trusted because the profile said so.
  Two failures surfaced along the way, and both turned out to be cross pairings rather than limits
  of the supported layout: C6, an Apache front door over a Hortonworks backend, where the insert
  lands and the stats update is refused for want of a transactional write ID; and C7, a Hortonworks
  front door over an Apache backend, where Hive's own `add_write_notification_log` is refused
  because the backend is not a Hortonworks runtime. C6 was chased to a conclusion: the proxy is not
  losing the field - the two clients issue different RPCs, and the vendor build never calls
  `set_aggr_stats_for` at all. Federation was ruled out by a non-federated database failing
  identically, stand configuration by both HiveServer2 instances carrying the same settings.

## Two caveats on faithfulness

- The Kerberos profile is complete end to end — client → HiveServer2 → proxy → metastores → HDFS,
  with no service falling back to simple auth. But the HDP HiveServer2 only starts with
  `hive.in.test=true`, which is what lets a session switch the engine to `mr`; Hortonworks builds
  without MapReduce. The *metadata* path is unaffected, query execution is not what an HDP cluster
  would do.
- All SQL runs as local MapReduce, so timings and concurrency behaviour say nothing about
  production.
