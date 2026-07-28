# Smoke test matrix

What has actually been run on this stand, and what has not. Every ✅ below was observed on the
configuration described here — not inferred from a similar case passing.

**Configuration under test**

| Component | Version / role |
| --- | --- |
| Proxy | the fat jar from `target/`, two front doors: 9083 `APACHE_3_1_3`, 9084 `HORTONWORKS_3_1_0_3_1_0_78` |
| `hms-hdp` | Hortonworks standalone metastore `3.1.0.3.1.0.0-78` — default catalog, owns ACID/txn state |
| `hms-apache` | Apache standalone metastore `3.1.3` — non-default catalog |
| `hs2` | Apache HiveServer2 `3.1.3` → Apache front door |
| `hs2-hdp` | vendor HDP HiveServer2 `3.1.0.3.1.0.0-78` → Hortonworks front door |
| Storage | **two** Apache Hadoop `3.1.3` clusters: `namenode` (catalog `hdp`), `namenode-b` (catalog `apache`) |
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

Driven by `--scenario rest` with curl from the host (plain) or curl `--negotiate` from inside
`stand-proxy` (kerberos - the KDC and the `proxy` hostname only resolve in-network, and the
container's curl is GSS-capable). The loaded table is the hand-registered `smoke_iceberg_tbl`
(see the stand README). The Kerberos profile carried the listener disabled through phase 5a
because SPNEGO needed a GSS-capable curl inside the network; once that stopped being true the
listener was turned on there too (`rest-catalog.kerberos.principal=HTTP/proxy@SMOKE.LOCAL`,
same keytab as the Thrift front door) and the write round trip, the write gate and an
unauthenticated-request check were run against it - see the 2026-07-28 kerberos entry below.
The read-only rows (G2-G22, G27-G30) have not yet been re-run against the Kerberos profile and
stay `n/a` until they are.

| # | Check | plain | kerberos |
| --- | --- | --- | --- |
| G1 | `GET /v1/config` advertises `prefix=hdp` (the default catalog) | ✅ | ✅ |
| G2 | Namespace list and load (`default`) | ✅ | n/a |
| G3 | Table listing shows the Iceberg table and hides plain Hive tables of the same database | ✅ | n/a |
| G4 | Table load returns `metadata-location` and full metadata read from HDFS by the proxy itself | ✅ | n/a |
| G5 | Unknown prefix → clean 404 `NoSuchCatalogException` | ✅ | n/a |
| G6 | Unknown table → clean 404 | ✅ | n/a |
| G7 | `DELETE` of a non-existent table answers a clean 404, not a silent 2xx | ✅ | n/a |
| G8 | `GET /v1/config?warehouse=apache` advertises `prefix=apache` | ✅ | n/a |
| G9 | Unknown warehouse (`GET /v1/config?warehouse=no_such_warehouse_smoke`) → clean 400 | ✅ | n/a |
| G10 | Clean namespace view under the `apache` prefix lists `default` with no `apache__`-prefixed external names | ✅ | n/a |
| G11 | Table load under the `apache` prefix (`smoke_iceberg_tbl_ap`, second HDFS cluster) returns `metadata-location` | ✅ | n/a |
| G12 | Federated namespace `apache__default` stays visible under the default prefix | ✅ | n/a |
| G13 | Listing and load of `smoke_iceberg_tbl_ap` through the federated `apache__default` name under the default prefix | ✅ | n/a |
| G14 | A default-catalog table under the `apache` prefix → clean 404 | ✅ | n/a |
| G15 | The external name `apache__default` used as a namespace under the `apache` prefix → clean 404 | ✅ | n/a |
| G16 | The second catalog's plain Hive table (`smoke_read_ap`) stays invisible in the `apache` listing | ✅ | n/a |
| G17 | REST metrics (`requests_total`, `listener_info`) visible on the management `/metrics` endpoint | ✅ | n/a |
| G18 | `HEAD` on namespaces/tables answers `204` when present and `404` when absent, including under the non-default `apache` prefix and for a plain Hive table (`smoke_read_hdp`) | ✅ | n/a |
| G19 | Error response for a missing namespace carries the mapped `404`, `type` and `message` but no `"stack":[...]` server trace | ✅ | n/a |
| G20 | An unparseable `POST .../metrics` body answers `400` (`BadRequestException`), not a `500` | ✅ | n/a |
| G21 | `GET /v1/config` and `GET /v1/{prefix}/config` (both resolving to the default catalog) advertise the table-create and table-drop write routes, on top of the namespaces read route | ✅ | n/a |
| G22 | `GET /v1/{second-prefix}/config` (non-default catalog) advertises the namespaces read route and carries no write route - proves discovery advertises the write/read asymmetry, not only the default side | ✅ | n/a |
| G23 | Table write round trip on the default catalog: `POST` create (`200`), `GET` load (`metadata-location` present), `DELETE` drop (`2xx`) | ✅ | ✅ |
| G24 | Direct `POST` create under the non-default `apache` prefix refused with `403` (`ForbiddenException`) | ✅ | ✅ |
| G25 | `POST` create under the federated `apache__default` namespace, reached through the default prefix, refused with `403` - proves the write gate is enforced on the *resolved* catalog, not the request's own prefix | ✅ | ✅ |
| G26 | Real `POST` commit against the just-created table (`assert-table-uuid` requirement + `set-properties` update) answers `200` and the returned `metadata-location` differs from create's - proof a new metadata file was actually written through `HiveTableOperations.commit`, not a silent no-op | ✅ | ✅ |
| G27 | `POST /v1/{prefix}/tables/rename` answers `204`, and `GET` on the new name answers `200` | ✅ | n/a |
| G28 | `POST /v1/{prefix}/transactions/commit` naming a table in the federated `apache__default` namespace refused with `403` | ✅ | n/a |
| G29 | `POST /v1/{prefix}/namespaces` with a federated name (`apache__zzz_smoke`) refused with `403` | ✅ | n/a |
| G30 | `POST /v1/{prefix}/tables/rename` with a federated *destination* namespace (source table still under its current name) refused with `403` - proves the destination side of the gate, not just the source | ✅ | n/a |
| G31 | A request without `--negotiate` is rejected `401` with a `WWW-Authenticate: Negotiate` challenge and an empty body | n/a | ✅ |

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

## Two caveats on faithfulness

- The Kerberos profile is complete end to end — client → HiveServer2 → proxy → metastores → HDFS,
  with no service falling back to simple auth. But the HDP HiveServer2 only starts with
  `hive.in.test=true`, which is what lets a session switch the engine to `mr`; Hortonworks builds
  without MapReduce. The *metadata* path is unaffected, query execution is not what an HDP cluster
  would do.
- All SQL runs as local MapReduce, so timings and concurrency behaviour say nothing about
  production.
