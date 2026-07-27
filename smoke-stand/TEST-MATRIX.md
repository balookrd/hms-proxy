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

## Two caveats on faithfulness

- The Kerberos profile is complete end to end — client → HiveServer2 → proxy → metastores → HDFS,
  with no service falling back to simple auth. But the HDP HiveServer2 only starts with
  `hive.in.test=true`, which is what lets a session switch the engine to `mr`; Hortonworks builds
  without MapReduce. The *metadata* path is unaffected, query execution is not what an HDP cluster
  would do.
- All SQL runs as local MapReduce, so timings and concurrency behaviour say nothing about
  production.
