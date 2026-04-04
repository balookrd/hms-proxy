**Beeline Smoke**

Replace `jdbc:hive2://...` with your actual connection string and add `principal=...` if needed.
Assume:

- separator: `__`
- Hortonworks backend catalog: `hdp`
- Apache backend catalog: `apache`

```sql
!connect jdbc:hive2://proxy-host:10000/default
```

## Test matrix

Use this matrix as the operational checklist for smoke coverage. It makes explicit which client /
front-door / backend / auth combinations are expected to work, degrade, or fail before you start
running the detailed Beeline or direct HMS steps below.

| Client version | Front-door profile | Backend profile | Auth mode | Method families | Expected result |
| --- | --- | --- | --- | --- | --- |
| Beeline / HiveServer2 SQL client | `APACHE_3_1_3` | mixed `APACHE_3_1_3` + Hortonworks `3.1.0.x` | `NONE` | reads, namespace switching, DDL/DML | Should pass through one proxy endpoint with correct cross-catalog routing. |
| Beeline / HiveServer2 SQL client | `APACHE_3_1_3` | mixed `APACHE_3_1_3` + Hortonworks `3.1.0.x` | `KERBEROS` | reads, namespace switching, DDL/DML | Same as above, plus front-door SASL/Kerberos must succeed. |
| Beeline / HiveServer2 SQL client | `APACHE_3_1_3` or `HORTONWORKS_*` | mixed `APACHE_3_1_3` + Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | views, cross-catalog view rewrite, materialized views | Should pass when view rewrite is enabled; unsupported MV backends must fail explicitly. |
| Direct HMS smoke CLI `txn` | `APACHE_3_1_3` | Hortonworks `3.1.0.x` default catalog | `NONE` | `open_txns`, `allocate_table_write_ids`, `lock`, `check_lock`, `get_valid_write_ids`, `commit_txn` | Should pass; confirms default-catalog txn path for Hortonworks-routed traffic. |
| Direct HMS smoke CLI `txn` | `APACHE_3_1_3` | `APACHE_3_1_3` default catalog | `NONE` | `open_txns`, `allocate_table_write_ids`, `lock`, `check_lock`, `get_valid_write_ids`, `commit_txn` | Should pass; confirms default-catalog txn path for Apache-routed traffic. |
| Direct HMS smoke CLI `txn` | any | mixed backends | `KERBEROS` | same txn family plus authenticated front door | Should pass when Kerberos login and, if enabled, backend impersonation are configured correctly. |
| Direct HMS smoke CLI `lock` | `APACHE_3_1_3` | any non-default catalog backend | `NONE` | `open_txns`, `lock`, `check_lock`, `heartbeat`, `unlock`, `abort_txn` with `SHARED_READ` + `DB` + `NO_TXN` | Should pass; confirms the synthetic shim for `CREATE TABLE`-style non-transactional DDL locks. |
| Direct HMS smoke CLI `lock` | `APACHE_3_1_3` | any non-default catalog backend | `NONE` | `open_txns`, `lock`, `check_lock`, `heartbeat`, `unlock`, `abort_txn` with `EXCLUSIVE` + `PARTITION` + `NO_TXN` | Should pass; confirms the synthetic shim for partition rename/drop style non-transactional DDL locks. |
| Direct HMS smoke CLI `notification` | `HORTONWORKS_*` with standalone jar | Hortonworks `3.1.0.x` default catalog | `NONE` or `KERBEROS` | `add_write_notification_log` | Should pass only when both the front door and routed backend expose a compatible Hortonworks runtime. |
| Direct HMS smoke CLI `notification` | `HORTONWORKS_*` with standalone jar | `APACHE_3_1_3` | `NONE` or `KERBEROS` | `add_write_notification_log` | Should fail with an explicit `requires a Hortonworks backend runtime` style error. |
| Any client using id-only txn / lock lifecycle RPCs | any | mixed backends | `NONE` or `KERBEROS` | `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat` | Should be evaluated as default-catalog-only behavior, not true per-catalog fanout routing. |

Practical automation:
- for the Beeline / HS2 blocks below, you can automate them with `scripts/run-real-installation-smoke-simple.sh --scenario sql`
- for the Beeline / HS2 blocks below with Kerberos, use `scripts/run-real-installation-smoke-kerberos.sh --scenario sql`

**1. Basic Front-Door Check**
```sql
set -v;
show databases;
```

Expected:
- databases are visible as `hdp__...` and `apache__...`
- the connection succeeds through the proxy

**2. Read path: Hortonworks backend**
```sql
use hdp__default;
show tables;
describe formatted some_table;
select * from some_table limit 5;
```

**3. Read path: Apache backend**
```sql
use apache__default;
show tables;
describe formatted some_table;
select * from some_table limit 5;
```

**4. Переключение между backend в одной сессии**
```sql
use hdp__default;
show tables;

use apache__default;
show tables;

use hdp__default;
show tables;
```

Expected:
- there is no backend “stickiness”
- routing stays correct

**5. DDL: Hortonworks backend**
```sql
use hdp__default;

-- managed
create table if not exists smoke_managed_tbl (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;

alter table smoke_managed_tbl set tblproperties ('smoke'='true', 'table_kind'='managed');

insert into smoke_managed_tbl partition (p='2026-03-31') values (1, '2026-03-31');

select id, ds, p from smoke_managed_tbl where p='2026-03-31';

show partitions smoke_managed_tbl;

alter table smoke_managed_tbl partition (p='2026-03-31') rename to partition (p='2026-04-01');

show partitions smoke_managed_tbl;

select id, ds, p from smoke_managed_tbl where p='2026-04-01';

drop table smoke_managed_tbl;

-- external
create external table if not exists smoke_external_tbl (
  id int,
  ds string
)
stored as parquet
location '/tmp/hms-proxy-smoke/hdp/external/smoke_external_tbl';

alter table smoke_external_tbl set tblproperties ('smoke'='true', 'table_kind'='external');

insert into smoke_external_tbl values (2, '2026-03-31');

select * from smoke_external_tbl where id=2;

describe formatted smoke_external_tbl;

drop table smoke_external_tbl;

-- transactional=true
create table if not exists smoke_txn_tbl (
  id int,
  ds string
)
clustered by (id) into 1 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true', 'table_kind'='transactional');

insert into smoke_txn_tbl values (1, '2026-03-31');

select * from smoke_txn_tbl where id=1;

drop table smoke_txn_tbl;
```

**6. DDL: Apache backend**
```sql
use apache__default;

-- managed
create table if not exists smoke_managed_tbl (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;

alter table smoke_managed_tbl set tblproperties ('smoke'='true', 'table_kind'='managed');

insert into smoke_managed_tbl partition (p='2026-03-31') values (1, '2026-03-31');

select id, ds, p from smoke_managed_tbl where p='2026-03-31';

show partitions smoke_managed_tbl;

alter table smoke_managed_tbl partition (p='2026-03-31') rename to partition (p='2026-04-01');

show partitions smoke_managed_tbl;

select id, ds, p from smoke_managed_tbl where p='2026-04-01';

drop table smoke_managed_tbl;

-- external
create external table if not exists smoke_external_tbl (
  id int,
  ds string
)
stored as parquet
location '/tmp/hms-proxy-smoke/apache/external/smoke_external_tbl';

alter table smoke_external_tbl set tblproperties ('smoke'='true', 'table_kind'='external');

insert into smoke_external_tbl values (2, '2026-03-31');

select * from smoke_external_tbl where id=2;

describe formatted smoke_external_tbl;

drop table smoke_external_tbl;

-- transactional=true
create table if not exists smoke_txn_tbl (
  id int,
  ds string
)
clustered by (id) into 1 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true', 'table_kind'='transactional');

insert into smoke_txn_tbl values (1, '2026-03-31');

select * from smoke_txn_tbl where id=1;

drop table smoke_txn_tbl;
```

Expected:
- managed DDL/DML works for the routed backend, including create table, insert + select, and partition rename
- on a non-default catalog, `CREATE TABLE` and partition rename exercise the synthetic `NO_TXN`
  lock shim, so failures there should be investigated as lock-routing issues rather than SQL parsing
- `external` tables keep an explicit custom `LOCATION`
- `external` and `transactional='true'` variants also allow insert + select where supported
- `transactional='true'` tables are accepted only where the backend supports ACID table creation
- table type and key properties are visible in `describe formatted`

**7. Mixed Negative Check**
```sql
use hdp__default;
show tables;

use apache__default;
select count(*) from some_table;
```

Expected:
- commands reach the correct backend without namespace errors

**8. Views and Materialized Views**

Run this block with `federation.view-text-rewrite.mode=rewrite`. If you need the original client SQL
to stay byte-for-byte visible through HMS, also set
`federation.view-text-rewrite.preserve-original-text=true`.

```sql
use hdp__default;

create or replace view smoke_view_local as
select * from hdp__default.some_table;

show create table smoke_view_local;
describe formatted smoke_view_local;
select * from smoke_view_local limit 5;

create or replace view smoke_view_cross as
select * from apache__default.some_table;

show create table smoke_view_cross;
select * from smoke_view_cross limit 5;
```

If your backend supports materialized views, also run:

```sql
use hdp__default;

create materialized view if not exists smoke_mv_local as
select * from hdp__default.some_table;

show create table smoke_mv_local;
describe formatted smoke_mv_local;
drop materialized view if exists smoke_mv_local;
```

Expected:
- `show create table` / HMS `get_table` return proxy namespaces like `hdp__default`
- with `preserve-original-text=true`, `viewOriginalText` keeps the client SQL while
  `viewExpandedText` still routes correctly on the backend
- cross-catalog references like `apache__default.some_table` are stored in backend-compatible form
  and stay queryable through the proxy
- if the backend does not support materialized views, the failure is explicit rather than silent

**9. Direct Synthetic Lock Smoke**

These checks complement the Beeline DDL steps above and reproduce the problematic cross-catalog
lock lifecycle directly through HMS thrift. Run them against a catalog that is non-default from the
proxy point of view. In the examples below, assume `apache` is not `routing.default-catalog`.

Practical runner:
- for a simple front door, prefer `scripts/run-real-installation-smoke-simple.sh --scenario locks`
- for a Kerberos front door, prefer `scripts/run-real-installation-smoke-kerberos.sh --scenario locks`
- build the repo and use `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock`
- see the "Manual HMS smoke client" section in [README.md](README.md) for Kerberos launch examples

Create-table style DB lock:

```bash
java -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock \
  --uri thrift://proxy-host:9083 \
  --db apache__default \
  --lock-type SHARED_READ \
  --lock-level DB \
  --operation-type NO_TXN \
  --transactional false
```

Partition rename/drop style lock:

```bash
java -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock \
  --uri thrift://proxy-host:9083 \
  --db apache__default \
  --table smoke_managed_tbl \
  --partition p=2026-04-01 \
  --lock-type EXCLUSIVE \
  --lock-level PARTITION \
  --operation-type NO_TXN \
  --transactional false
```

Expected:
- the client prints `open_txns`, `lock`, `check_lock`, `heartbeat`, `unlock`, and `abort_txn`
- the lock is returned as `ACQUIRED` or at worst `WAITING`, but not with `NoSuchTxnException`
- the second command should be run after the managed-table DDL block created and renamed the partition

**10. Notification/ACID path**

These checks are best done not only through Beeline, but also through a direct HMS thrift client,
because `add_write_notification_log` is not necessarily triggered by SQL wrappers directly.

Practical runner:
- for a simple front door, prefer `scripts/run-real-installation-smoke-simple.sh --scenario all`
- for a Kerberos front door, prefer `scripts/run-real-installation-smoke-kerberos.sh --scenario all`
- set `HMS_SMOKE_TXN_SECONDARY_*` if you want the runner to validate both default Hortonworks and default Apache txn targets
- build the repo and use `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli`
- `txn` mode covers `open_txns` / `allocate_table_write_ids` / `lock` / `check_lock` /
  `get_valid_write_ids` / `commit_txn`
- `notification` mode covers Hortonworks-only `add_write_notification_log`
- see the "Manual HMS smoke client" section in [README.md](README.md) for Kerberos launch examples

Important:
- lifecycle RPCs without `dbName` / `fullTableName`
  (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`)
  are intentionally pinned to `routing.default-catalog`
- multi-catalog ACID routing is expected only where namespace can be extracted from the request payload

Check for the Hortonworks backend:
- `open_txns`
- `allocate_table_write_ids`
- `lock`
- `commit_txn`
- `get_valid_write_ids`
- `add_write_notification_log`

Expected:
- `open_txns` / `commit_txn` and other id-only lifecycle RPCs go to `routing.default-catalog`
- request-based ACID methods (`allocate_table_write_ids`, `get_valid_write_ids`) are routed by payload
- `add_write_notification_log` works only for the Hortonworks backend catalog
- proxy logs contain `trace stage=backend-request` / `backend-response` for `add_write_notification_log`

**11. Negative check: Hortonworks front -> Apache backend notification path**

Send `add_write_notification_log` through an HMS thrift client to a database/table that routes to
the Apache backend.

Expected:
- the proxy returns an explicit `requires a Hortonworks backend runtime` error
- there is no silent success
- no side notification traffic appears on the Apache backend

Practical runner:
- set `HMS_SMOKE_NOTIFICATION_NEGATIVE_DB` and `HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE`
- then run `scripts/run-real-installation-smoke-simple.sh --scenario notification` or
  `scripts/run-real-installation-smoke-kerberos.sh --scenario notification`

**12. Mixed Runtime Switching Check**

In a single client session, run in sequence:
- read/DDL on `hdp__default`
- read/DDL on `apache__default`
- `add_write_notification_log` on `hdp__default`
- another read on `apache__default`

Expected:
- one catalog runtime does not “stick” to another
- namespace rewrite remains correct after notification/ACID calls

**13. What To Watch In Proxy Logs**
Look for:
- `Starting HMS proxy`
- `Routing config: defaultCatalog=...`
- `Compatibility config: frontendProfile=..., frontendVersion=...`
- `Backend catalog '...' selected runtimeProfile=... compatibilityProfile=...`
- `backend-request catalog=... method=...`
- repeated `UNKNOWN_METHOD`
- `Unsupported Hortonworks frontend method` errors
- `requires a Hortonworks backend runtime` errors
- trace entries for `add_write_notification_log`, `open_txns`, `commit_txn`, `lock`
