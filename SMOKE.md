**Beeline Smoke**

Replace `jdbc:hive2://...` with your actual connection string and add `principal=...` if needed.
Assume:

- separator: `__`
- Hortonworks backend catalog: `hdp`
- Apache backend catalog: `apache`

```sql
!connect jdbc:hive2://proxy-host:10000/default
```

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

create table if not exists smoke_managed_tbl (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;

alter table smoke_managed_tbl set tblproperties ('smoke'='true', 'table_kind'='managed');

insert into smoke_managed_tbl partition (p='2026-03-31') values (1, '2026-03-31');

show partitions smoke_managed_tbl;

alter table smoke_managed_tbl partition (p='2026-03-31') rename to partition (p='2026-04-01');

show partitions smoke_managed_tbl;

truncate table smoke_managed_tbl;

drop table smoke_managed_tbl;

create external table if not exists smoke_external_tbl (
  id int,
  ds string
)
stored as parquet
location '/tmp/hms-proxy-smoke/hdp/external/smoke_external_tbl';

alter table smoke_external_tbl set tblproperties ('smoke'='true', 'table_kind'='external');

describe formatted smoke_external_tbl;

drop table smoke_external_tbl;

create table if not exists smoke_txn_tbl (
  id int,
  ds string
)
clustered by (id) into 1 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true', 'table_kind'='transactional');

insert into smoke_txn_tbl values (1, '2026-03-31');

select * from smoke_txn_tbl limit 5;

truncate table smoke_txn_tbl;

drop table smoke_txn_tbl;
```

**6. DDL: Apache backend**
```sql
use apache__default;

create table if not exists smoke_managed_tbl (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;

alter table smoke_managed_tbl set tblproperties ('smoke'='true', 'table_kind'='managed');

insert into smoke_managed_tbl partition (p='2026-03-31') values (1, '2026-03-31');

show partitions smoke_managed_tbl;

alter table smoke_managed_tbl partition (p='2026-03-31') rename to partition (p='2026-04-01');

show partitions smoke_managed_tbl;

truncate table smoke_managed_tbl;

drop table smoke_managed_tbl;

create external table if not exists smoke_external_tbl (
  id int,
  ds string
)
stored as parquet
location '/tmp/hms-proxy-smoke/apache/external/smoke_external_tbl';

alter table smoke_external_tbl set tblproperties ('smoke'='true', 'table_kind'='external');

describe formatted smoke_external_tbl;

drop table smoke_external_tbl;

create table if not exists smoke_txn_tbl (
  id int,
  ds string
)
clustered by (id) into 1 buckets
stored as orc
tblproperties ('transactional'='true', 'smoke'='true', 'table_kind'='transactional');

insert into smoke_txn_tbl values (1, '2026-03-31');

select * from smoke_txn_tbl limit 5;

truncate table smoke_txn_tbl;

drop table smoke_txn_tbl;
```

Expected:
- managed DDL/DML works for the routed backend
- `external` tables keep an explicit custom `LOCATION`
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

**8. Notification/ACID path**

These checks are best done not only through Beeline, but also through a direct HMS thrift client,
because `add_write_notification_log` is not necessarily triggered by SQL wrappers directly.

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

**9. Negative check: Hortonworks front -> Apache backend notification path**

Send `add_write_notification_log` through an HMS thrift client to a database/table that routes to
the Apache backend.

Expected:
- the proxy returns an explicit `requires a Hortonworks backend runtime` error
- there is no silent success
- no side notification traffic appears on the Apache backend

**10. Mixed Runtime Switching Check**

In a single client session, run in sequence:
- read/DDL on `hdp__default`
- read/DDL on `apache__default`
- `add_write_notification_log` on `hdp__default`
- another read on `apache__default`

Expected:
- one catalog runtime does not “stick” to another
- namespace rewrite remains correct after notification/ACID calls

**11. What To Watch In Proxy Logs**
Look for:
- `Detected backend catalog ... metastore version ...`
- `legacy request API compatibility mode`
- `backend-request catalog=... method=...`
- repeated `UNKNOWN_METHOD`
- `Unsupported Hortonworks frontend method` errors
- `requires a Hortonworks backend runtime` errors
- trace entries for `add_write_notification_log`, `open_txns`, `commit_txn`, `lock`
