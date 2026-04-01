**Smoke-тест для Beeline и HMS clients**

Подставь свой `jdbc:hive2://...` и при необходимости `principal=...`.
Предположим:

- separator: `__`
- Hortonworks backend catalog: `hdp`
- Apache backend catalog: `apache`

```sql
!connect jdbc:hive2://proxy-host:10000/default
```

**1. Базовая проверка фронта**

```sql
set -v;
show databases;
```

Ожидание:
- видны базы в формате `hdp__...` и `apache__...`
- подключение идёт через proxy без ошибок

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

Ожидание:
- нет “залипания” одного backend
- routing остаётся корректным

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

Ожидание:
- managed DDL/DML проходит в нужный routed backend, включая insert + select + rename partition
- у `external` таблиц сохраняется явно заданный `LOCATION`
- для `external` и `transactional='true'` тоже есть явная проверка insert + select там, где backend это поддерживает
- таблицы с `transactional='true'` создаются только там, где backend поддерживает ACID
- тип таблицы и ключевые properties видны в `describe formatted`

**7. Mixed negative-check**

```sql
use hdp__default;
show tables;

use apache__default;
select count(*) from some_table;
```

Ожидание:
- команды идут в правильный backend
- namespace не ломается

**8. Notification / ACID path**

Эти проверки лучше делать не только через Beeline, но и прямым HMS thrift client, потому что
`add_write_notification_log` обычно не вызывается SQL-обёртками напрямую.

Практически это можно запускать готовым клиентом из репозитория:
- собрать проект и использовать `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli`
- режим `txn` покрывает `open_txns` / `allocate_table_write_ids` / `lock` / `check_lock` /
  `get_valid_write_ids` / `commit_txn`
- режим `notification` покрывает Hortonworks-only `add_write_notification_log`
- примеры запуска с Kerberos есть в разделе "Ручной HMS smoke client" в [README.ru.md](README.ru.md)

Важно:
- lifecycle RPC без `dbName` / `fullTableName`
  (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`)
  сознательно привязаны к `routing.default-catalog`
- multi-catalog routing для ACID ожидается только там, где namespace можно вытащить из payload

Проверить для Hortonworks backend:
- `open_txns`
- `allocate_table_write_ids`
- `lock`
- `commit_txn`
- `get_valid_write_ids`
- `add_write_notification_log`

Ожидание:
- `open_txns` / `commit_txn` и другие id-only lifecycle RPC идут в `routing.default-catalog`
- request-based ACID методы вроде `allocate_table_write_ids` и `get_valid_write_ids`
  маршрутизируются по payload
- `add_write_notification_log` проходит только в Hortonworks backend
- в логах proxy видны `trace stage=backend-request` / `backend-response` для
  `add_write_notification_log`

**9. Negative check: Hortonworks front -> Apache backend notification path**

Через HMS thrift client отправить `add_write_notification_log` на базу/таблицу, которая
маршрутизируется в Apache backend.

Ожидание:
- proxy возвращает явную ошибку про `requires a Hortonworks backend runtime`
- нет silent success
- в Apache backend не появляется побочный notification traffic

**10. Проверка после mixed runtime переключений**

В одной клиентской сессии последовательно выполнить:
- read/DDL на `hdp__default`
- read/DDL на `apache__default`
- `add_write_notification_log` на `hdp__default`
- повторный read на `apache__default`

Ожидание:
- runtime одного каталога не “залипает” на другой
- namespace rewrite остаётся корректным после notification/ACID вызова

**11. Что смотреть в логах proxy**

Ищи:
- `Detected backend catalog ... metastore version ...`
- `legacy request API compatibility mode`
- `backend-request catalog=... method=...`
- повторяющиеся `UNKNOWN_METHOD`
- ошибки `Unsupported Hortonworks frontend method`
- ошибки `requires a Hortonworks backend runtime`
- trace-записи для `add_write_notification_log`, `open_txns`, `commit_txn`, `lock`
