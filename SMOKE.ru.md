**Smoke-тест для Beeline и HMS clients**

Подставь свой `jdbc:hive2://...` и при необходимости `principal=...`.
Предположим:

- separator: `__`
- Hortonworks backend catalog: `hdp`
- Apache backend catalog: `apache`

```sql
!connect jdbc:hive2://proxy-host:10000/default
```

## Test matrix

Используй эту матрицу как короткий operational checklist для smoke-покрытия. Она заранее
показывает, какие комбинации client / front-door / backend / auth должны работать, работать в
degraded-режиме или падать явно, ещё до детальных шагов ниже.

| Client version | Front-door profile | Backend profile | Auth mode | Method families | Expected result |
| --- | --- | --- | --- | --- | --- |
| Beeline / HiveServer2 SQL client | `APACHE_3_1_3` | смешанные `APACHE_3_1_3` + Hortonworks `3.1.0.x` | `NONE` | read, namespace switching, DDL/DML | Должно проходить через один proxy endpoint с корректным cross-catalog routing. |
| Beeline / HiveServer2 SQL client | `APACHE_3_1_3` | смешанные `APACHE_3_1_3` + Hortonworks `3.1.0.x` | `KERBEROS` | read, namespace switching, DDL/DML | То же самое, плюс должен успешно проходить front-door SASL/Kerberos. |
| Beeline / HiveServer2 SQL client | `APACHE_3_1_3` или `HORTONWORKS_*` | смешанные `APACHE_3_1_3` + Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | view, cross-catalog view rewrite, materialized views | Должно проходить при включённом view rewrite; backend без MV support должен падать явно. |
| Direct HMS smoke CLI `txn` | `APACHE_3_1_3` | Hortonworks `3.1.0.x` default catalog | `NONE` | `open_txns`, `allocate_table_write_ids`, `lock`, `check_lock`, `get_valid_write_ids`, `commit_txn` | Должно проходить; это проверка default-catalog txn path для Hortonworks-routed трафика. |
| Direct HMS smoke CLI `txn` | `APACHE_3_1_3` | `APACHE_3_1_3` default catalog | `NONE` | `open_txns`, `allocate_table_write_ids`, `lock`, `check_lock`, `get_valid_write_ids`, `commit_txn` | Должно проходить; это проверка default-catalog txn path для Apache-routed трафика. |
| Direct HMS smoke CLI `txn` | любой | смешанные backend | `KERBEROS` | то же txn family + аутентифицированный фронт | Должно проходить, если корректно настроены Kerberos login и, при необходимости, backend impersonation. |
| Direct HMS smoke CLI `notification` | `HORTONWORKS_*` с standalone jar | Hortonworks `3.1.0.x` default catalog | `NONE` или `KERBEROS` | `add_write_notification_log` | Должно проходить только если и front door, и routed backend имеют совместимый Hortonworks runtime. |
| Direct HMS smoke CLI `notification` | `HORTONWORKS_*` с standalone jar | `APACHE_3_1_3` | `NONE` или `KERBEROS` | `add_write_notification_log` | Должно падать с явной ошибкой уровня `requires a Hortonworks backend runtime`. |
| Любой клиент, использующий id-only txn / lock lifecycle RPC | любой | смешанные backend | `NONE` или `KERBEROS` | `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat` | Это нужно трактовать как default-catalog-only поведение, а не как настоящее per-catalog routing. |

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

**8. Views и materialized views**

Этот блок стоит прогонять с `federation.view-text-rewrite.mode=rewrite`. Если важно, чтобы
оригинальный клиентский SQL оставался видимым в HMS без изменений, дополнительно включи
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

Если backend поддерживает materialized views, дополнительно проверить:

```sql
use hdp__default;

create materialized view if not exists smoke_mv_local as
select * from hdp__default.some_table;

show create table smoke_mv_local;
describe formatted smoke_mv_local;
drop materialized view if exists smoke_mv_local;
```

Ожидание:
- `show create table` / HMS `get_table` возвращают proxy namespace вида `hdp__default`
- при `preserve-original-text=true` `viewOriginalText` остаётся пользовательским, а
  `viewExpandedText` всё ещё корректно маршрутизируется для backend
- cross-catalog ссылки вроде `apache__default.some_table` сохраняются в backend-compatible форме
  и продолжают работать через proxy
- если backend не поддерживает materialized views, ошибка явная, без silent success

**9. Notification / ACID path**

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

**10. Negative check: Hortonworks front -> Apache backend notification path**

Через HMS thrift client отправить `add_write_notification_log` на базу/таблицу, которая
маршрутизируется в Apache backend.

Ожидание:
- proxy возвращает явную ошибку про `requires a Hortonworks backend runtime`
- нет silent success
- в Apache backend не появляется побочный notification traffic

**11. Проверка после mixed runtime переключений**

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
- `Starting HMS proxy`
- `Routing config: defaultCatalog=...`
- `Compatibility config: frontendProfile=..., frontendVersion=...`
- `Backend catalog '...' selected runtimeProfile=... compatibilityProfile=...`
- `backend-request catalog=... method=...`
- повторяющиеся `UNKNOWN_METHOD`
- ошибки `Unsupported Hortonworks frontend method`
- ошибки `requires a Hortonworks backend runtime`
- trace-записи для `add_write_notification_log`, `open_txns`, `commit_txn`, `lock`
