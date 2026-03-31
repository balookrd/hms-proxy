**Beeline Smoke**

Подставь ваш `jdbc:hive2://...` и при необходимости `principal=...`. Предположу separator `__`, каталог Hortonworks backend — `hdp`, Apache backend — `apache`.

```sql
!connect jdbc:hive2://proxy-host:10000/default
```

**1. Базовая проверка фронта**
```sql
set -v;
show databases;
```

Ожидание:
- видны базы в виде `hdp__...` и `apache__...`
- подключение проходит через proxy

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
- маршрутизация остаётся корректной

**5. DDL: Hortonworks backend**
```sql
use hdp__default;

create table if not exists smoke_tbl (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;

alter table smoke_tbl set tblproperties ('smoke'='true');

insert into smoke_tbl partition (p='2026-03-31') values (1, '2026-03-31');

show partitions smoke_tbl;

alter table smoke_tbl partition (p='2026-03-31') rename to partition (p='2026-04-01');

show partitions smoke_tbl;

truncate table smoke_tbl;

drop table smoke_tbl;
```

**6. DDL: Apache backend**
```sql
use apache__default;

create table if not exists smoke_tbl (
  id int,
  ds string
)
partitioned by (p string)
stored as parquet;

alter table smoke_tbl set tblproperties ('smoke'='true');

insert into smoke_tbl partition (p='2026-03-31') values (1, '2026-03-31');

show partitions smoke_tbl;

alter table smoke_tbl partition (p='2026-03-31') rename to partition (p='2026-04-01');

show partitions smoke_tbl;

truncate table smoke_tbl;

drop table smoke_tbl;
```

**7. Негативная mixed-check**
```sql
use hdp__default;
show tables;

use apache__default;
select count(*) from some_table;
```

Ожидание:
- команды идут в нужный backend без ошибок namespace

**8. Notification/ACID path**

Эти проверки лучше делать не только через Beeline, а ещё и прямым HMS thrift client, потому что `add_write_notification_log` не обязательно вызывается SQL-обёртками напрямую.

Важно:
- lifecycle RPC без `dbName`/`fullTableName` (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`) теперь сознательно привязаны к `routing.default-catalog`
- multi-catalog routing для ACID ожидается только там, где namespace можно извлечь из request payload

Проверить для Hortonworks backend:
- `open_txns`
- `allocate_table_write_ids`
- `lock`
- `commit_txn`
- `get_valid_write_ids`
- `add_write_notification_log`

Ожидание:
- `open_txns` / `commit_txn` и другие id-only lifecycle RPC уходят в `routing.default-catalog`
- request-based ACID методы (`allocate_table_write_ids`, `get_valid_write_ids`) маршрутизируются по payload
- `add_write_notification_log` проходит только в каталог на Hortonworks backend
- в логах proxy видны `trace stage=backend-request` / `backend-response` для `add_write_notification_log`

**9. Negative check: Hortonworks front -> Apache backend notification path**

Через HMS thrift client отправить `add_write_notification_log` на базу/таблицу, которая маршрутизируется в Apache backend.

Ожидание:
- proxy возвращает явную ошибку про `requires a Hortonworks backend runtime`
- не происходит silent success
- в Apache backend не появляется побочный notification traffic

**10. Что проверить после mixed runtime переключений**

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
