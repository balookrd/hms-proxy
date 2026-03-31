**Beeline Smoke**

Подставь ваш `jdbc:hive2://...` и при необходимости `principal=...`. Предположу separator `__`, каталог Hortonworks backend — `hdp`, GNU backend — `gnu`.

```sql
!connect jdbc:hive2://proxy-host:10000/default
```

**1. Базовая проверка фронта**
```sql
set -v;
show databases;
```

Ожидание:
- видны базы в виде `hdp__...` и `gnu__...`
- подключение проходит через proxy

**2. Read path: Hortonworks backend**
```sql
use hdp__default;
show tables;
describe formatted some_table;
select * from some_table limit 5;
```

**3. Read path: GNU backend**
```sql
use gnu__default;
show tables;
describe formatted some_table;
select * from some_table limit 5;
```

**4. Переключение между backend в одной сессии**
```sql
use hdp__default;
show tables;

use gnu__default;
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

**6. DDL: GNU backend**
```sql
use gnu__default;

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

use gnu__default;
select count(*) from some_table;
```

Ожидание:
- команды идут в нужный backend без ошибок namespace

**8. Что смотреть в логах proxy**
Ищи:
- `Detected backend catalog ... metastore version ...`
- `legacy request API compatibility mode`
- `backend-request catalog=... method=...`
- повторяющиеся `UNKNOWN_METHOD`
- ошибки `Unsupported Hortonworks frontend method`
