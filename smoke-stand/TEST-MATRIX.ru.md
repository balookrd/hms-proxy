# Матрица smoke-тестов

Что на этом стенде действительно прогонялось, а что нет. Каждая ✅ ниже наблюдалась на описанной
здесь конфигурации — а не выведена из того, что прошёл похожий случай.

**Конфигурация, на которой проверялось**

| Компонент | Версия / роль |
| --- | --- |
| Прокси | fat jar из `target/`, два front door: 9083 `APACHE_3_1_3`, 9084 `HORTONWORKS_3_1_0_3_1_0_78` |
| `hms-hdp` | standalone-метастор Hortonworks `3.1.0.3.1.0.0-78` — default catalog, владеет ACID/txn-состоянием |
| `hms-apache` | standalone-метастор Apache `3.1.3` — non-default catalog |
| `hs2` | Apache HiveServer2 `3.1.3` → Apache front door |
| `hs2-hdp` | вендорский HDP HiveServer2 `3.1.0.3.1.0.0-78` → Hortonworks front door |
| Хранилище | **два** кластера Apache Hadoop `3.1.3`: `namenode` (каталог `hdp`), `namenode-b` (каталог `apache`) |
| Аутентификация | профиль `plain` (без SASL) и профиль `kerberos` (realm `SMOKE.LOCAL`, один на оба кластера) |

Обозначения: ✅ пройдено · ❌ падает по design · — не прогонялось · n/a неприменимо.

## A. Direct HMS smoke CLI — `--scenario all`

Гоняется через `scripts/run-real-installation-smoke.sh`; оба профиля завершились с
`scenario 'all' completed successfully`.

| # | Сценарий | Какие RPC проходят через прокси | plain | kerberos |
| --- | --- | --- | --- | --- |
| A1 | `txn` | `open_txns` → `allocate_table_write_ids` → `lock` → `check_lock` → `get_valid_write_ids` → `commit_txn` | ✅ | ✅ |
| A2 | Лок non-default каталога | `lock` SHARED_READ + DB + NO_TXN → `check_lock` → `heartbeat` → `unlock` → `abort_txn` | ✅ | ✅ |
| A3 | Партиционный лок | то же, EXCLUSIVE + PARTITION + NO_TXN | ✅ | ✅ |
| A4 | Кросс-каталожный лок | один `lock`, компоненты которого называют два каталога (`--second-db`) | ✅ | ✅ |
| A5 | Notification, позитив | `add_write_notification_log`, HDP front door → HDP backend | ✅ | ✅ |
| A6 | Notification, негатив | тот же вызов против Apache-бэкенда — должен быть отклонён | ✅ отклонён | ✅ отклонён |

## B. SQL через **Apache** HiveServer2 (front door 9083)

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| B1 | Федерация имён — `show databases` → `default`, `apache__default` | ✅ | ✅ |
| B2 | Чтение из обоих каталогов | ✅ | ✅ |
| B3 | DDL + `describe formatted` (location на «своём» HDFS каталога) | ✅ | ✅ |
| B4 | Партиции: create → insert → `show partitions` → rename → count | ✅ | ✅ |
| B5 | Внешняя таблица: create → alter → insert → describe → drop | ✅ | ✅ |
| B6 | `INSERT` через локальный MapReduce | ✅ | ✅ |
| B7 | View + cross-catalog view rewrite | ✅ | ✅ |
| B8 | Кросс-каталожный JOIN в одном statement | ✅ | ✅ |
| B9 | JOIN двух баз одного каталога | ✅ | ✅ |
| B10 | Permanent UDF (`UDFReverse` → `yxorp`) | ✅ | ✅ |

## C. SQL через вендорский **HDP** HiveServer2 (front door 9084)

HDP-клиент не может пользоваться Apache-listener — Thrift не умеет договариваться о версии, —
поэтому только этот путь покрывает Hortonworks front door настоящим клиентом.

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| C1 | Всё из раздела B | ✅ | ✅ |
| C2 | Транзакционные (ACID) таблицы: create → insert → count | ✅ | ✅ |
| C3 | `add_write_notification_log`, отправленный **самим Hive** после ACID-записи, с настоящими delta-путями и контрольными суммами | ✅ | ✅ |
| C4 | `allocate_table_write_ids` / `get_valid_write_ids` через федерацию | ✅ | ✅ |
| C5 | Materialized view с включённым rewrite (`show materialized views` → `Yes`) | ✅ | ✅ |

## D. Два HDFS-кластера

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| D1 | Таблицы двух каталогов ложатся на **разные** namenode | ✅ | ✅ |
| D2 | Физическая проверка: перекрёстных файлов нет | ✅ | ✅ |
| D3 | Запись в оба кластера | ✅ | ✅ |
| D4 | Кросс-кластерный JOIN — одна MapReduce-задача читает с обеих файловых систем | ✅ | ✅ |
| D5 | Неквалифицированный `LOCATION` переписывается на файловую систему каталога-владельца | ✅ | ✅ |
| D6 | `LOCATION`, названный на *другом* кластере, переписывается на свой | ✅ | ✅ |
| D7 | `DROP ... PURGE` удаляет данные на кластере своего каталога | ✅ | ✅ |

## E. Отдельные проверки

| # | Проверка | Результат |
| --- | --- | --- |
| E1 | Guard транзакционного DDL на `create_table_with_environment_context` | ✅ блокирует транзакционную таблицу, обычную пропускает |
| E2 | Readiness-проба не ломает SASL (15 × `/readyz`, следом Kerberos-смоук) | ✅ |
| E3 | `hms_proxy_lock_request_split_total{catalog}` считает расщепления lock-запросов | ✅ |

## G. Iceberg REST catalog front door (host-порт 19183)

Только plain-профиль; Kerberos-профиль держит listener выключенным (SPNEGO покрыт in-JVM
тестом `SpnegoIntegrationTest`). Гоняется через `--scenario rest` curl'ом с хоста;
загружаемая таблица — зарегистрированная вручную `smoke_iceberg_tbl` (см. README стенда).

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| G1 | `GET /v1/config` объявляет `prefix=hdp` (default-каталог) | ✅ | n/a |
| G2 | Листинг и load namespace (`default`) | ✅ | n/a |
| G3 | Листинг таблиц показывает Iceberg-таблицу и прячет обычные Hive-таблицы той же базы | ✅ | n/a |
| G4 | Load таблицы возвращает `metadata-location` и полные метаданные, прочитанные из HDFS самим прокси | ✅ | n/a |
| G5 | Неизвестный prefix → чистый 404 `NoSuchCatalogException` | ✅ | n/a |
| G6 | Неизвестная таблица → чистый 404 | ✅ | n/a |
| G7 | Write-роут (`DELETE` таблицы) отклонён, не-2xx | ✅ | n/a |
| G8 | `GET /v1/config?warehouse=apache` объявляет `prefix=apache` | ✅ | n/a |
| G9 | Неизвестный warehouse (`GET /v1/config?warehouse=no_such_warehouse_smoke`) → чистый 400 | ✅ | n/a |
| G10 | Чистое представление namespace под prefix `apache` показывает `default` без утечки внешних имён вида `apache__*` | ✅ | n/a |
| G11 | Load таблицы под prefix `apache` (`smoke_iceberg_tbl_ap`, второй HDFS-кластер) возвращает `metadata-location` | ✅ | n/a |

## F. Что не покрыто и почему

| Область | Причина |
| --- | --- |
| ACID на non-default каталоге | Прокси отказывает в `allocate_table_write_ids` вне default-каталога **по design** — проходить нечему |
| YARN / Tez, распределённое исполнение | Стенд гоняет только локальный MapReduce; ничего не говорит о поведении прокси под конкурентностью настоящего кластера |
| Ranger, Atlas, HA | Вне области стенда |
| Cross-realm Kerberos trust | Оба кластера намеренно в одном realm; cross-realm проверял бы KDC, а не прокси |
| Конкурентность / нагрузка | Все сценарии однопоточные. Synthetic lock shim в частности выдаёт локи, не проверяя конфликты, поэтому изоляция писателей здесь не проверяется ничем |

## Журнал ревалидаций

Повторные прогоны матрицы после того, как таблица выше была заполнена впервые. Заявлено только
то, что повторный прогон действительно выполнил; строка, не упомянутая здесь, не повторялась —
её ✅ опирается на прежний прогон.

- **2026-07-27**, jar `1.0.4-38128c8b` (ветка `feature/iceberg-rest-fe-phase1`, ребейзнутая на
  `main`; Iceberg REST listener остаётся выключенным, так что проверялся именно Thrift-путь).
  Перепрогнано и зелено: весь раздел A на обоих профилях, разделы B и C на обоих профилях через
  оба HiveServer2 — кроме шагов, которые их env-флаги держат выключенными по умолчанию (B9
  кросс-базовый join, C2/C3 ACID SQL, C5 materialized view). Разделы D и E не повторялись.
  Прогон вскрыл три дефекта стенда/раннера, все починены в `main` в тот же день: SQL-проход
  исчерпывал `server.max-worker-threads=64` (каждый async-exec-поток HiveServer2 держит одно
  соединение к метастору — лимит теперь 256), ассерт B10 полагался на то, что
  `show functions like` найдёт короткое имя, хотя Hive 3.1.3 регистрирует функцию
  квалифицированной, а cleanup-`trap RETURN` раннера срабатывал повторно в объемлющей функции
  после двухпроходного прогона и убивал его под `set -u` уже после всех пройденных проверок.
  Позже в тот же день на plain-профиле был включён Iceberg REST listener ветки и впервые
  прогнан раздел G (`--scenario rest`, затем ещё раз как REST-шаг полностью зелёного
  `--scenario all`).
  В тот же день, после регистрации второй Iceberg-таблицы каталога `apache`
  (`smoke_iceberg_tbl_ap`) на её собственном кластере (`namenode-b`), были прогнаны и новые
  multi-catalog REST-строки (G8-G11) — в тех же прогонах `--scenario rest` и `--scenario all`.

## Две оговорки честности

- Kerberos-профиль полный сквозняком — клиент → HiveServer2 → прокси → метасторы → HDFS, ни один
  сервис не откатывается на simple auth. Но HDP HiveServer2 стартует только с `hive.in.test=true`:
  именно это позволяет сессии переключить движок на `mr`, потому что Hortonworks собирает без
  MapReduce. Путь *метаданных* от этого не страдает, исполнение запросов — не то, что делал бы
  настоящий HDP-кластер.
- Весь SQL идёт локальным MapReduce, поэтому тайминги и поведение под конкурентностью ничего не
  говорят о проде.
