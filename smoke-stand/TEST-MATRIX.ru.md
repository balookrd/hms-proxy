# Матрица smoke-тестов

Что на этом стенде действительно прогонялось, а что нет. Каждая ✅ ниже наблюдалась на описанной
здесь конфигурации — а не выведена из того, что прошёл похожий случай.

**Конфигурация, на которой проверялось**

| Компонент | Версия / роль |
| --- | --- |
| Прокси | fat jar из `target/`, три front door: 9083 `APACHE_3_1_3`, 9084 `HORTONWORKS_3_1_0_3_1_0_78`, 9085 `APACHE_4_1_0` (последний — только там, где его объявляет конфиг) |
| `hms-hdp` | standalone-метастор Hortonworks `3.1.0.3.1.0.0-78` — default catalog в базовом конфиге, владеет ACID/txn-состоянием |
| `hms-apache` | standalone-метастор Apache `3.1.3` — non-default catalog, а под `.env.apache` — default |
| `hms-hive4` | standalone-метастор Apache Hive `4.1.0` (официальный образ) — default catalog под `.env.hive4`, compose-профиль `hive4` |
| `hs2` | Apache HiveServer2 `3.1.3` → Apache front door |
| `hs2-hdp` | вендорский HDP HiveServer2 `3.1.0.3.1.0.0-78` → Hortonworks front door |
| `hs2-hive4` | Apache HiveServer2 `4.1.0` (официальный образ, Tez local mode) → Hive 4 front door, compose-профиль `hive4fe` |
| Хранилище | **два** кластера Apache Hadoop `3.1.3`: `namenode` (каталоги `hdp` и `hive4`), `namenode-b` (каталог `apache`) |
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
| C8 | **Парная топология** — каждый front door со своим метастором в роли default-каталога, чужой при этом удалённый: Hortonworks-фронт, пока default — `hms-hdp`, и Apache-фронт, пока default — `hms-apache` (`.env.apache`). Обе пары проходят секции B и C целиком, **включая ACID-блок**: в логе прокси есть `allocate_table_write_ids`, транзакционная таблица создаётся. В паре с Apache `add_write_notification_log` не вызывается вовсе — поэтому C7 там и не возникает | ✅ обе пары | ✅ обе пары |
| C9 | **Операции, ломающие метаданные**, на managed- и external-таблицах в обоих каталогах: `ALTER TABLE ADD COLUMNS` (значение, записанное через новую колонку, читается обратно), `ALTER TABLE RENAME TO` (таблица отвечает под новым именем), `ALTER TABLE DROP PARTITION` и `TRUNCATE TABLE`. Переименование двигает каталог managed-таблицы, но оставляет location external-таблицы на месте, поэтому сценарий называет переименованную таблицу через `show tables like`, а не вычитывает её из `describe formatted` | — | ✅ обе пары |
| C10 | `TRUNCATE` в **метастор Hortonworks** раньше отклонял сам этот метастор, а не прокси: на позиционный `truncate_table` он вызывал `HdfsAdmin.getEncryptionZoneForPath` и падал с `NoSuchMethodError`, потому что образ работал на hadoop-hdfs 2.2.0 — зон шифрования HDFS до 2.6 не существовало, — тогда как сам jar метастора собран против Hadoop 3.1.1. **Починено сборкой образа на вендорском Hadoop** (`prepare.sh` кладёт HDP 3.1.1 common, common/lib и hdfs-client в `override-hdp`, впереди Maven-набора; Guava исключена — HDP несёт 11.0.2, а метастору нужна 19). Проверено: `TRUNCATE` от Apache-клиента теперь опустошает таблицу с 1 строки до 0. У образа Apache-метастора был ровно тот же дефект, и он вылечен так же — Hadoop 3.1.0 из resolved-набора Apache HiveServer2 (явным списком jar-ов, а не копированием каталога: там же лежит `hive-exec`, которому нельзя затенять проверяемый jar метастора). После пересборки обоих образов пара Apache проходит **с включённым `TRUNCATE`**, а пара Hortonworks по-прежнему проходит — регрессий нет. Осталось поведение клиента, а не classpath: Hortonworks-клиент шлёт пустой список партиций и не усекает ничего, поэтому в его env-файле флаг выключен. Разобрано экспериментом: различается один аргумент. Клиент Apache шлёт `partNames=null` — «усечь таблицу целиком», — поэтому метастор удаляет каталог таблицы и доходит до проверки зоны шифрования, которая не линкуется. Вендорский клиент шлёт `partNames=[]`, пустой список партиций, поэтому удалять метастору нечего и он возвращается, не сделав работы. По дороге отсечены, каждый фиксацией переменной: роль каталога (тот же клиент падает и когда `hdp` — default) и location (в обеих ролях одинаков). **Следствие: `TRUNCATE` через Hortonworks-клиента — no-op**, поэтому прогон, который лишь проверяет отсутствие ошибки, не доказывает ничего. Сценарий теперь утверждает число строк после операции (`truncate_emptied_*`), и на этом пути проверка честно краснеет. Поведением управляет флаг `HMS_SMOKE_SQL_RUN_TRUNCATE`: он включён в `sql-apache-kerberos.env`, где Apache-клиент теперь опустошает таблицу, и выключен в `sql-kerberos.env`, где вендорский клиент прошёл бы, ничего не сделав | — | ✅ воспроизведено |
| C6 | **Кросс-пара, вне парной топологии C8.** ACID — свойство **front door**, а не только каталога: те же `create` и `insert` в транзакционную таблицу здесь проходят, а через Apache front door падают — вставка доходит, а обновление статистики отклоняется с `Cannot change stats state for a transactional table without providing the transactional write state for verification (new write ID -1, valid write IDs null)`, и наружу это выходит падающим `StatsTask`. **Write ID теряет не прокси** — клиенты шлют разные RPC. Вендорская сборка не вызывает `set_aggr_stats_for` вовсе: у неё `get_valid_write_ids` → `alter_table_with_environment_context` → `commit_txn`. Клиент Apache 3.1.3 заканчивает вызовом `set_aggr_stats_for`, в котором транзакционного write state нет, Hortonworks-бэкенд его отклоняет, и клиент откатывает транзакцию. По дороге отсечено: федерация ни при чём (нефедерированная БД `default` падает так же), конфигурация стенда ни при чём (у обоих HiveServer2 одинаковые `hive.support.concurrency`/`hive.txn.manager`), потери полей в прокси нет (`NamespaceInternalizer` копирует структуру через deep copy). Чего этот стенд решить не может: наткнулся ли бы клиент Apache 3.1.3 на то же правило, работая против метастора Apache, — с ним в роли default-каталога запрос умирает раньше, на C7 | ✅ отказ воспроизводится | — |
| C7 | **Кросс-пара, вне парной топологии C8.** Когда default-каталогом стоит **метастор Apache 3.1.3** (`.env.apache`), ACID-пути нет вовсе: Hive сам шлёт `add_write_notification_log` после ACID-записи, а прокси отклоняет Hortonworks-запрос всякий раз, когда бэкенд не является Hortonworks-рантаймом (строка A6 фиксирует этот отказ как правильный). Запрос умирает в `MoveTask` с голым `Internal error processing add_write_notification_log`. Всё, что не ACID, в секциях B и C на этой раскладке проходит | ✅ | — |

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

Гоняется через `--scenario rest` curl'ом с хоста (plain) либо изнутри `stand-proxy`
(kerberos — KDC и hostname `proxy` резолвятся только внутри сети, а curl в контейнере собран с
GSS). Загружаемая таблица — зарегистрированная вручную `smoke_iceberg_tbl` (см. README стенда).
Kerberos-профиль всю фазу 5a держал listener выключенным, потому что SPNEGO требовал
GSS-способный curl внутри сети; как только это перестало быть верным, listener включили и там
тоже (`rest-catalog.kerberos.principal=HTTP/proxy@SMOKE.LOCAL`, тот же keytab, что и у Thrift
front door). С 2026-07-29 kerberos-колонку гоняет сам smoke-скрипт
(`HMS_SMOKE_REST_CURL_OPTS=--negotiate -u :` в `env/kerberos.env`, после kinit внутри
контейнера), так что обе колонки проходят один и тот же набор проверок; вручную остаётся только
G18 (HEAD-запросы, которых в скрипте никогда не было).

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| G1 | `GET /v1/config` объявляет `prefix=hdp` (default-каталог) | ✅ | ✅ |
| G2 | Листинг и load namespace (`default`) | ✅ | ✅ |
| G3 | Листинг таблиц показывает Iceberg-таблицу и прячет обычные Hive-таблицы той же базы | ✅ | ✅ |
| G4 | Load таблицы возвращает `metadata-location` и полные метаданные, прочитанные из HDFS самим прокси | ✅ | ✅ |
| G5 | Неизвестный prefix → чистый 404 `NoSuchCatalogException` | ✅ | ✅ |
| G6 | Неизвестная таблица → чистый 404 | ✅ | ✅ |
| G7 | `DELETE` несуществующей таблицы отвечает чистым 404, а не тихим 2xx | ✅ | ✅ |
| G8 | `GET /v1/config?warehouse=apache` объявляет `prefix=apache` | ✅ | ✅ |
| G9 | Неизвестный warehouse (`GET /v1/config?warehouse=no_such_warehouse_smoke`) → чистый 400 | ✅ | ✅ |
| G10 | Чистое представление namespace под prefix `apache` показывает `default` без утечки внешних имён вида `apache__*` | ✅ | ✅ |
| G11 | Load таблицы под prefix `apache` (`smoke_iceberg_tbl_ap`, второй HDFS-кластер) возвращает `metadata-location` | ✅ | ✅ |
| G12 | Federated namespace `apache__default` остаётся виден под default-prefix | ✅ | ✅ |
| G13 | Листинг и load `smoke_iceberg_tbl_ap` через federated-имя `apache__default` под default-prefix | ✅ | ✅ |
| G14 | Таблица default-каталога под prefix `apache` → чистый 404 | ✅ | ✅ |
| G15 | Внешнее имя `apache__default`, использованное как namespace под prefix `apache` → чистый 404 | ✅ | ✅ |
| G16 | Обычная Hive-таблица второго каталога (`smoke_read_ap`) не видна в листинге под prefix `apache` | ✅ | ✅ |
| G17 | REST-метрики (`requests_total`, `listener_info`) видны на management-endpoint `/metrics` | ✅ | ✅ |
| G18 | `HEAD` на namespace/таблицу отвечает `204`, если объект существует, и `404`, если нет — в том числе под не-default prefix `apache` и для обычной Hive-таблицы (`smoke_read_hdp`) | ✅ | n/a |
| G19 | Error-ответ на отсутствующий namespace несёт смапленные `404`, `type` и `message`, но без `"stack":[...]` server trace | ✅ | ✅ |
| G20 | Нераспарсиваемое тело `POST .../metrics` отвечает `400` (`BadRequestException`), а не `500` | ✅ | ✅ |
| G21 | `GET /v1/config` и `GET /v1/{prefix}/config` (оба резолвятся в default-каталог) объявляют write-роуты create и drop таблицы поверх read-роута namespaces | ✅ | ✅ |
| G22 | `GET /v1/{second-prefix}/config` (non-default каталог) объявляет read-роут namespaces и не несёт ни одного write-роута — доказывает, что discovery объявляет write/read-асимметрию, а не только default-сторону | ✅ | ✅ |
| G23 | Write round trip таблицы на default-каталоге: `POST` create (`200`), `GET` load (`metadata-location` присутствует), `DELETE` drop (`2xx`) | ✅ | ✅ |
| G24 | Прямой `POST` create под non-default prefix `apache` отклонён с `403` (`ForbiddenException`) | ✅ | ✅ |
| G25 | `POST` create под federated-namespace `apache__default`, достигнутым через default-prefix, отклонён с `403` — доказывает, что write gate проверяется на *резолвленном* каталоге, а не на prefix запроса | ✅ | ✅ |
| G26 | Настоящий `POST` commit против только что созданной таблицы (requirement `assert-table-uuid` + update `set-properties`) отвечает `200`, и возвращённый `metadata-location` отличается от того, что дал create — доказательство, что новый metadata-файл действительно записан через `HiveTableOperations.commit`, а не тихий no-op | ✅ | ✅ |
| G27 | `POST /v1/{prefix}/tables/rename` отвечает `204`, а `GET` по новому имени отвечает `200` | ✅ | ✅ |
| G28 | `POST /v1/{prefix}/transactions/commit`, называющий таблицу в federated-namespace `apache__default`, отклонён с `403` | ✅ | ✅ |
| G29 | `POST /v1/{prefix}/namespaces` с federated-именем (`apache__zzz_smoke`) отклонён с `403` | ✅ | ✅ |
| G30 | `POST /v1/{prefix}/tables/rename` с federated destination-namespace (source-таблица ещё под текущим именем) отклонён с `403` — доказывает проверку именно destination-стороны gate, а не только source | ✅ | ✅ |
| G31 | Запрос без `--negotiate` отклоняется `401` с вызовом `WWW-Authenticate: Negotiate` и пустым телом | n/a | ✅ |
| G32 | Namespace DDL round trip: `POST .../namespaces` create (`200`), `GET` load (`200`), `POST .../properties` update (`200`) с последующим `GET`, подтверждающим, что property реально появилось, `DELETE` (`204`), `GET` после этого (`404`) — по-настоящему новое: `RoutingMetaStoreClient` не реализовывал `createDatabase`/`alterDatabase`/`dropDatabase` до этой фазы, так что namespace DDL впервые дошёл до реального metastore | ✅ | ✅ |
| G33 | View write round trip: `POST .../views` create отвечает `200` с реальным `metadata-location`, `GET .../views` листит новый view, `POST .../views/{view}` update (requirement `assert-view-uuid` + `set-properties`) отвечает `200`, и последующий `GET` подтверждает, что property реально появилось, `POST /v1/{prefix}/views/rename` отвечает `204`, view загружается обратно `200` под новым именем, а под старым именем отвечает `404` — именно эта пара доказывает, что rename переместил view, а не скопировал его, `DELETE` отвечает `204` | ✅ | ✅ |
| G34 | `POST /v1/{prefix}/transactions/commit` против только что созданной таблицы: отвечает `204`, и `metadata-location` таблицы после этого отличается от того, что дал create — доказательство, что multi-table commit реально записал новый metadata-файл, а не тихий no-op | ✅ | ✅ |
| G35 | `POST .../views` (CREATE_VIEW, полное валидное тело view) в federated-namespace `apache__default` отклонён с `403` — минимальное тело вместо этого получает `400`, потому что не парсится ещё до того, как gate вообще проверяется, так что `400` здесь означал бы, что тело запроса некорректно, а не что gate пропустил write | ✅ | ✅ |
| G36 | `DELETE .../views/{view}` (DROP_VIEW) под federated-namespace `apache__default` отклонён с `403` | ✅ | ✅ |
| G37 | `DELETE /v1/{prefix}/namespaces/{ns}` (DROP_NAMESPACE) federated-namespace `apache__default` отклонён с `403` | ✅ | ✅ |
| G38 | `POST .../properties` (UPDATE_NAMESPACE) federated-namespace `apache__default` отклонён с `403` | ✅ | ✅ |
| G39 | REGISTER_TABLE round trip: создать таблицу, `DELETE` БЕЗ purge (metadata-файл переживает drop на HDFS, `GET` подтверждает `404`), `POST .../register` регистрирует её заново из этого metadata-файла (`200`, `metadata-location` присутствует), `GET` загружает обратно (`200`), `DELETE` удаляет — последний объявленный write-роут без позитивного доказательства | ✅ | ✅ |
| G40 | `POST .../tables/{table}` (UPDATE_TABLE, per-table commit) под federated-namespace `apache__default` отклонён с `403` — названная таблица не обязана существовать, что доказывает: gate отвечает до lookup | ✅ | ✅ |
| G41 | `DELETE .../tables/{table}` (DROP_TABLE) под federated-namespace `apache__default` отклонён с `403` | ✅ | ✅ |
| G42 | `POST .../register` (REGISTER_TABLE, заведомо фиктивный `metadata-location`) под federated-namespace `apache__default` отклонён с `403` до любой попытки прочитать metadata-файл | ✅ | ✅ |
| G43 | `POST .../views/{view}` (UPDATE_VIEW) под federated-namespace `apache__default` отклонён с `403` | ✅ | ✅ |
| G44 | `POST /v1/{prefix}/views/rename` (RENAME_VIEW) с federated destination-namespace отклонён с `403` — view-аналог G30 | ✅ | ✅ |

С G39-G44 у каждого из тринадцати write-роутов `WriteRouteGate` теперь есть и позитивный round
trip (там, где роут действительно обслуживается), и gate-негатив против federated-namespace.

## H. Iceberg interop через все бэкенды и диалекты front door

Гоняется через `smoke-stand/run-iceberg-interop-smoke.sh` (стенд-локальный: каждый шаг — docker
exec в контейнер соответствующего движка). Одна Iceberg-таблица проходит через **все три диалекта
front door плюс REST**, и весь сценарий повторяется с каждым из трёх метасторов стенда в роли
default-каталога — записи разрешены только туда, поэтому default-каталог и **есть** бэкенд под
тестом:

| Бэкенд под тестом | Runtime-профиль | Хранилище | Как |
| --- | --- | --- | --- |
| Hortonworks `3.1.0.3.1.0.0-78` (`hms-hdp`) | `HORTONWORKS_3_1_0_3_1_0_78` | `namenode` | конфиг по умолчанию, `--prefix hdp` |
| Apache `3.1.3` (`hms-apache`) | `APACHE_3_1_3` | `namenode-b` | `.env.apache`, `--prefix apache` |
| Apache Hive `4.1.0` (`hms-hive4`) | `APACHE_4_1_0` | `namenode` | `.env.hive4`, `--prefix hive4` |

Iceberg REST writer (`smoke-stand/iceberg-rest-writer` — клиентская половина REST-протокола,
которую curl сыграть не может) работает внутри `stand-proxy`; оба HiveServer2 3.1-диалектов несут
`iceberg-hive-runtime` 1.6.1 — последний релиз с Hive 3-рантаймом, в Iceberg 1.7 он удалён, — а у
HiveServer2 Hive 4 (`hs2-hive4`, официальный образ, Tez local mode) поддержка Iceberg встроена.

Каждая ячейка ниже наблюдалась на всех трёх бэкендах, если в строке не сказано иначе.

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| H1 | REST пишет настоящие данные: writer создаёт таблицу через REST front door, пишет Parquet-файлы в HDFS и коммитит их снапшотом через REST; его собственный скан читает 2 строки обратно | ✅ | ✅ |
| H2 | Вендорский HDP HiveServer2 (Hortonworks front door, 9084) читает REST-строки (`count=2`), дописывает одну `INSERT`-ом, читает обратно `count=3` | ✅ | ✅ |
| H3 | Apache HiveServer2 (Apache front door, 9083) дописывает ещё одну и читает обратно `count=4` | ✅ | ✅ |
| H4 | HiveServer2 Hive 4 (**Hive 4 front door, 9085** — диалект `APACHE_4_1_0`, единственный listener, которым может пользоваться Hive 4-клиент) читает всё, что записали два 3.1-движка (`count=4`), дописывает свою строку и читает обратно `count=5` | ✅ | ✅ |
| H5 | Полный REST-скан видит коммиты всех SQL-движков (`rows=5`) — и метаданные, и данные проходят через все четыре пути доступа | ✅ | ✅ |
| H6 | REST `DELETE` удаляет таблицу: `GET` отвечает `404`, `show tables` через SQL её больше не показывает | ✅ | ✅ |
| H7 | Kerberos сквозняком: writer аутентифицирует REST одноразовыми SPNEGO-токенами на каждый запрос (кастомный Iceberg `AuthManager`) и пишет в HDFS как `smoke-user` из keytab; все три SQL-прохода — по SASL | n/a | ✅ |
| H8 | Та же таблица пишется через бэкенд 3.1-линии на втором HDFS-кластере (`--prefix apache`) — именно это ставит `APACHE_3_1_3` на путь REST-записи: до этого runtime-профиля не дотягивается никакая другая раскладка, потому что записи идут только в default-каталог | ✅ | ✅ |

### H9-H12. Какой front door создаёт таблицу (`--origin`)

В строках выше таблицу создаёт REST, а SQL её подхватывает. `--origin` вращает эту роль: каждый
front door по очереди становится тем, кто создаёт и пишет первым, а остальные три меняют то, что
он создал. Таблица ниже заполнена на бэкенде `hive4`; с тех пор как 3.1-DDL говорит `EXTERNAL`
(см. ниже), SQL-инициатор работает и на двух других бэкендах — какие именно комбинации измерены,
перечисляет запись журнала ревалидаций от 2026-08-04:

| # | Инициатор (создаёт + пишет 2 строки) | Кто меняет дальше | plain | kerberos |
| --- | --- | --- | --- | --- |
| H9 | REST front door (Iceberg catalog `createTable`) | HDP, Apache, Hive 4 → 5 строк | ✅ | ✅ |
| H10 | HDP HiveServer2 (`STORED BY 'HiveIcebergStorageHandler'`) | REST, Apache, Hive 4 → 5 строк | ✅ | ✅ |
| H11 | Apache HiveServer2 (тот же DDL) | REST, HDP, Hive 4 → 5 строк | ✅ | ✅ |
| H12 | Hive 4 HiveServer2 (`STORED BY ICEBERG`) | REST, HDP, Apache → 5 строк; держится на двух починках со стороны прокси, см. ниже | ✅ | ✅ |

Каждый участник читает текущий итог **до** своей записи, поэтому каждая передача через границу
front door доказана, а не предположена; финальный круг заставляет всех участников подтвердить
один и тот же счёт.

**Почему в 3.1-DDL стоит `EXTERNAL`.** `sql_create_ddl` создаёт таблицу 3.1-линии как `create
external table ... stored by 'HiveIcebergStorageHandler'`, и это слово несущее. Без него таблица
становится `MANAGED_TABLE` в 3.1-метасторе, а запись в managed не-ACID таблицу под `DbTxnManager`
заставляет Hive взять **EXCLUSIVE**-лок на саму таблицу до конца запроса — тогда как
Iceberg-коммит, которым тот же самый запрос завершается
(`HiveIcebergOutputCommitter.commitJob` → `org.apache.iceberg.hive.MetastoreLock`), просит у
метастора свой EXCLUSIVE-лок на неё же. Запрос попадает во взаимоблокировку с самим собой: четыре
попытки с интервалом в три минуты, потом локальная MapReduce-задача умирает, и beeline сообщает
только `return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask`, а настоящая причина
видна лишь в логе HiveServer2 как `MetastoreLock$WaitingForLockException`.

Измерено в обоих вариантах через `show locks` прямо во время запроса. Managed: транзакция самого
запроса держит `default.<таблица> ACQUIRED EXCLUSIVE`, и Iceberg-лок стоит за ней в очереди, пока
задача не сдастся. External: таблицы в лок-запросе нет вовсе — только плейсхолдер
`_dummy_database`, — а Iceberg-лок возвращается ACQUIRED сразу. Метастор Hive 4 скрывает эту
разницу, переписывая нетранзакционную managed-таблицу во внешнюю: тот же DDL через тот же HDP
HiveServer2 даёт `Table Type: EXTERNAL_TABLE` и `TRANSLATED_TO_EXTERNAL=TRUE`. Поэтому строки
`--origin` месяцами проходили на бэкенде `hive4`, тогда как тот же прогон против `hdp` висел.
Метасторы Hortonworks и Apache 3.1 такой трансляции не делают, и слово приходится писать в самом
DDL. Ничего при этом не теряется: на стенде каждая Iceberg-таблица и так внешняя — REST-фронт
создаёт их такими же. Собственный лок pointer-guard’а в прокси всё это время был ни при чём: он
встаёт в очередь за тем, что держит запрос, сдаётся по своему бюджету в 10 секунд, снимает
собственную заявку и чинит запись без лока.

**H12 подробно: кому позволено сохранить Hive-дескриптор.** Раньше здесь было написано, что
таблицу, созданную Hive 4, не читает 3.1-линия, — из предположения, что `STORED BY ICEBERG`
оставляет в StorageDescriptor абстрактный `inputFormat` `org.apache.hadoop.mapred.FileInputFormat`.
**Это объяснение было неверным, причём неверным так, что сценарий не мог этого заметить**:
`--origin hive4` исключал оба 3.1-движка из прогона и утверждал ограничение вместо того, чтобы
его проверять. Измерено заново 31.07.2026: Hive 4 создаёт таблицу с конкретным
`HiveIcebergInputFormat`, и оба 3.1-движка её читают. Реален же дефект на стороне **записи**, и
их два — в двух разных процессах.

Оба растут из одной развилки в `HiveTableOperations` Iceberg. Каждый коммит перестраивает
StorageDescriptor и пишет одну из двух форм: при включённом Hive-движке — `storage_handler` плюс
конкретные `HiveIcebergInputFormat`/`OutputFormat`/`SerDe`; при выключенном — абстрактные
`FileInputFormat`/`FileOutputFormat`/`LazySimpleSerDe`, а `storage_handler` при этом
*удаляется*. Выбор делается по собственному свойству таблицы `engine.hive.enabled`, а если
таблица его не задаёт — по `iceberg.engine.hive.enabled` в Hadoop-конфигурации того процесса,
который коммитит. Таблица, созданная через `STORED BY ICEBERG` в Hive 4, не задаёт
`engine.hive.enabled` вовсе — это проверено чтением её `metadata.json`, — поэтому каждый
следующий писатель решает этот вопрос за себя:

- **Собственные REST-коммиты прокси** попадали на выключенную сторону: REST front door собирал
  свой Iceberg-клиент без этого флага. Один REST-append переписывал созданную Hive таблицу в
  plain-files-форму, и 3.1-движки переставали её открывать. Починено ключом
  `rest-catalog.hive-engine-descriptor` (по умолчанию `true`), который применяется к копии
  Hadoop `Configuration` каждого каталога в `IcebergRestServices.open`.
- **Собственные коммиты 3.1-HiveServer2** попадают туда же: `iceberg-hive-runtime` 1.6.1 внутри
  `hs2-hdp`/`hs2` читает флаг из *своего* `hive-site.xml`, где его нет, и никакая настройка
  прокси до этой JVM не дотягивается. Поэтому обычный `INSERT` от HDP в созданную Hive 4 таблицу
  ломал дескриптор шагом позже, а несущий его запрос — совершенно законный forward commit, у
  `IcebergTablePointerGuard` не было к нему претензий по указателю. Починено в том же guard: раз
  запись метастора всё равно прочитана, он теперь сохраняет и Hive-дескриптор из неё
  (`routing.iceberg-pointer-guard.hive-engine-descriptor`, по умолчанию `true`, считается как
  outcome `hive_descriptor_kept`). Guard только *сохраняет*: таблице, у которой в записи
  метастора нет storage handler, он его никогда не выдаёт.

Так что прокси здесь вовсе не сторонний наблюдатель: это единственное место, через которое
проходят и REST-писатель, и все SQL-движки, а значит — единственное место, где таблицу,
созданную одним движком, можно защитить от представлений другого движка о том, должен ли Hive
уметь её читать. Прописать `iceberg.engine.hive.enabled=true` в `hive-site.xml` каждого движка
починило бы вторую половину в источнике, и на реальном кластере это стоит сделать; стенд этого
намеренно не делает, чтобы сценарий проверял прокси, а не обходной путь.

### H13-H20. Row-level DML: `DELETE` и `UPDATE`

Всё, что выше, только дописывает строки, поэтому delete-файлов там не возникает вовсе. Этот блок
их создаёт. Гоняется через `smoke-stand/run-iceberg-rowlevel-smoke.sh`. Hive 4 — единственный
движок стенда с нативным row-level DML поверх Iceberg, поэтому пишет он: удаляет и обновляет
строки в v2-таблице, созданной через REST front door. Остальные три front door затем обязаны
прочитать то, что он оставил. Прогнано на бэкенде `hive4`, по разу на каждое значение
`write.delete.mode`/`write.update.mode`:

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| H13 | Hive 4 `DELETE FROM ... WHERE` удаляет строки из v2-таблицы, созданной REST-ом, и REST это видит: его собственный скан отдаёт 3 строки вместо 5 и больше не находит удалённый id | ✅ | ✅ |
| H14 | Hive 4 `UPDATE ... SET` меняет значение на месте, и REST видит новое: по-прежнему 3 строки, `src=updated` даёт ровно 1, `src=rest` — остальные 2 | ✅ | ✅ |
| H15 | `merge-on-read` действительно merge-on-read: после удаления в спланированном скане 1 data-файл **плюс 1 delete-файл** — исходный пятистрочный data-файл не тронут, строки отсеиваются на чтении | ✅ | ✅ |
| H16 | **Оба 3.1-движка корректно читают merge-on-read-результат** — полный скан строк через каждый из них возвращает ровно те строки, что выжили, то есть `iceberg-hive-runtime` 1.6.1 применяет position-deletes | ✅ | ✅ |
| H17 | HDP-движок 3.1 по-прежнему делает `INSERT` в таблицу, которую Hive 4 изменил построчно, и все четыре front door затем сходятся на 4 строках | ✅ | ✅ |
| H18 | Ни один 3.1-движок не умеет row-level DML сам: `DELETE` и `UPDATE` отклоняются на этапе компиляции с `SemanticException [Error 10297]: Attempt to do update or delete on table default.smoke_iceberg_rowlevel that is not transactional`, а содержимое таблицы после этого не меняется | ✅ | ✅ |
| H19 | `copy-on-write` действительно copy-on-write: то же удаление оставляет **0 delete-файлов**, потому что Hive 4 вместо этого переписывает data-файл, и все движки читают тот же результат | ✅ | ✅ |
| H20 | Purge-drop по-прежнему вычищает v2-таблицу с delete-файлами: в каталоге таблицы не выживает ни parquet, ни avro, ни `metadata.json` | ✅ | ✅ |

**Граница проходит по записи, а не по чтению** — так же, как и в H12 выше. HiveServer2 3.1 с
`iceberg-hive-runtime` 1.6.1 планирует скан таблицы format-version 2 с position-deletes и
применяет их; чего он не умеет, так это их *порождать*: storage handler Hive 3 не регистрирует
таблицу как транзакционную, и семантический анализатор останавливает запрос до того, как появится
план. То есть писатель Hive 4 и читатель 3.1 могут работать с одной и той же построчно изменённой
таблицей, а клиент 3.1, который попробует её изменить, падает сразу и заметно, а не дописывает
половину. В отличие от H12, вот здесь решения прокси действительно нет: запрос вообще не доходит
до метастора.

Две вещи сценарий проверяет специально — без них он проходил бы вхолостую:

- Каждая проверка чтения — полный скан строк `select id, src`, никогда не `select count(*)`. Hive
  умеет брать count из Iceberg-сводки, которую держит как статистику таблицы, поэтому читатель, не
  умеющий применять delete-файлы, всё равно назвал бы правильное число.
- Режим сценарий проверяет по фактической форме файлов таблицы, а не принимает на веру как
  настройку. Два значения делают друг друга осмысленными: одна и та же проверка видит 1 delete-файл
  при `merge-on-read` и 0 при `copy-on-write`, так что Hive 4, проигнорировавший свойство, завалил
  бы один из двух прогонов.

Что вскрыла постройка сценария (всё найдено самим сценарием, не ревью):

- Бэкенд-рантайм `APACHE_4_1_0` вообще не мог открыть живое Thrift-соединение: его клиент
  сгенерирован против libthrift 0.16, а fat jar несёт 0.9.3, и юнит-тесты мокали invocation-слой.
  Починено в прокси: изолированный Hive 4-рантайм теперь загружает спутник-jar'ы
  (`libthrift-0.16.0`, `libfb303-0.9.3`, `hive-storage-api-4.1.0`, вендорены в
  `hive-metastore/`) child-first, а `ThriftValueConverter` конвертирует структуры и
  инфраструктурные исключения thrift через границу загрузчиков; закреплено тестом
  `Hive4IsolatedRuntimeTest`.
- **Front door** `APACHE_4_1_0` тоже не мог обслужить запись: Hive 4 добавил четвёртую константу
  `LockType` — `EXCL_WRITE`, которой нет в Apache 3.1.3, внутреннем представлении прокси. При
  конвертации значение исчезало, required-поле не проходило валидацию, и `INSERT` Hive 4-клиента
  получал голое «Internal error processing lock», после чего повторял запрос бесконечно. Починено
  в `Hive4FrontendBridge`: EXCL_WRITE понижается до EXCLUSIVE (никогда до SHARED_WRITE — понижение
  не должно давать больше параллелизма, чем просил клиент); закреплено двумя round-trip тестами в
  `FrontendBridgeThriftSerializationTest`.
- `scheduled_query_poll` отклоняется чистым `UNKNOWN_METHOD` каждые несколько секунд: HiveServer2
  Hive 4 опрашивает scheduled queries — Hive 4-only фичу без соответствия в Apache 3.1.3. Шум в
  логе by design, не падение сценария.
- `DELETE .../tables/{table}?purgeRequested=true` отвечал 500: purge обходит манифесты таблицы
  через Avro, а Maven выбирал avro 1.7.4 (из `hadoop-mapreduce-client-core`, та же глубина
  дерева, объявлен раньше) вместо 1.12.0, против которой собран `iceberg-core`. Починено пином
  avro; сценарий теперь заканчивается настоящим purge и проверяет, что ни один data-, manifest-
  или metadata-файл его не пережил.
- После пересборки стенда JVM могут держать устаревший DNS-резолв и ходить не на тот namenode
  («File does not exist» для существующих файлов или упавшая запись в HDFS сразу после старта);
  лечится перезапуском затронутого контейнера после стабилизации сети. Задевает и прокси, и все
  три HiveServer2, а достаточно `docker compose up --build <service>` — он пересоздаёт всю
  цепочку depends_on вместе с HDFS. Тот же класс stale-session проблем, что уже ловил перепрогон
  2026-07-27.

## I. Изоляция писателей

Write gate пускает записи только в default-каталог — на том основании, что лишь его коммиты
берут настоящий Hive-лок, а остальные обслуживает шим, выдающий локи без проверки конфликтов.
Обе половины этого утверждения теперь закреплены.

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| I1 | Шим выдаёт две конфликтующие EXCLUSIVE-блокировки на одну партицию одновременно — та самая небезопасность, ради сдерживания которой существует write gate; закреплено юнит-тестом, чтобы переход шима к проверке конфликтов был осознанным (`RoutingMetaStoreProxySyntheticReadLocksTest#syntheticShimGrantsConflictingExclusiveLocksOnTheSameObject`) | n/a | n/a |
| I2 | 5 конкурентных REST-писателей дописывают одну таблицу в default-каталоге: все 5 коммитят, в таблице ровно 6 строк (1 базовая + 5) — потерянных обновлений нет | ✅ | ✅ на бэкенде Hive 4 |
| I3 | 8 конкурентных писателей: число строк равно числу писателей, отчитавшихся об успехе, плюс одна базовая, а тот писатель, что отклонён, падает с `CommitFailedException: branch main has changed` — состязание разрешается отказом устаревшему писателю, а не тихой перезаписью | ✅ 7 коммитов, 1 отказ | ✅ на бэкенде Hive 4; сколько писателей отклонено, меняется от прогона к прогону: в одном 7 коммитов и 1 отказ, в другом 8 и ни одного |
| I4 | **Через разные front door**: REST-append'ы и Hive-`INSERT`'ы (Hortonworks front door) коммитят в одну таблицу с пересекающимися окнами коммита | ✅ | ✅ 12/12 на 3.1-бэкенде против 1 потери из 12 до того, как починка стала брать табличный лок Iceberg |
| I5 | **Multi-table транзакция под состязанием**: двухтабличный `POST /v1/{prefix}/transactions/commit`, у которого требование по второй таблице устарело из-за конкурирующего писателя, отклоняется с `409 CommitFailedException: Requirement failed: branch main has changed`, изменение **не остаётся ни на одной** из таблиц, а строки конкурента целы | ✅ | ✅ |
| I6 | Тот же маршрут **не** атомарен, когда падает не требование, а сам коммит: все требования проверяются заранее, после чего таблицы коммитятся по одной без откатов, поэтому сбой на середине оставляет предыдущие таблицы закоммиченными, а прокси отвечает `500 CommitStateUnknownException` (`IcebergRestEndpointIntegrationTest#multiTableTransactionMustNotReportSuccessWhenTheSecondCommitFails`, подтверждено на стенде, на живом метасторе Hive 4, исчерпанием ddl-класса rate-limit'а) | — | ✅ |

Гоняется через `smoke-stand/run-iceberg-concurrency-smoke.sh`: скрипт считает писателей,
завершившихся с кодом 0, и требует, чтобы число строк совпало с ними ровно. Писатель, упавший
громко, — корректное поведение и прогон не валит; писатель, отчитавшийся об успехе без своих
строк, — валит.

Для I4 одного счётчика строк недостаточно: beeline-`INSERT` десятки секунд занят MapReduce до
своего коммита, а REST-append коммитит через секунду после старта, так что одновременный запуск
просто выстраивает их в очередь. Поэтому сценарий шлёт REST-append'ы раундами, пока жив хоть один
SQL-писатель, а затем **проверяет пересечение**: каждый коммит Iceberg заканчивается
`alter_table`, который прокси логирует вместе с потоком — REST-запросы на `hms-proxy-rest-*`,
Thrift на `pool-*-thread-*`, — и два окна обязаны пересечься. Детектор проверен на прогоне, где
пересечения **не** было (REST закончил за 4 с до первого SQL-коммита), и распознаёт это, так что
вхолостую пройти прогон не может.

I5 и I6 отвечают на один вопрос с двух сторон, с которых на него и натыкается клиент. Драйвер —
`smoke-stand/run-iceberg-txn-contention-smoke.sh`, и состязание в нём настоящее, а не подстроенное:
второй писатель дописывает одну из двух таблиц через тот же front door, из-за чего ссылка `main`
этой таблицы уезжает вперёд, а транзакция приходит уже со snapshot id, прочитанным до этого
append'а. Это ровно та форма, которую отправил бы проигравший участник гонки; играть с таймингами
для этого не нужно. Заканчивается сценарий **позитивным контролем**: та же транзакция с актуальным
snapshot id обязана быть принята и применена к обеим таблицам. Без него транзакцию столь же
убедительно отклонили бы некорректное тело, неверный prefix или таблица, недоступная для записи.
Итог: состязание по требованиям — «всё или ничего», а сбой коммита на середине атомарности не даёт,
поэтому читать «транзакция» как «атомарна при любом сбое» клиенту нельзя.

Чего прогон **не** показывает: в логе прокси не было ни `check_lock`, ни `WAITING`, то есть
устаревшего писателя отверг собственный requirement-чек Iceberg по snapshot id ветки, а не
ожидание лока. Hive-лок при этом остаётся важен — именно он делает атомарным окно
«прочитать-сверить-`alter_table`», — но в этом прогоне блокирующий путь не понадобился, чтобы
защитить данные.

### Подробности I4: смешение REST- и SQL-писателей теряет строки

На plain-профиле межпутевой прогон проходил стабильно (13 писателей из 14 коммитят, один
REST-писатель отклонён с «branch main has changed», 14 строк — ровно как надо). На
Kerberos-профиле тот же сценарий **примерно в половине прогонов теряет закоммиченную строку**, и
теряется она на REST-стороне: при 4 REST-писателях в раунде против 2 Hive-`INSERT`'ов прогон
закончился с 14 писателями, отчитавшимися об успехе, и 14 строками вместо 15, а разбор по
маркерам назвал пострадавшего — `baseline, sql901, sql902, w1..w7, w9..w12`, то есть **нет w8**.

Процесс этого писателя завершился с кодом 0, значит `newAppend().commit()` вернул управление
штатно: Iceberg сообщил ему, что коммит состоялся. Коммит, который отчитался об успехе и потом
исчез, — это потеря данных, а не состязание: устаревшего писателя полагается отклонять
`CommitFailedException`, что и происходит с теми, кто здесь честно падает.

Чисто REST-овые прогоны (5 и 8 конкурентных писателей, оба профиля) не потеряли ни строки,
поэтому подозрение на смесь: прокси коммитит через Iceberg 1.9.2, а HiveServer2 — через
`iceberg-hive-runtime` 1.6.1 в собственной JVM, и встречаются они только на локе метастора. Что
именно виновато — лок, который не берёт Hive-сторона, лок, который прокси держит недостаточно
долго, или что-то ещё, — не установлено; с этого и надо начинать. Воспроизведение:

```bash
smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hive4 --writers 4 --sql-writers 2 --sql-engine hdp --kerberos
```

На plain пока не наблюдалось, но в механизме ничто не выглядит специфичным для аутентификации:
керберизованные прогоны просто медленнее, а значит окно шире.

**Причина и насколько помогает фикс.** `INSERT` в HiveServer2 начинается с
`alter_table_with_environment_context` с `alterTableOpType=DROPPROPS`, куда кладётся `Table`,
снятый при компиляции запроса. Метастор применяет эти параметры целиком, поэтому стирается каждый
Iceberg-ключ, который есть в записи и отсутствует в запросе, — в первую очередь
`metadata_location`, но также `table_type`, `storage_handler`, `previous_metadata_location` и
набор `current-snapshot-*`, — а идёт всё это вне Iceberg-лока, так что ничто их не сериализует.
`IcebergTablePointerGuard` теперь сливает такой alter поверх записи, которую метастор держит
сейчас (параметры записи как база, параметры клиента сверху, оба указателя принудительно как в
записи), отличая честный коммит по `previous_metadata_location` (запрос, чья база равна текущему
указателю, двигает таблицу вперёд и проходит без изменений; любой другой несёт устаревшую копию).

**Определение по запросу было no-op; теперь оно идёт по записи метастора.** Проверено по проводу:
`alter_table` от HiveServer2 несёт `params={EXTERNAL, numFiles, numRows, totalSize,
transient_lastDdlTime}` и **никакого `metadata_location`**, поэтому первая версия guard'а, которая
искала устаревший указатель *в запросе*, выходила на первой же проверке. Шесть чистых прогонов,
которые ей записали, не доказывали ничего: WARN, который он пишет при починке указателя, не
появился ни разу, а при наблюдённой частоте потерь один к восьми серия из шести чистых прогонов
случается примерно в 45% случаев и без всякого фикса. Iceberg-ность цели теперь читается из
метастора.

**Измерено — со счётчиком, который придаёт зелёному прогону смысл.** Десять прогонов подряд
командой выше: все зелёные **и** во всех
`hms_proxy_iceberg_pointer_guard_events_total{outcome="repaired"}` вырос ровно на 2 за прогон — по
одному на SQL-писателя, тот самый DROPPROPS-alter, которым открывается `INSERT`. Число строк:
11/11, 15/15, 15/15, 10/10, 11/11, 11/11, 11/11, 10/10, 15/15, 15/15 (строки против 1 baseline +
успешные писатели); в двух прогонах из десяти один REST-писатель получил громкий отказ, что и есть
корректное поведение. `outcome="forward_commit"` — 10–15 за прогон: REST-коммиты, опознанные и не
тронутые. Для сравнения: восемь прогонов, записанных здесь раньше, были прогонами с молча
бездействующим guard'ом, и один из них потерял строку.

Первый же прогон поймал дефект, которого не видели unit-тесты: guard читал запись по «сырому»
имени метода, а у Hive 4 в IDL нет позиционного `get_table`, поэтому все 13 чтений того прогона
упали с `NoSuchMethodException` (`outcome="read_failed"`) и не починили ничего — именно на той
линии бэкендов, чей compare-and-swap guard и использует. Теперь чтение идёт через backend adapter,
который апгрейдит его до `get_table_req`.

**Сколько стоит лишнее чтение.** Тот же стенд, оба HiveServer2, `create table` плюс пять
`INSERT` на каждом — 15 `alter_table` в обоих случаях, ни одного по Iceberg-таблице:

| `table-cache-ttl-ms` | чтений (`not_iceberg`) | без чтения (`cache_suppressed`) | средний `alter_table` |
| --- | --- | --- | --- |
| `30000` (по умолчанию) | 2 | 13 | 11.1 ms (0.166 s / 15) |
| `0` (кэш выключен) | 15 | 0 | 12.4 ms (0.185 s / 15) |

То есть отрицательный кэш убрал 87 % добавленных round trip, а даже когда читает каждый alter,
чтение стоит около 1.3 ms при собственной цене `alter_table` ~11 ms. Iceberg-таблицы не кэшируются
никогда — их указатель обязан читаться заново, — поэтому в прогонах конкурентности выше чтение
происходит на каждом alter тестируемой таблицы.

**Остаток гонки и как он закрыт.** Чтение указателя и применение alter'а были двумя отдельными
вызовами, поэтому коммит, попавший между ними, всё ещё затирался. На Hive 4-бэкендах
`expected_parameter_key`/`expected_parameter_value` превращали это в громкий отказ; линия 3.1 оба
ключа игнорирует, и там окно оставалось открытым. Теперь оно закрыто тем, что починка держит тот
самый табличный лок, который берёт сам Iceberg.

**Что стенд показал про локи ещё до написания кода.** Один SQL-`INSERT` в Iceberg-таблицу на этом
профиле, по trace-логу прокси:

| время | вызов | лок |
| --- | --- | --- |
| `08,045` | Hive берёт лок под собственный txn 957 | `LockComponent(db=_dummy_database, table=_dummy_table)` и больше ничего |
| `08,249` | тот самый `DROPPROPS`-alter, который чинит guard | никакого лока на таблице не держится |
| `12,982` | коммит Iceberg внутри HiveServer2 берёт свой лок | `LockRequest(txnid=0, components=[LockComponent(db=default, table=<таблица>)])` |
| `13,033` | `alter_table` этого коммита | **внутри** этого лока |
| `13,097` | `unlock` | лок держали 115 ms |

Два факта определили решение. Hive **не** берёт лок на целевую таблицу `INSERT`'а, поэтому лок,
взятый guard'ом при обслуживании alter'а этого же `INSERT`'а, не встанет в очередь за statement'ом,
который он обслуживает. А честный коммит Iceberg шлёт свой `alter_table` **изнутри** табличного
лока, поэтому запрос этого лока до решения о том, что за alter пришёл, заблокировался бы на локе,
который держит вызывающий, ждущий ответа, — самоблокировка на каждом честном коммите. Поэтому
guard сначала читает без лока и берёт лок **только чтобы починить**, затем перечитывает под локом и
сливает поверх найденного. Форма запроса скопирована с `org.apache.iceberg.hive.MetastoreLock`,
одинаковой в Iceberg 1.6.1 (внутри HiveServer2) и 1.9.2 (REST-путь прокси): один компонент
EXCLUSIVE уровня таблицы с backend-именем БД, без `txnid`.

**Измерено — и до, и после.** Двенадцать прогонов команды выше с `--prefix hdp` (3.1-бэкенд, где
метастор игнорирует compare-and-swap), сначала на неизменённом jar:

| | прогонов | потерь | `repaired` | под локом |
| --- | --- | --- | --- | --- |
| до (guard без лока) | 12 | **1** — в прогоне 12 было 10 строк на 10 успешных писателей, отсутствовал маркер `sql901` | 2 за прогон | n/a |
| после (guard держит лок) | 12 | **0** — во всех прогонах строки сошлись с успешными писателями | 2 за прогон | 2 за прогон |

Частоту потерь на линии 3.1 до этого не измеряли ни разу — все прежние цифры сняты на `hive4`, где
compare-and-swap уже превращает потерянное обновление в громкий отказ. За двенадцать прогонов после
изменения `repair_locked` в точности равнялся `repaired` (по 24), а `repair_lock_timeout`,
`repair_lock_failed` и `lock_release_failed` остались нулевыми: каждая починка была атомарной и ни
один лок не остался висеть. Последнее на этом стенде важно особенно: его метастор работает с
`metastore.compactor.initiator.on=false` и без housekeeping-потоков, поэтому утёкший лок никогда не
был бы собран и заблокировал бы все последующие коммиты по таблице.

**Сколько стоит лок.** Секции B и C через оба HiveServer2 и отдельно пять SQL-`INSERT`'ов в одну
Iceberg-таблицу (каждый — одна починка плюс один forward-коммит):

| нагрузка | `lock-enabled` | `alter_table` | среднее | взято локов |
| --- | --- | --- | --- | --- |
| секции B + C | `true` | 14 | 20.7 ms | **0** |
| секции B + C | `false` | 14 | 13.9 ms | 0 |
| 5 `INSERT`'ов, Iceberg-таблица | `true` | 10 | 14.7 ms | 5 (`repair_locked`) |
| 5 `INSERT`'ов, Iceberg-таблица | `false` | 10 | 15.7 ms | 0 (`repair_lock_skipped`) |

SQL-секции **не берут лок вообще** ни в одной конфигурации — ни одна их таблица не Iceberg, поэтому
починка не срабатывает, — а значит разрыв 20.7 против 13.9 ms есть чистый разброс между прогонами и
заодно предел разрешения этого измерения: около 7 ms на 14 наблюдениях. На пути, где лок всё же
берётся, три добавленных RPC (`lock`, второе `get_table`, `unlock`) укладываются в тот же шум —
прогоны с локом вышли на 1 ms *быстрее*, чем без него, при пяти починках в каждом.

**Что осталось открытым.** Лок, не выданный за `lock-acquire-timeout-ms` (по умолчанию 10 s),
оставляет починку без защиты, а не отменяет запись; а бэкенд, чей ACID-housekeeping действительно
собирает просроченные локи, может собрать наш из-под `alter_table`, который занял больше
`hive.txn.timeout`. И то и другое считается счётчиками (`repair_lock_timeout` и WARN рядом с ним), а
не объявляется невозможным; в этих прогонах не случилось ни разу.

## F. Что не покрыто и почему

| Область | Причина |
| --- | --- |
| ACID на non-default каталоге | Прокси отказывает в `allocate_table_write_ids` вне default-каталога **по design** — проходить нечему |
| YARN / Tez, распределённое исполнение | Стенд гоняет только локальный MapReduce; ничего не говорит о поведении прокси под конкурентностью настоящего кластера |
| Ranger, Atlas, HA | Вне области стенда |
| Cross-realm Kerberos trust | Оба кластера намеренно в одном realm; cross-realm проверял бы KDC, а не прокси |
| Длительная нагрузка | Секция I покрывает конкурентные REST-коммиты в одну таблицу (I2, I3), смешение REST- и SQL-писателей через разные front door (I4) и двухтабличную транзакцию под состязанием (I5, I6). Не покрыта длительность: каждый прогон — всплеск из горстки писателей, а не постоянная нагрузка, и ни throughput, ни latency под ней не измеряются |
| `CTAS`, `INSERT OVERWRITE`, `LOAD DATA`, `MSCK REPAIR`, конвертация managed↔external | Через прокси не гонялись ни разу. C9 покрывает `ADD COLUMNS`, `RENAME TO`, `DROP PARTITION` и `TRUNCATE`; это следующий слой |
| Партиционированные Iceberg-таблицы, эволюция схемы, `MERGE INTO` | H13-H20 покрывают row-level `DELETE`/`UPDATE` на непартиционированной таблице с фиксированной схемой. Partition spec (и его эволюция), добавление/переименование/удаление колонок и `MERGE INTO` через прокси не гонялись ни разу |

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
  Дополнительный прогон в тот же день добавил и прошёл строки G12-G16 про федерацию и изоляцию:
  federated-имя под default-prefix (включая листинг и load) и чистые 404 на каждую
  кросс-каталожную форму.
  Ещё позже jar `1.0.20-eec20f1a` добавил строку G17: с `HMS_SMOKE_REST_METRICS_URL`, указывающим
  на management-endpoint стенда, оба прогона — `--scenario rest` и `--scenario all` — забрали его
  curl'ом и подтвердили, что серии `hms_proxy_rest_requests_total` и `hms_proxy_rest_listener_info`
  присутствуют и заполнены после того, как отработали REST-проверки.
  Ещё позже jar `1.0.23-613b7a1e` (апгрейд на Iceberg 1.9.2, Jackson запинен на `2.18.3`)
  перепрогнал разделы A-D и G и получил зелёный результат; SQL-слой через оба HiveServer2
  сыграл роль детектора Jackson-регрессии для этого пина.
  Ещё позже jar `1.0.33-01704804` (укрепление: error-ответы без stack trace, 400 на
  нераспарсиваемое тело, объявление endpoint'ов) добавил строки G19-G21 и перепрогнал
  `--scenario rest` и `--scenario all` — оба зелёные; `GET /v1/config` и `GET /v1/apache/config`
  забраны curl'ом, и оба несли девятиэлементный список `endpoints`, а `docker logs stand-proxy`
  не показал WARN-шума `stream closed` от HEAD-проверок из G18.
  Ещё позже jar `1.0.34-5397bb81` укрепил проверку строки G21: раньше раннер лишь делал `grep`
  на присутствие ключа `"endpoints"`, что не отличает read-only листинг от такого же листинга
  с добавленным write-роутом. Теперь для `GET /v1/config` и `GET /v1/{prefix}/config` проверяется
  и наличие read-записи `GET /v1/{prefix}/namespaces`, и отсутствие любой записи
  `POST /v1/{prefix}/namespaces` или `DELETE`. `--scenario rest` перепрогнан зелёным на
  пересобранном jar'е; то, что укреплённая проверка действительно различает случаи, подтверждено
  временной подменой ожидаемого имени роута на несуществующее — раннер упал с сообщением
  "config does not advertise the namespaces read route", после чего подмена была отменена.
  Ещё позже jar `1.0.41-931b78d4` (phase 5a: write-запросы к таблицам для default-каталога,
  write gate и асимметричное объявление endpoint'ов; поверх легли фикс выравнивания версий
  `hadoop-hdfs`/`hadoop-common` и расширенный catch-all `Throwable` в `IcebergHttpHandler`)
  добавил строки G22-G25 и обновил G7, G21. `--scenario rest` и `--scenario all` оба
  перепрогнаны зелёными: подтверждено, что `GET /v1/config` и `GET /v1/{prefix}/config`
  (default-каталог) несут write-роуты create и drop таблицы; подтверждено, что
  `GET /v1/apache/config` не несёт ни одного. Таблица, созданная через
  `POST /v1/hdp/namespaces/default/tables`, загрузилась обратно с `metadata-location` и
  удалилась `204`; прямой create под `/v1/apache/namespaces/default/tables` и create под
  `/v1/hdp/namespaces/apache__default/tables` оба ответили `403`. SQL-слой (разделы B и C,
  оба HiveServer2) перепрогнан как регрессионная проверка на изменение Hadoop-зависимостей,
  раз write-запросы к таблицам и собственные ACID-коммиты Hive теперь идут по одному и тому же
  lock-пути; прошёл, при этом `stand-hs2-hdp` пришлось сначала перезапустить (его сессия
  HiveServer2 протухла после пересборки стенда — свежая сессия открылась штатно против того же,
  иначе не тронутого состояния HDFS), а для прохода через Hortonworks понадобился
  `HMS_SMOKE_SQL_HDP_SESSION_INIT=set hive.execution.engine=mr;`, как документировано в
  `smoke-stand/env/simple.env`.

- **2026-07-28**, jar `1.0.43-c4685ef7` (на стенде не менялся; новые проверки добавлены только
  в smoke-скрипт). Добавлены строки G26-G30: write round trip теперь включает НАСТОЯЩИЙ commit
  против только что созданной таблицы и rename round trip, а не только create/load/drop, а
  негативы gate теперь покрывают COMMIT_TRANSACTION, CREATE_NAMESPACE и rename с federated
  destination — поверх уже существующей пары CREATE_TABLE. COMMIT_TRANSACTION в частности был
  критическим обходом, найденным в ходе этой фазы, и до сих пор был закрыт только unit-тестами.
  `--scenario rest` и `--scenario all` оба перепрогнаны зелёными: `metadata-location` из ответа
  create (оканчивающийся на `00000-...`) отличался от `metadata-location` из ответа commit
  (`00001-...`), переименованная таблица загрузилась обратно с `200`, все три новых негатива
  ответили `403`. Проверка G26 доказала свою различающую способность: она была временно изменена
  так, чтобы требовать равенства `metadata-location` commit'а и create (то есть утверждать
  no-op commit); раннер упал с сообщением "did not write a new metadata file", подтвердив, что
  проверка ловит тихо не сработавший commit; проверка была восстановлена, оба сценария
  перепрогнаны зелёными.

- **2026-07-28** (вторая запись), Iceberg REST listener впервые включён на Kerberos-профиле:
  у KDC появился принципал `HTTP/proxy@SMOKE.LOCAL` в том же keytab, которым уже пользуется
  Thrift front door, а `hms-proxy-kerberos.properties` получил блок `rest-catalog.*`,
  указывающий на него, на том же порту 19183, что и plain-профиль. Подъём стенда в таком виде
  вскрыл настоящий баг, а не просто отсутствующую строку конфига: `IcebergRestService` строил
  собственную голую `Configuration` вместо того, чтобы переиспользовать Kerberos-осведомлённый
  `HiveConf` каталога, поэтому любой REST-write падал с "Failed to specify server's Kerberos
  principal name" сразу после того, как RPC до NameNode доходил; починено протягиванием
  `CatalogBackend.hiveConf()` через `IcebergRestServices.open(...)`. Следом обнаружился второй,
  специфичный только для стенда пробел — уже после того, как сам RPC к NameNode заработал:
  в per-catalog Hadoop-конфиге не хватало `dfs.data.transfer.protection`, так что настоящая
  запись блока на datanode при create рвала соединение ("could only be written to 0 of the 1
  minReplication nodes"), хотя чисто NameNode-овый RPC (существующий delete в purge-пути) в
  этом ключе никогда не нуждался; добавлены `catalog.hdp.conf.dfs.data.transfer.protection=authentication`
  и та же настройка для `catalog.apache` в `hms-proxy-kerberos.properties`, в соответствии с тем,
  что `hdfs/hadoop-kerberos*.env` уже требует от datanode'ов. После обоих исправлений сначала
  перепрогнан `docker exec stand-proxy /opt/hms-proxy/scripts/run-real-installation-smoke-kerberos.sh
  --scenario all`, чтобы подтвердить, что апгрейд Hadoop-зависимости, вместе с которым приехала
  REST-фича (`hadoop-hdfs` 2.2.0 -> 2.6.0), не сломал уже существующие керберизованные
  Thrift/lock-пути — прогон завершился `scenario 'all' completed successfully`
  (`TApplicationException` у негативной notification-проверки — задокументированное поведение
  libthrift 0.9.3 для RPC без объявленных исключений, а не провал). Затем, изнутри `stand-proxy`
  после `kinit -kt smoke-user.keytab`, curl с `--negotiate` прогнал строки G1, G23-G26 и новую
  G31 (ниже): неаутентифицированный запрос получил чистый `401`/`WWW-Authenticate: Negotiate`;
  `GET /v1/config` объявил `prefix=hdp` вместе с write-роутами; таблица создана (`200`),
  загружена обратно (`200`), закоммичена по-настоящему (`200`, `metadata-location` сместился с
  файла `00000-...` на `00001-...`), отклонена с `403` и напрямую под prefix `apache`, и через
  federated-namespace `apache__default` под default-prefix, и удалена (`204`).
  `docker logs stand-proxy` показал, что `lock`/`unlock` create и commit прошли через
  `catalog=hdp, backend=hdp` с небольшими последовательными lock ID (387, 388 — схема настоящего
  бэкенда, а не synthetic-shim'а), а `logs/hms-proxy-audit.log` нёс
  `"authenticatedUser":"smoke-user@SMOKE.LOCAL"` в каждой из этих записей. Остальные read-only
  строки Kerberos-колонки (G2-G22, G27-G30) не перепрогонялись и остаются `n/a`.

- **2026-07-28** (третья запись), jar `1.0.49-2b778592` (фаза 5b: namespace DDL в
  `RoutingMetaStoreClient` и объявление полного write-роута в `GET /v1/config`). До этого прогона
  стенд ещё стоял на jar'е до фазы, и прямая проверка показала, что `POST
  /v1/{prefix}/namespaces` отвечает `406` ("does not support `IMetaStoreClient.createDatabase`")
  — namespace DDL ни разу ещё не проверялся против настоящего metastore. Добавлены строки
  G32-G34 для трёх новых round trip'ов (namespace DDL, view write, transaction commit через
  `POST /v1/{prefix}/transactions/commit`); роут per-table commit (G26) уже был покрыт и остался
  зелёным, эта фаза его не затронула.
  После пересборки fat jar и рестейджа (`./prepare.sh && docker compose up -d --build`,
  plain-профиль) `--scenario rest` и `--scenario all` оба перепрогнаны зелёными, на этот раз
  реально прогоняя namespace DDL впервые: `POST /v1/hdp/namespaces` создал `smoke_rest_ns`
  (`200`), `GET` загрузил его обратно, `POST .../properties` выставил `smoke=yes` (`200`), и
  последующий `GET` подтвердил, что property реально появилось, `DELETE` отвечал `204`, а
  финальный `GET` — `404`. View round trip создал `smoke_rest_view` (`200`, реальный
  `metadata-location`), листнул его и удалил (`204`). Transaction round trip создал таблицу,
  закоммитил её через `POST /v1/hdp/transactions/commit` (`204`) и подтвердил, что
  `metadata-location` таблицы при перезагрузке сместился с файла `00000-...` на `00001-...` —
  ручные curl round trip'ы против работающего стенда зафиксировали те же verbatim-ответы вне
  smoke-скрипта, для протокола.
  Шаг 4 задачи доказал, что новая transaction-проверка реально различающая: проверка была
  временно инвертирована — потребовать, чтобы `metadata-location` НЕ менялся, `--scenario rest`
  перепрогнан и упал с "did not write a new metadata file: metadata-location is still
  '...00001-...'", как и ожидалось, затем проверка восстановлена и оба сценария (`rest` и `all`)
  перепрогнаны зелёными.
  Затем стенд переключён на Kerberos-профиль
  (`docker compose --env-file .env.kerberos --profile kerberos up -d --build`), и изнутри
  `stand-proxy` после `kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL` curl с
  `--negotiate` вручную прогнал G32 (namespace DDL) и G33 (view write) — именно так задача и
  ограничила Kerberos-перепрогон. Оба прошли идентично plain-профилю — те же статусы, тот же
  эффект, — а `hms-proxy-audit.log` показал настоящие записи `create_database`/`alter_database`/
  `drop_database` с `"authenticatedUser":"smoke-user@SMOKE.LOCAL"`, подтверждая, что namespace
  DDL под Kerberos тоже дошёл до реального HDP-бэкенда. G34 (transaction commit) под Kerberos не
  перепрогонялся и остаётся `n/a` — в рамках заявленного объёма задачи.
  Ещё позже (тот же jar, изменение только в скрипте, снова на plain-профиле): view round trip
  (G33) расширен, чтобы прогнать два объявленных view-роута, которые он до сих пор ни разу не
  прогонял, — update (requirement `assert-view-uuid`, `POST .../views/{view}`) и rename (`POST
  /v1/{prefix}/views/rename`), — и добавлены ещё четыре негатива `WriteRouteGate` (G35-G38:
  CREATE_VIEW, DROP_VIEW, DROP_NAMESPACE и UPDATE_NAMESPACE — все против federated-namespace
  `apache__default` под default-prefix). `--scenario rest` и `--scenario all` оба перепрогнаны
  зелёными: перезагрузка view после update подтвердила, что `"smoke":"updated"` реально
  закрепилось, rename ответил `204`, view загрузилось обратно `200` под новым именем, а под
  старым именем ответило `404`, и все четыре новых негатива ответили `403` (у CREATE_VIEW —
  с полным валидным телом view, поскольку заглушка ранее отвечала `400` ещё до того, как gate
  вообще был достигнут). Новая проверка эффекта rename доказала свою различающую способность:
  ожидаемый статус был временно перевёрнут с `404` на `200` (то есть утверждалось, что старое
  имя view остаётся доступным после rename); перепрогон упал — старое имя по-прежнему честно
  отвечало `404`, что инвертированная проверка теперь отвергала, — подтвердив, что проверка
  поймает rename, который копирует view вместо того, чтобы его переместить; проверка
  восстановлена, оба сценария (`rest` и `all`) перепрогнаны зелёными.

- **2026-07-29**, jar `1.0.4-14af4def` (`main` после мержа; включает fail-closed-ужесточение
  gate для нерезолвящихся namespace из `5f84d4e`). Smoke-скрипт получил шесть проверок,
  замкнувших покрытие write-поверхности: REGISTER_TABLE round trip (G39: create, drop без purge,
  повторная регистрация из пережившего drop metadata-файла, load обратно, drop) и по одному
  gate-негативу на каждый ещё не покрытый write-роут (G40-G44: UPDATE_TABLE, DROP_TABLE,
  REGISTER_TABLE, UPDATE_VIEW и RENAME_VIEW с federated destination) — теперь у каждого из
  тринадцати гейтуемых write-роутов есть и позитив, и негатив. На plain-профиле `--scenario all`
  прошёл зелёным дважды (до и после SPNEGO-рефакторинга ниже), `--scenario rest` — зелёным между
  ними; проверка register доказала свою различающую способность: ожидаемый статус временно
  заменён с `200` на `403` — прогон упал с "REST register ... returned HTTP 200" и полным телом
  метаданных, прочитанным обратно из HDFS, что подтвердило и что проверка кусается, и что
  register действительно работает; проверка восстановлена, сценарий перепрогнан зелёным.
  В тот же день REST-смоук получил `HMS_SMOKE_REST_CURL_OPTS` (дополнительные опции curl для
  каждого REST-запроса, например `--negotiate -u :`) плюс автоматизированную версию G31: когда
  опции заданы, запрос БЕЗ них обязан быть отклонён `401` с вызовом `WWW-Authenticate:
  Negotiate` и пустым телом. `env/kerberos.env` получил полный REST-блок (внутрисетевой URL
  `http://proxy:9183`, оба prefix, write-таблица/namespace/view и management-метрики
  `http://proxy:9090/metrics`), так что Kerberos-колонку REST теперь гоняет сам скрипт, а не
  набранный вручную curl. Затем стенд переключён на Kerberos-профиль, и
  `docker exec stand-proxy /opt/hms-proxy/scripts/run-real-installation-smoke-kerberos.sh
  --env-file /opt/hms-proxy/smoke-env/kerberos.env --scenario all` (после kinit и `docker cp`
  обновлённых `scripts/` и env-файла) завершился `scenario 'all' completed successfully` —
  первый скриптовый полный REST-проход под Kerberos. Этот прогон перевёл kerberos-колонку
  G2-G17, G19-G22, G27-G30 и G34-G38 из `n/a` в наблюдённо-зелёную и покрыл новые G39-G44 на
  обоих профилях; G18 (HEAD-запросы) остаётся ручной и под Kerberos сохраняет `n/a`. Разделы
  B-D (SQL/HDFS-слои) не перепрогонялись — изменения касаются только REST-смоука, Java-дельта
  jar'а с последнего полного SQL-прохода — ужесточение write gate, а CLI-сценарии раздела A
  (txn, локи, notification) перепрогнаны зелёными на обоих профилях в составе двух проходов
  `--scenario all`.

- **2026-07-29** (вторая запись), добавлена секция H: Iceberg interop-сценарий поверх бэкенда
  Hive 4.1.0, прогнан зелёным на обоих профилях в день постройки. Новые части стенда: контейнер
  `hms-hive4` (официальный `apache/hive:4.1.0`, тонкая обёртка в `smoke-stand/hms-hive4/` —
  собственный conf-symlink-механизм официального образа молча не работает, потому что в образе
  нет `find`, так что обёртка пишет конфиги напрямую), конфиги
  `hms-proxy-hive4[-kerberos].properties` (default-каталог `hive4`,
  `runtime-profile=APACHE_4_1_0`), клиент `iceberg-rest-writer` (Iceberg 1.9.2, Parquet в HDFS +
  REST-коммиты, SPNEGO на каждый запрос через кастомный `AuthManager` под Kerberos),
  `iceberg-hive-runtime` 1.6.1 в обоих образах HiveServer2 и принципал
  `hive/hms-hive4@SMOKE.LOCAL` в KDC. Фикс прокси, который вынудил сценарий (спутник-jar'ы +
  child-first thrift для изолированного Hive 4-рантайма, кросс-loader `ThriftValueConverter`),
  перепрогнал весь юнит-набор зелёным (641 тест). Свежему HDFS нужен каталог `/warehouse/hive4`
  рядом с остальными warehouse-каталогами — REST-путь create его не создаёт. 500 на purge-drop
  (заметки секции H) остаётся открытым.

- **2026-07-29** (третья запись), в тот же профиль добавлен **front door** Hive 4, и секция H
  получила строку H4: `additional-frontends.hive4fe` на 9085 (`APACHE_4_1_0`) плюс `hs2-hive4` —
  HiveServer2 Hive 4.1.0 из официального образа (Tez local mode, Iceberg встроен) — и принципал
  `hive/hs2-hive4@SMOKE.LOCAL`. Теперь сценарий доказывает, что одна Iceberg-таблица читается и
  пишется через все три Thrift-диалекта и REST одновременно; оба профиля перепрогнаны зелёными
  (`rows=5`: 2 rest + 1 hdp + 1 apache + 1 hive4).
  Включение front door вскрыло баг совместимости из заметок секции H (EXCL_WRITE), а
  Kerberos-проход потребовал двух стендовых настроек, которые у образов HiveServer2 3.1 в том или
  ином виде уже были: `yarn.resourcemanager.principal` + `mapreduce.job.hdfs-servers` в
  *core-site.xml* (без них INSERT падает с «Can't get Master Kerberos principal for use as
  renewer») и `tez.local.mode.without.network=true` — иначе Tez в local mode общается со своим
  in-process AM по Hadoop RPC, а тот под Kerberos требует SASL, для которого у него нет принципала
  («Client cannot authenticate via:[TOKEN, KERBEROS]» → «TezSession has already shutdown»). В
  официальном образе нет и Kerberos-клиента, поэтому обёртка ставит `krb5-workstation` для
  beeline, который смоук запускает внутри контейнера. Ловушка с устаревшим DNS сработала ещё
  дважды: `docker compose up --build <service>` пересоздаёт всю цепочку depends_on вместе с HDFS,
  после чего пару HiveServer2 3.1 нужно перезапустить (а прогон, стартовавший во время
  пересоздания, падает прямо на записи в HDFS).

- **2026-07-29** (четвёртая запись), interop-сценарий перестал быть hive4-only: `--prefix` теперь
  называет тот каталог, который текущий конфиг делает default-ным, `hs2-hive4` вынесен в
  собственный compose-профиль (`hive4fe`), чтобы диалект Hive 4 можно было гонять против любого
  бэкенда, Hive 4 front door добавлен в `hms-proxy.properties`/`hms-proxy-kerberos.properties`, а
  новая пара `hms-proxy-apache[-kerberos].properties` (плюс `.env.apache[-kerberos]`) меняет роли
  двух метасторов 3.1-линии местами, делая default-ным Apache 3.1.3. Эта раскладка — единственный
  способ вообще поставить `APACHE_3_1_3` на путь REST-записи (записи разрешены только в
  default-каталог), и она же переносит весь сценарий на второй HDFS-кластер. После этого секция H
  прогнана зелёной шесть раз — по разу на каждый бэкенд и профиль: `hdp` plain и kerberos,
  `apache` plain и kerberos, `hive4` plain и kerberos (последние два перепрогнаны уже после
  рефакторинга, так что ни одна ячейка не опирается на дорефакторинговый скрипт). Каждый прогон
  завершался `rows=5` и удалённой таблицей. Новых дефектов прокси не всплыло: найденное ранее
  понижение EXCL_WRITE — ровно то, что заставило диалект Hive 4 работать поверх 3.1-бэкенда, то
  есть capability `hive4_frontdoor_to_apache_backend_downgrade` впервые прогнана настоящим
  Hive 4-клиентом.

- **2026-07-29** (пятая запись), добавлен `--origin`: каждый front door по очереди создаёт
  таблицу, а остальные три её меняют (строки H9-H12); сценарий также получил чтение **до** каждой
  записи и финальный круг со всеми участниками, так что каждая передача доказана, а не
  предположена. Восемь прогонов на бэкенде `hive4` — plain и Kerberos для каждого из четырёх
  инициаторов, все зелёные. До 3.1-линии не дотягивается только инициатор Hive 4, и причина вне
  прокси: `STORED BY ICEBERG` не пишет в StorageDescriptor конкретный `inputFormat`. Это
  проверено руками до того, как было записано: в дескрипторе, который прокси передал, стоял
  `org.apache.hadoop.mapred.FileInputFormat`, а явное указание класса обработчика в DDL дало
  вместо него `inputFormat: null`. Обратное направление работает: таблицы, созданные storage
  handler'ом 3.1-линии, несут `HiveIcebergInputFormat`, и Hive 4 их спокойно читает и дописывает.

- **2026-07-29** (шестая запись), фикс purge влит, и сценарий перестал его обходить.
  `DELETE ...?purgeRequested=true` отвечал 500, потому что Maven выбирал avro 1.7.4 вместо
  1.12.0, против которой собран `iceberg-core`; с пином purge сначала прогнали руками против
  стенда — таблица с двумя строками, пять файлов под ней (parquet, manifest, manifest list, два
  metadata JSON), в ответ `204`, ноль оставшихся файлов и `404` при перезагрузке, — а
  interop-сценарий теперь заканчивается тем же purge плюс проверкой, что его ничего не пережило.
  Выяснилось, что прокси кэширует устаревший DNS namenode так же, как JVM HiveServer2: первый
  create после пересоздания HDFS падал на записи, пока контейнер прокси не перезапустили.

- **2026-07-29** (седьмая запись), добавлена секция I — изоляция писателей, — а затем расширена
  на разные front door. Прогоны стенда: 5 REST-писателей (все коммитят, 6 строк), 8 REST-писателей
  (7 коммитят, 1 отклонён с «branch main has changed», 8 строк) и REST против Hive-`INSERT`'ов на
  Hortonworks front door (13 из 14 коммитят, 14 строк). Межпутевой счётчик строк сначала ничего не
  стоил: REST-сторона заканчивала за четыре секунды до первого SQL-коммита, что было прямо видно в
  логе прокси. Поэтому сценарий переделан — REST-append'ы идут раундами, пока работает SQL-сторона,
  и добавлена проверка пересечения окон коммита. Сам детектор затем проверен на том самом
  непересёкшемся логе и корректно называет его непересечением, так что вхолостую проверка не
  пройдёт.

- **2026-07-30**, репозиторий на `074526b` в `main`; **код прокси под тестом не был** — это
  изменение добавляет новый раннер (`run-iceberg-rowlevel-smoke.sh`) и три добавления в
  REST-writer (`--properties` у `create`, `--where` у `count` и команду `files`, которая отдаёт
  число data- и delete-файлов спланированного скана). Стенд гонял уже собранный fat jar из
  `smoke-stand/proxy/`, неизменный на всех четырёх прогонах. В секции H появились строки H13-H20 —
  они закрывают row-level-пробел, оставшийся от interop-сценария: тот только дописывает строки,
  поэтому до сих пор на стенде не появилось ни одного delete-файла.
  Четыре прогона, все зелёные, все на бэкенде `hive4`: `--mode merge-on-read` и
  `--mode copy-on-write`, каждый сначала на plain-профиле (`.env.hive4`, профили `hive4`+`hive4fe`+
  `hdp`), затем на Kerberos (`.env.hive4-kerberos`, `--kerberos`). Каждый прогон — одна и та же
  последовательность: REST создаёт таблицу format-version 2 и дописывает 5 строк, все три
  SQL-движка читают её как контроль, Hive 4 удаляет две строки и обновляет одну, REST-клиент
  проверяет эффект (3 строки, `src=updated` ровно 1, `src=rest` ровно 2), все три движка читают
  результат, HDP-движок дописывает в неё строку, оба 3.1-движка получают отказ на собственные
  `DELETE`/`UPDATE`, и REST делает purge-drop с проверкой, что ничего не осталось. После этого
  writer переработали (переставили методы, поправили одну строку лога), и kerberos-прогон
  merge-on-read повторили зелёным уже на пересобранном jar — ни одна зелёная ячейка не опирается на
  jar, которого больше нет.
  Главный результат — **отрицательный**, и записан именно так: линия 3.1 читает merge-on-read
  нормально. Ожидание на входе было противоположным — что `iceberg-hive-runtime` 1.6.1 не применит
  position-deletes и H16 станет ещё одним ограничением «Hive 4 записал — 3.1 не читает» рядом с
  H12. Применяет: оба 3.1-движка вернули ровно `1:rest, 3:rest, 5:updated`. Значит, ограничение
  только в том, что 3.1-линия не умеет row-level *записи* (H18, отказ на этапе компиляции с
  SemanticException 10297).
  Инвертировать проверку, чтобы доказать её различительную силу, не потребовалось: `merge-on-read`
  и `copy-on-write` проходят через одну и ту же проверку формы файлов и дали 1 delete-файл и 0
  соответственно, так что Hive 4, тихо проигнорировавший `write.delete.mode`, завалил бы один из
  двух прогонов. Сканы намеренно `select id, src`, а не `select count(*)`: count может прийти из
  закэшированной Hive-статистики по Iceberg-таблице вообще без чтения delete-файла.
  Не гонялось: бэкенды `hdp` и `apache` (`--prefix hdp` / `--prefix apache`) — row-level-строки
  опираются только на бэкенд `hive4`, потому что проверяем возможности Hive-стороны, а не
  runtime-профиль бэкенда; не тронуты и партиционированные таблицы, эволюция схемы (остаются
  непокрытыми, см. секцию F) и `MERGE INTO`. Другие секции не перепрогонялись.
  Заметки по стенду на будущее: раннер работает на хосте, то есть его `sed` — это BSD sed; первая
  версия использовала альтернацию `\|`, которая молча ничего не находила, и прогон упал на пустом
  чтении формы файлов. Переключение стенда между `.env.hive4` и `.env.hive4-kerberos` пересоздаёт
  HDFS-цепочку контейнеров, так что привычная оговорка про перезапуск из-за устаревшего DNS
  остаётся в силе.

- **2026-07-30**, jar `1.0.19-f4cbeea7`, собранный из `f4cbeea` и разложенный на стенд, — то есть
  под тестом ровно тот код прокси, который закоммичен: починка pointer guard'а уже берёт табличный
  лок Iceberg, а `rest-catalog.purge.mode` существует. Закрыты последние две открытые ячейки
  секции I и добавлены строки I5 и I6.
  У I2 и I3 появилась Kerberos-колонка (`--prefix hive4 --kerberos`, `--writers 5` и `--writers 8`,
  `--sql-writers 0`): 6 строк на 1 базовую + 5, а на 8 писателях число строк равно числу
  писателей, отчитавшихся об успехе. На восьми писателях прогон сделан дважды намеренно — именно
  это и исправило таблицу: в одном прогоне писатель был отклонён (7 + 1), в следующем не отклонён
  никто (8 + 0). Оба исхода корректны, поэтому формулировка I3 — и утверждение README стенда, что
  на восьми писателях отказ получается «стабильно», — выдавали за правило то, что меняется от
  прогона к прогону. Инвариант, который проверяет сценарий, — число строк.
  I5 и I6 дал новый `run-iceberg-txn-contention-smoke.sh`, прогнанный на бэкенде `hive4` на обоих
  профилях (Kerberos трижды, plain один раз после подъёма всего стенда на `.env.hive4`):
  двухтабличная транзакция с устаревшим `assert-ref-snapshot-id` отклоняется с
  `409 CommitFailedException`, изменение не остаётся ни на одной из таблиц, 5 строк конкурента
  целы, а позитивный контроль с актуальным snapshot id принят и применён к обеим таблицам.
  Не прогонялось: I6 на профиле plain — он закреплён тестом
  `IcebergRestEndpointIntegrationTest#multiTableTransactionMustNotReportSuccessWhenTheSecondCommitFails`
  и подтверждён на стенде под Kerberos исчерпанием ddl-класса rate-limit'а; воспроизведение того же
  на plain требует той же правки конфигурации и не добавляет ничего к тому, что юнит-тест решает
  детерминированно. Другие секции не перепрогонялись, а `stand-hs2-hdp` всё время оставался в своём
  керберизованном контейнере (он в профиле `hdp`, который эти прогоны не используют).
  Заметки по стенду на будущее: раннер работает на хосте, то есть `sed` — это BSD sed, и `\?` там
  **не** читается как «необязательный»; BRE в стиле GNU молча ничего не находил и возвращал каждое
  поле JSON вместе с его собственным именем (`grep` при этом `\?` понимает — это и маскировало
  ошибку). Под Kerberos `curl` работает внутри `stand-proxy`, поэтому ни `-o`, ни `--data @file` не
  вправе указывать на хостовый путь — тело обязано возвращаться в stdout. У пересозданного
  `stand-proxy` нет кэша тикетов, поэтому сценарий делает `kinit` сам, а не ожидает готового.
  Переключить между профилями один прокси нельзя: метасторы сохраняют свою аутентификацию и
  отвечают `500`, так что на другой env-файл приходится поднимать весь стенд.

- **31.07.2026**, jar собран из этого изменения и уложен в стенд (размер
  `/opt/hms-proxy/hms-proxy.jar` сверен с собранным fat jar, совпал), **H12 из задокументированного
  ограничения стала проходящей строкой** — а само ограничение, как выяснилось, никогда и не
  существовало. Старый текст утверждал, что таблицу, созданную Hive 4, не читает 3.1-линия, потому
  что `STORED BY ICEBERG` оставляет абстрактный `inputFormat`. Не оставляет: таблица создаётся с
  конкретным `HiveIcebergInputFormat`, и оба 3.1-движка её читают. Формулировка держалась потому,
  что `--origin hive4` исключал эти два движка из прогона, — сценарий утверждал ограничение вместо
  того, чтобы его проверять. Когда исключение убрали, один за другим вскрылись два настоящих
  дефекта на стороне **записи**, оба из развилки `engine.hive.enabled` в `HiveTableOperations`
  Iceberg (см. «H12 подробно»): собственные REST-коммиты прокси срезали Hive-дескриптор — починено
  раньше ключом `rest-catalog.hive-engine-descriptor`; а затем его срезал обычный `INSERT` от
  3.1-HiveServer2, чему никакая настройка прокси не могла помешать в источнике, потому что флаг
  читается внутри JVM того движка. Починено тем, что `IcebergTablePointerGuard` теперь сохраняет
  дескриптор из записи метастора (`routing.iceberg-pointer-guard.hive-engine-descriptor`, новый
  outcome `hive_descriptor_kept`).
  Причина найдена, а не угадана: развилка `hiveEngineEnabled`/`storageDescriptor` вычитана из
  `iceberg-hive-runtime-1.6.1.jar` через `javap`, отсутствие `engine.hive.enabled` — из
  собственного `metadata.json` таблицы в HDFS, а сгенерированный `hive-site.xml` контейнера
  `hs2-hdp` проверен на этот флаг до того, как был тронут код.
  Прогоны, все зелёные: изолированная проба (Hive 4 создаёт, HDP делает `INSERT`, дескриптор цел,
  строки читаются через HDP, метрика показывает ровно один `hive_descriptor_kept`);
  `run-iceberg-interop-smoke.sh --prefix hive4` для **всех четырёх origin** на plain и
  `--origin hive4` и `--origin rest` на Kerberos; `run-iceberg-rowlevel-smoke.sh --prefix hive4` и
  `run-iceberg-concurrency-smoke.sh --prefix hive4` на plain — как регрессионное покрытие для
  изменения guard'а, которое теперь трогает каждый Iceberg-`alter_table`. Юнит-набор: 714 тестов,
  0 падений, 0 пропусков, на Java 17.
  Стенд намеренно по-прежнему **не** задаёт `iceberg.engine.hive.enabled` ни в одном из двух
  3.1-HiveServer2. Задать его там значило бы починить второй дефект в источнике и получить зелёный
  сценарий без участия прокси — именно поэтому он и не задан: строка перестала бы проверять
  починку. На реальном кластере задать его всё равно стоит.
  Не прогонялось: бэкенды `hdp` и `apache` (`--prefix hdp` / `--prefix apache`) и Kerberos-колонка
  секций row-level и concurrency. Изменение прокси лежит в общем для всех трёх бэкендов
  routing-пути и покрыто юнит-тестами на обоих runtime-профилях, но на стенде остальные два
  бэкенда для этого изменения не перезапускались.

- **2026-08-03**, jar `1.0.27-d14e85c2`, SQL-слой стенда впервые запущен из файлов репозитория. До
  сих пор все SQL-настройки в `smoke-stand/env/*.env` были закомментированы, поэтому
  `--scenario all` писал «skipping beeline SQL smoke», а `--scenario sql` вообще не стартовал:
  секции B и C держались на ручных прогонах, которые никто не мог воспроизвести. Парную топологию
  теперь покрывают четыре env-файла — `sql.env`, `sql-apache.env` и их `-kerberos`-двойники, — а
  раннер выбирает проходы через `HMS_SMOKE_SQL_FRONT_DOORS` и ACID-блоки через
  `HMS_SMOKE_TRANSACTIONAL_SQL_FRONT_DOORS`.
  Четыре прогона, все зелёные: Hortonworks-фронт над Hortonworks-каталогом по умолчанию и Apache
  над Apache, каждый на профилях plain и Kerberos. ACID-блок отработал впервые, и это проверено, а
  не выведено из кода возврата: в логе прокси есть `allocate_table_write_ids`, транзакционная
  таблица создана в каждой паре. Керберизованность подтверждена тем же способом
  (`security.mode=KERBEROS`, 169 обращений от `hive/hs2@SMOKE.LOCAL`, SASL в логе), а не принята на
  веру потому, что так назван профиль.
  По дороге всплыли две поломки, и обе оказались кросс-парами, а не ограничениями поддерживаемой
  раскладки: C6 — Apache-фронт поверх Hortonworks-бэкенда, где вставка доходит, а обновление
  статистики отклоняется из-за отсутствия транзакционного write ID; и C7 — Hortonworks-фронт поверх
  Apache-бэкенда, где собственный `add_write_notification_log` Hive отклоняется, потому что бэкенд
  не Hortonworks-рантайм. C6 доведён до вывода: прокси поле не теряет — клиенты шлют разные RPC, и
  вендорская сборка `set_aggr_stats_for` не вызывает вовсе. Федерация отсечена тем, что
  нефедерированная БД падает так же, конфигурация стенда — тем, что у обоих HiveServer2 настройки
  совпадают.

- **2026-08-04**, jar прокси тот же, что в записи выше: изменение целиком в двух образах
  метасторов (коммиты 5067ffa, c8794da, 2dec04a). Теперь каждый работает на том Hadoop, против
  которого собран его jar метастора, а не на Maven-наборе `hadoop-common` 2.6.0 /
  `hadoop-hdfs` 2.2.0; jar-ы кладутся в `hms/override-{hdp,apache}` впереди Maven-набора. Что это
  починило, записано в C10: `TRUNCATE` от Apache-клиента раньше падал на
  `HdfsAdmin.getEncryptionZoneForPath`, а теперь опустошает таблицу.
  Зелёное после пересборки: SQL-пары секций B и C на Kerberos (Hortonworks над Hortonworks-каталогом
  по умолчанию, Apache над Apache с включённым `TRUNCATE`), `--scenario all` на Kerberos и
  Iceberg-interop на всех трёх бэкендах под Kerberos —
  `run-iceberg-interop-smoke.sh --prefix hive4 --kerberos`, `--prefix apache --kerberos` и
  `--prefix hdp --origin rest --kerberos`; каждый закончился собственной строкой `smoke passed`,
  все четыре front door сошлись на 5 строках, purge не оставил файлов.
  Не зелёное и не регрессия от пересборки: `--prefix hdp --origin hdp`. Эта комбинация никогда не
  гонялась — перебор `--origin` прогонялся только на бэкенде `hive4`, — и пройти не может: `INSERT`
  3.1-движка в `MANAGED_TABLE`, которую его же DDL создаёт в Hortonworks-метасторе, попадает во
  взаимоблокировку с Iceberg-локом, который берётся внутри того же самого запроса. Это измерено, а
  не выведено: строки локов прочитаны из `show locks`, пока запрос висел. В контрольном прогоне —
  тот же движок и тот же метастор, но внешняя таблица, созданная REST, — Hive лочит только
  плейсхолдер `_dummy_database`, и запрос проходит. См. «Почему в 3.1-DDL стоит `EXTERNAL`» в
  секции H.
  Не прогонялось: профиль plain ни для чего из перечисленного, а также сценарии row-level и
  concurrency.

- **2026-08-04, тот же день**, взаимоблокировка закрыта в источнике: `sql_create_ddl` создаёт
  таблицу 3.1-линии как `create external table ... stored by 'HiveIcebergStorageHandler'`. На всех
  остальных путях таблица и так внешняя — REST создаёт её такой, а метастор Hive 4 переписывает
  managed во внешнюю, — поэтому изменение убирает случайность DDL, а не ослабляет проверку: те же
  четыре front door по-прежнему передают друг другу одну и ту же таблицу.
  Четыре прогона на Kerberos, все зелёные; набор выбран так, чтобы покрыть SQL-инициатором каждый
  бэкенд и поймать регрессию на строке, которая была зелёной раньше: `--prefix hdp --origin hdp`
  (комбинация, которая раньше висела, теперь `smoke passed` с 5 строками),
  `--prefix hdp --origin apache`, `--prefix apache --origin apache` и
  `--prefix hive4 --origin hdp`. Механизм сначала проверен изолированно, а не только сценарием: тот
  же HDP HiveServer2, создавая внешнюю Iceberg-таблицу и вставляя в неё строки, лочит только
  `_dummy_database`, и Iceberg-лок сразу после этого возвращается `ACQUIRED` — тогда как
  managed-таблица держала `default.<таблица> ACQUIRED EXCLUSIVE` против самой себя.
  Не прогонялось: профиль plain, остальные инициаторы на бэкенде `apache`, а также сценарии
  row-level и concurrency.

- **2026-08-04, третья запись**, jar `1.0.44-a51616fb`, собран из `a51616f` и разложен по стенду
  через `prepare.sh` (`using hms-proxy-1.0.44-a51616fb-fat.jar`). Полный перепрогон матрицы после
  дневных правок — два фикса дескриптора Iceberg в прокси, оба образа метастора, пересобранные на
  подходящем Hadoop, `EXTERNAL`-DDL для линии 3.1 и новые операции над метаданными в SQL-сценарии.
  Контейнеры пересоздавались на каждом профиле (`up -d --build --force-recreate`), тома — а вместе
  с ними и поставленные руками фикстуры — сохранялись.
  Зелёное на Kerberos: `--scenario all` изнутри `stand-proxy` на `.env.kerberos`
  (`scenario 'all' completed successfully`); обе SQL-пары изнутри `stand-hs2-hdp` —
  `sql-kerberos.env` на `.env.kerberos` и `sql-apache-kerberos.env` на `.env.apache-kerberos`,
  каждая с финальным `scenario 'sql' completed successfully`. Ни одному из SQL-прогонов не
  доверяли по коду возврата: ACID-блок подтверждён восемью вызовами `allocate_table_write_ids` с
  настоящими парами `TxnToWriteId` в логе прокси, а `TRUNCATE` в паре Apache — двумя результатами,
  которые он печатает: `truncate_emptied_managed_hdp` и `truncate_emptied_managed_apache`, то есть
  он теперь действительно опустошает таблицу на *обоих* метасторах — ради этого и пересобирались
  образы (C10).
  `run-iceberg-interop-smoke.sh --kerberos` прогнан на всех трёх бэкендах по два инициатора на
  каждый — `--prefix hdp` с `--origin rest` и `--origin hdp`, `--prefix apache` с `--origin rest`
  и `--origin apache`, `--prefix hive4` с `--origin rest` и `--origin hdp`, — каждый прогон
  заканчивался своим `smoke passed`, все четыре front door сходились на 5 строках.
  `run-iceberg-rowlevel-smoke.sh --prefix hive4 --kerberos` прошёл в обоих режимах удаления,
  `run-iceberg-concurrency-smoke.sh --prefix hive4 --writers 8 --kerberos` прогнан дважды (5
  успехов против 3 громких отказов, затем 6 против 2 — оба раза число строк совпало с числом
  успешных писателей), а `run-iceberg-txn-contention-smoke.sh --prefix hive4 --kerberos` отверг
  устаревшую транзакцию с `409`, и его позитивный контроль был принят.
  Зелёное на профиле plain: `--scenario all` с хоста через `env/simple.env` и
  `run-iceberg-interop-smoke.sh --prefix hdp` для `--origin hdp` и `--origin rest`.
  Юнит-набор на Java 17: 714 тестов, 0 падений, 0 ошибок, 0 пропусков.
  **Всплыл один настоящий дефект, и он в сценариях, а не в прокси: проверка purge не умела падать
  под Kerberos.** Plain-прогон `--prefix hdp --origin hdp` остановился на `purge left 7 file(s)`, и
  эти семь оказались сиротами более раннего, оборванного прогона: их mtime шли с 08:11 до 08:23 UTC
  шагом в три минуты — ритм ретраев того коммита, который раньше вставал во взаимоблокировку на
  managed-таблице, — а идентификаторы манифеста и снапшота принадлежали другой таблице, не той, что
  прогон только что дропнул; в логе прокси при этом перечислены как удалённые ровно четыре
  манифеста текущей таблицы. Незамеченными они пролежали потому, что проверка выполняла
  `docker exec <namenode> hdfs dfs -ls -R ... 2>/dev/null | grep -c`, а у контейнера namenode нет
  своего Kerberos-тикета: измерено напрямую — вызов падает с `Client cannot authenticate
  via:[TOKEN, KERBEROS]` и кодом 1, stderr глушился, счётчик выходил `0`. То есть *каждый*
  Kerberos-прогон interop- и row-level-сценариев печатал «purge left no data, manifest or metadata
  files behind», ни разу не заглянув в HDFS. Та же слепота отключала уборку `hdfs dfs -rm -r -f` во
  всех трёх сценариях — потому сироты и накапливались.
  Починено в `run-iceberg-{interop,rowlevel,concurrency}-smoke.sh`: контейнер namenode делает
  собственный `kinit` по keytab узла (`hdfs/namenode@SMOKE.LOCAL` — суперпользователь HDFS, так что
  ему видно и написанное Hive), нечитаемый HDFS теперь фатальная ошибка, а не пустой листинг, и
  неудачная уборка роняет прогон. Способность краснеть доказана, а не предположена: с одним
  подложенным в каталог таблицы `orphan-probe.parquet` Kerberos-прогон interop упал с
  `purge left 1 file(s)` — тот самый прогон, который до фикса был бы зелёным.
  После этого все шесть Kerberos-прогонов interop и перечисленные выше прогоны row-level,
  concurrency и txn-contention повторены уже с починенной проверкой, плюс один plain-прогон
  interop, — так что ни одно «зелено» в этой записи не опирается на вакуумную версию. Остатки
  прежних прогонов перед этим убраны руками (`/warehouse/hdp/smoke_iceberg_interop`,
  `/warehouse/hive4/smoke_iceberg_{interop,rowlevel,concurrent}` и
  `/warehouse/apache/smoke_iceberg_interop` — в последнем всё ещё лежал полный набор файлов от
  2026-07-29); после фикса каждый прогон снова вычищает свой каталог, что проверено листингом обоих
  кластеров. Три файла данных, оставленные на `hive4` теми писателями concurrency, чьи коммиты были
  отвергнуты, — обычные сироты Iceberg, а не дефект прокси: отвергнутый коммит не удаляет то, что
  уже записал.
  Не прогонялось: SQL-слой на профиле plain, сценарии row-level и concurrency на бэкендах `hdp` и
  `apache` и на профиле plain, а также I4 (`--sql-writers`, смешанный прогон REST и SQL) в любом
  виде.

## Две оговорки честности

- Kerberos-профиль полный сквозняком — клиент → HiveServer2 → прокси → метасторы → HDFS, ни один
  сервис не откатывается на simple auth. Но HDP HiveServer2 стартует только с `hive.in.test=true`:
  именно это позволяет сессии переключить движок на `mr`, потому что Hortonworks собирает без
  MapReduce. Путь *метаданных* от этого не страдает, исполнение запросов — не то, что делал бы
  настоящий HDP-кластер.
- Весь SQL идёт локальным MapReduce, поэтому тайминги и поведение под конкурентностью ничего не
  говорят о проде.
