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
он создал. Прогнано на бэкенде `hive4`:

| # | Инициатор (создаёт + пишет 2 строки) | Кто меняет дальше | plain | kerberos |
| --- | --- | --- | --- | --- |
| H9 | REST front door (Iceberg catalog `createTable`) | HDP, Apache, Hive 4 → 5 строк | ✅ | ✅ |
| H10 | HDP HiveServer2 (`STORED BY 'HiveIcebergStorageHandler'`) | REST, Apache, Hive 4 → 5 строк | ✅ | ✅ |
| H11 | Apache HiveServer2 (тот же DDL) | REST, HDP, Hive 4 → 5 строк | ✅ | ✅ |
| H12 | Hive 4 HiveServer2 (`STORED BY ICEBERG`) | только REST → 3 строки; движки 3.1-линии **не могут** её прочитать, см. ниже | ✅ | ✅ |

Каждый участник читает текущий итог **до** своей записи, поэтому каждая передача через границу
front door доказана, а не предположена; финальный круг заставляет всех участников подтвердить
один и тот же счёт.

**Единственная асимметрия — и она хайвовая, а не прокси.** Таблицу, созданную Hive 4, не читает
3.1-линия: `STORED BY ICEBERG` оставляет в StorageDescriptor абстрактный `inputFormat`
`org.apache.hadoop.mapred.FileInputFormat` (а если выписать класс обработчика явно — там вообще
`null`), потому что Hive 4 резолвит настоящий формат через storage handler на этапе плана.
Hive 3.1 же инстанцирует то, что названо в дескрипторе, и падает с `Cannot create an instance of
InputFormat class org.apache.hadoop.mapred.FileInputFormat`. Таблицы, записанные собственным
`HiveTableOperations` Iceberg — путь REST и сам storage handler 3.1-линии, — несут конкретный
`HiveIcebergInputFormat`, поэтому любой другой инициатор читается везде, включая Hive 4. Прокси
передаёт дескриптор без изменений в обе стороны; здесь нет решения по маршрутизации или
совместимости, которое он мог бы принять иначе.

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
| I2 | 5 конкурентных REST-писателей дописывают одну таблицу в default-каталоге: все 5 коммитят, в таблице ровно 6 строк (1 базовая + 5) — потерянных обновлений нет | ✅ | — |
| I3 | 8 конкурентных писателей: 7 коммитят, 1 отклонён с `CommitFailedException: branch main has changed`, в таблице ровно 8 строк (1 базовая + 7) — состязание разрешается отказом устаревшему писателю, а не тихой перезаписью | ✅ | — |
| I4 | **Через разные front door**: REST-append'ы и Hive-`INSERT`'ы (Hortonworks front door) коммитят в одну таблицу с пересекающимися окнами коммита | ✅ | ⚠️ **окно сужено, но не закрыто** |

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
снятый при компиляции запроса. Метастор применяет эти параметры целиком, включая
`metadata_location`, поэтому попавший в промежуток REST-коммит стирается, а идёт всё это вне
Iceberg-лока, так что ничто их не сериализует. `IcebergTablePointerGuard` теперь вшивает в такой
alter указатель, который метастор держит сейчас, отличая честный коммит по
`previous_metadata_location` (запрос, чья база равна текущему указателю, двигает таблицу вперёд;
любой другой несёт устаревшую копию).

Это закрывает окно «с момента компиляции», но не всю гонку: guard читает текущий указатель, а
бэкенд применяет alter — это два отдельных вызова, и коммит, попавший между ними, по-прежнему
затирается. На восьми прогонах после фикса один потерял строку (до фикса — примерно каждый
второй), и ещё один дал обратное: писателю сообщили о провале коммита, а строки легли, что при
retry даст дубль. **Считать строку починенной нельзя.** Чтобы закрыть по-настоящему, alter должен
стать условным: либо прокси держит тот же табличный лок, что берёт Iceberg, на всё
чтение-и-запись, либо шлёт `expected_parameter_key`/`expected_parameter_value` HMS, чтобы
сдвинувшийся указатель ронял alter громко, а не затирал данные.

## F. Что не покрыто и почему

| Область | Причина |
| --- | --- |
| ACID на non-default каталоге | Прокси отказывает в `allocate_table_write_ids` вне default-каталога **по design** — проходить нечему |
| YARN / Tez, распределённое исполнение | Стенд гоняет только локальный MapReduce; ничего не говорит о поведении прокси под конкурентностью настоящего кластера |
| Ranger, Atlas, HA | Вне области стенда |
| Cross-realm Kerberos trust | Оба кластера намеренно в одном realm; cross-realm проверял бы KDC, а не прокси |
| Нагрузка и конкурентность за пределами одной таблицы | Секция I покрывает конкурентные REST-коммиты в одну таблицу. Всё остальное однокли­ентское: нет конкурентных SQL-писателей, нет multi-table транзакций под состязанием и нет длительной нагрузки |

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

## Две оговорки честности

- Kerberos-профиль полный сквозняком — клиент → HiveServer2 → прокси → метасторы → HDFS, ни один
  сервис не откатывается на simple auth. Но HDP HiveServer2 стартует только с `hive.in.test=true`:
  именно это позволяет сессии переключить движок на `mr`, потому что Hortonworks собирает без
  MapReduce. Путь *метаданных* от этого не страдает, исполнение запросов — не то, что делал бы
  настоящий HDP-кластер.
- Весь SQL идёт локальным MapReduce, поэтому тайминги и поведение под конкурентностью ничего не
  говорят о проде.
