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

Гоняется через `--scenario rest` curl'ом с хоста (plain) либо curl'ом с `--negotiate` изнутри
`stand-proxy` (kerberos — KDC и hostname `proxy` резолвятся только внутри сети, а curl в
контейнере собран с GSS). Загружаемая таблица — зарегистрированная вручную `smoke_iceberg_tbl`
(см. README стенда). Kerberos-профиль всю фазу 5a держал listener выключенным, потому что SPNEGO
требовал GSS-способный curl внутри сети; как только это перестало быть верным, listener включили
и там тоже (`rest-catalog.kerberos.principal=HTTP/proxy@SMOKE.LOCAL`, тот же keytab, что и у
Thrift front door), и против него прогнали write round trip, write gate и проверку
неаутентифицированного запроса — см. вторую запись за 2026-07-28 в журнале ниже. Read-only строки
(G2-G22, G27-G30) на Kerberos-профиле пока не перепрогонялись и остаются `n/a`.

| # | Проверка | plain | kerberos |
| --- | --- | --- | --- |
| G1 | `GET /v1/config` объявляет `prefix=hdp` (default-каталог) | ✅ | ✅ |
| G2 | Листинг и load namespace (`default`) | ✅ | n/a |
| G3 | Листинг таблиц показывает Iceberg-таблицу и прячет обычные Hive-таблицы той же базы | ✅ | n/a |
| G4 | Load таблицы возвращает `metadata-location` и полные метаданные, прочитанные из HDFS самим прокси | ✅ | n/a |
| G5 | Неизвестный prefix → чистый 404 `NoSuchCatalogException` | ✅ | n/a |
| G6 | Неизвестная таблица → чистый 404 | ✅ | n/a |
| G7 | `DELETE` несуществующей таблицы отвечает чистым 404, а не тихим 2xx | ✅ | n/a |
| G8 | `GET /v1/config?warehouse=apache` объявляет `prefix=apache` | ✅ | n/a |
| G9 | Неизвестный warehouse (`GET /v1/config?warehouse=no_such_warehouse_smoke`) → чистый 400 | ✅ | n/a |
| G10 | Чистое представление namespace под prefix `apache` показывает `default` без утечки внешних имён вида `apache__*` | ✅ | n/a |
| G11 | Load таблицы под prefix `apache` (`smoke_iceberg_tbl_ap`, второй HDFS-кластер) возвращает `metadata-location` | ✅ | n/a |
| G12 | Federated namespace `apache__default` остаётся виден под default-prefix | ✅ | n/a |
| G13 | Листинг и load `smoke_iceberg_tbl_ap` через federated-имя `apache__default` под default-prefix | ✅ | n/a |
| G14 | Таблица default-каталога под prefix `apache` → чистый 404 | ✅ | n/a |
| G15 | Внешнее имя `apache__default`, использованное как namespace под prefix `apache` → чистый 404 | ✅ | n/a |
| G16 | Обычная Hive-таблица второго каталога (`smoke_read_ap`) не видна в листинге под prefix `apache` | ✅ | n/a |
| G17 | REST-метрики (`requests_total`, `listener_info`) видны на management-endpoint `/metrics` | ✅ | n/a |
| G18 | `HEAD` на namespace/таблицу отвечает `204`, если объект существует, и `404`, если нет — в том числе под не-default prefix `apache` и для обычной Hive-таблицы (`smoke_read_hdp`) | ✅ | n/a |
| G19 | Error-ответ на отсутствующий namespace несёт смапленные `404`, `type` и `message`, но без `"stack":[...]` server trace | ✅ | n/a |
| G20 | Нераспарсиваемое тело `POST .../metrics` отвечает `400` (`BadRequestException`), а не `500` | ✅ | n/a |
| G21 | `GET /v1/config` и `GET /v1/{prefix}/config` (оба резолвятся в default-каталог) объявляют write-роуты create и drop таблицы поверх read-роута namespaces | ✅ | n/a |
| G22 | `GET /v1/{second-prefix}/config` (non-default каталог) объявляет read-роут namespaces и не несёт ни одного write-роута — доказывает, что discovery объявляет write/read-асимметрию, а не только default-сторону | ✅ | n/a |
| G23 | Write round trip таблицы на default-каталоге: `POST` create (`200`), `GET` load (`metadata-location` присутствует), `DELETE` drop (`2xx`) | ✅ | ✅ |
| G24 | Прямой `POST` create под non-default prefix `apache` отклонён с `403` (`ForbiddenException`) | ✅ | ✅ |
| G25 | `POST` create под federated-namespace `apache__default`, достигнутым через default-prefix, отклонён с `403` — доказывает, что write gate проверяется на *резолвленном* каталоге, а не на prefix запроса | ✅ | ✅ |
| G26 | Настоящий `POST` commit против только что созданной таблицы (requirement `assert-table-uuid` + update `set-properties`) отвечает `200`, и возвращённый `metadata-location` отличается от того, что дал create — доказательство, что новый metadata-файл действительно записан через `HiveTableOperations.commit`, а не тихий no-op | ✅ | ✅ |
| G27 | `POST /v1/{prefix}/tables/rename` отвечает `204`, а `GET` по новому имени отвечает `200` | ✅ | n/a |
| G28 | `POST /v1/{prefix}/transactions/commit`, называющий таблицу в federated-namespace `apache__default`, отклонён с `403` | ✅ | n/a |
| G29 | `POST /v1/{prefix}/namespaces` с federated-именем (`apache__zzz_smoke`) отклонён с `403` | ✅ | n/a |
| G30 | `POST /v1/{prefix}/tables/rename` с federated destination-namespace (source-таблица ещё под текущим именем) отклонён с `403` — доказывает проверку именно destination-стороны gate, а не только source | ✅ | n/a |
| G31 | Запрос без `--negotiate` отклоняется `401` с вызовом `WWW-Authenticate: Negotiate` и пустым телом | n/a | ✅ |
| G32 | Namespace DDL round trip: `POST .../namespaces` create (`200`), `GET` load (`200`), `POST .../properties` update (`200`) с последующим `GET`, подтверждающим, что property реально появилось, `DELETE` (`204`), `GET` после этого (`404`) — по-настоящему новое: `RoutingMetaStoreClient` не реализовывал `createDatabase`/`alterDatabase`/`dropDatabase` до этой фазы, так что namespace DDL впервые дошёл до реального metastore | ✅ | ✅ |
| G33 | View write round trip: `POST .../views` create отвечает `200` с реальным `metadata-location`, `GET .../views` листит новый view, `DELETE` отвечает `204` | ✅ | ✅ |
| G34 | `POST /v1/{prefix}/transactions/commit` против только что созданной таблицы: отвечает `204`, и `metadata-location` таблицы после этого отличается от того, что дал create — доказательство, что multi-table commit реально записал новый metadata-файл, а не тихий no-op | ✅ | n/a |

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

## Две оговорки честности

- Kerberos-профиль полный сквозняком — клиент → HiveServer2 → прокси → метасторы → HDFS, ни один
  сервис не откатывается на simple auth. Но HDP HiveServer2 стартует только с `hive.in.test=true`:
  именно это позволяет сессии переключить движок на `mr`, потому что Hortonworks собирает без
  MapReduce. Путь *метаданных* от этого не страдает, исполнение запросов — не то, что делал бы
  настоящий HDP-кластер.
- Весь SQL идёт локальным MapReduce, поэтому тайминги и поведение под конкурентностью ничего не
  говорят о проде.
