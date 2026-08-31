# HMS Proxy

[![CI](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml)

English documentation: [README.md](README.md), [SMOKE.md](SMOKE.md)

HMS Proxy - это catalog-aware Hive Metastore federation and compatibility proxy для смешанных
окружений с Apache Hive `3.1.3`, Hive `4.1.x` и Hortonworks Data Platform `3.1.0.x`.

Он даёт один (или несколько) production-facing HMS Thrift endpoint(ов), которые федеративно
маршрутизируют каталоги в несколько backend metastore, сглаживают API-различия Apache 3.1.3 /
HDP 3.1.0.x / Hive 4.1.x в обе стороны и задают явную security boundary между клиентами и
backend HMS сервисами.

## Три опоры

### 1. Federation

Один production-facing HMS Thrift endpoint для HiveServer2 и прямых HMS API клиентов (или
несколько, на разных портах, когда клиенты разных версий Hive не могут делить общий front
door — в Thrift-протоколе нет version-negotiation handshake), который маршрутизирует запросы
в несколько backend metastore по явному `catName` или по legacy database name в формате
`catalog<separator>db`.

Это позволяет централизовать catalog-aware routing и selective exposure, не заставляя клиентов
знать внутреннюю раскладку backend metastore.

### 2. Compatibility bridge

Apache Hive Metastore `3.1.3`, Hortonworks `3.1.0.x` или Hive `4.1.x` на фронте (один primary
listener плюс опциональные дополнительные listener'ы на отдельных портах) поверх любой
комбинации Apache 3.1.3, Hortonworks 3.1.0.x и Hive 4.1.x backend'ов. Proxy выполняет
compatibility downgrade выбранных `*_req` API для старых Hortonworks backend'ов; поднимает
два positional read метода, удалённых в Hive 4 (`get_table`, `get_table_objects_by_name`),
когда фронтит Hive 4 backend для Apache 3.1.3 клиента; и опускает Hive 4-only `*_req`
wrappers для read/стандартного DDL до их positional Apache 3.1.3 эквивалентов, когда
обслуживает Hive 4 клиентов против Apache 3.1.3 backend. Hortonworks-специфичные RPC
доступны через HDP `standalone-metastore` jar при сконфигурированном HDP backend; truly
Hive 4-only API (data connectors, scheduled queries, stored procedures, packages, ACID v2
extensions) отвечают `TApplicationException UNKNOWN_METHOD`.

На практике proxy становится мостом для смешанных Apache/HDP/Hive 4 estate и поэтапных
миграций в обе стороны, а не просто request router.

### 3. Security boundary

Proxy как security boundary между клиентами и backend metastore: Kerberos/SASL на фронте,
опциональный outbound Kerberos к backend и опциональная impersonation аутентифицированного
Kerberos пользователя.

Так authentication, identity propagation и политика доступа к backend сосредоточены в одной
точке.

## Каноническая модель маршрутизации

В этом README встречаются два слоя именования:

| Термин | Что означает |
| --- | --- |
| Внешнее имя | Клиентский namespace на границе proxy. Примеры: `catName=catalog2`, `catalogName=catalog2`, legacy `dbName=catalog2__sales`. |
| Внутреннее имя | Реальные имена на выбранном backend HMS. Пример: backend `catName=hive`, `dbName=sales`. |
| Default catalog | `routing.default-catalog`, куда proxy идёт, если запрос не несёт надёжного catalog hint. |
| Ambiguous request | В запросе есть конфликтующие catalog hints, либо multi-catalog write не разрешается ровно в один catalog. |

Дальше routing работает так:

| Форма запроса | Как proxy выбирает backend | Что происходит без namespace |
| --- | --- | --- |
| Object-scoped read/write RPC | Сначала явный proxy `catName`. Иначе внешний `dbName` / `fullTableName` вида `catalog2__sales`. Иначе `routing.default-catalog` для совместимости. | Идёт в `routing.default-catalog`. |
| Session-level и global read RPC | Используется `routing.default-catalog`. | Идёт в `routing.default-catalog`. |
| ACID RPC, где в payload ещё назван объект | Routing по этому payload, например по `dbName` или `fullTableName` в `get_valid_write_ids`, `allocate_table_write_ids`, `compact` или `add_dynamic_partitions`. | Если routable object namespace уже нет, применяется строка ниже. |
| Txn / lock lifecycle RPC, где остались только ids | Пинятся к `routing.default-catalog`, например `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`. | Идут в `routing.default-catalog`; non-ACID `SELECT`, допустимые `NO_TXN` DDL и non-transactional write lock на non-default catalog может обслужить synthetic shim. |
| Global write и catalog-registry RPC | Требуют ровно один owned namespace в multi-catalog deployment. | Если ownership получается ambiguous, proxy безопасно падает вместо того, чтобы угадывать target catalog. |

Если запрос несёт backend `catName` вроде `hive`, а не proxy catalog id, это поле для
совместимости не считается авторитетным: proxy пытается маршрутизировать по `dbName`, а если
этого мало, идёт в `routing.default-catalog`.

Для mutation path proxy предпочитает policy-driven guarantees, а не best-effort guessing:
deterministic routing, explicit namespace ownership, отсутствие silent split-brain writes и safe
failure, если mutation остаётся ambiguous.

Следующие настройки меняют клиентское представление имён или SQL, но не сам выбор backend:

| Switch | Эффект |
| --- | --- |
| `routing.catalog-db-separator` | Меняет внешний legacy формат, например `catalog2__sales` вместо `catalog2.sales`. |
| `federation.preserve-backend-catalog-name=true` | Возвращает backend `catName` / `catalogName` вроде `hive`, но routing всё равно идёт по внешнему `dbName` или явному proxy catalog. |
| `federation.view-text-rewrite.mode=REWRITE` | Переписывает SQL внутри view между внешними и внутренними именами; на выбор backend для самого RPC не влияет. |

## Матрица поведения RPC

| Группа RPC | Статус | Поведение |
| --- | --- | --- |
| Metadata read/write RPC с явным namespace (`catName`, `dbName`, `fullTableName`) | supported | Нормально маршрутизируются в нужный catalog/backend. |
| Legacy read/write RPC с database name в формате `catalog<separator>db` | supported | Маршрутизируются по externalized database name; table/database объекты переписываются на обратном пути. |
| Apache `3.1.3` wrapper RPC против старых Hortonworks `3.1.0.x` backend | degraded | Proxy повторяет часть `*_req` API через старые legacy методы вроде `get_table`. |
| Apache `3.1.3` positional RPC против Hive `4.1.x` backend (`catalog.<name>.runtime-profile=APACHE_4_1_0`) | degraded | Backend adapter автоматически поднимает два positional метода, удалённых в Hive 4 (`get_table`, `get_table_objects_by_name`), до их `*_req` эквивалентов; всё остальное идёт через binary-compatible Thrift делегацию без изменений. |
| Hive `4.1.x` клиенты (`compatibility.frontend-profile=APACHE_4_1_0`) против Apache `3.1.3` или Hortonworks `3.1.0.x` backend | degraded | Frontend bridge понижает выбранные Hive 4-only `*_req` wrappers на read и стандартного DDL (`get_database_req`, `get_table_req`, `get_partition*_req`, `create_table_req`, `alter_table_req` и т.п.) до их позиционных Apache 3.1.3 эквивалентов; 199 общих методов идут через binary-compatible делегацию. |
| Hive 4-only API (data connectors, scheduled queries, stored procedures, packages, ACID v2 extensions) на `APACHE_4_1_0` front door | rejected | Отвечают `TApplicationException UNKNOWN_METHOD`; для этих RPC нет безопасного downgrade в Apache 3.1.3 backend. |
| Session-level и global read-only RPC без catalog context | degraded | Идут в `routing.default-catalog`, включая `getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns` и `get_open_txns_info`. |
| Read-only service API, которых нет на backend (`TApplicationException UNKNOWN_METHOD` на notifications, privilege refresh/introspection, token/key listings кроме delegation-token issuance, txn/lock/compaction status) | degraded | Proxy возвращает empty compatibility response вместо ошибки. Если метод у backend есть, но вызов провалился (любой другой тип `TApplicationException`, transport failure или `MetaException`), клиент получает ошибку — пустой ответ не выдаётся за реальное ACID, lock или privilege состояние. |
| Optional service read RPC (`get_active_resource_plan`, `get_all_resource_plans`, `get_runtime_stats`) | degraded | Proxy возвращает empty compatibility response при любом сбое backend, потому что эти RPC отдают только опциональные workload-management и diagnostic данные. |
| ACID / txn / lock lifecycle RPC без routable namespace (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`) | degraded | Пинятся к `routing.default-catalog`; для non-ACID `SELECT` lock, допустимых non-transactional `NO_TXN` DDL lock и non-transactional write lock на non-default catalog proxy может использовать synthetic lock shim, но это всё ещё не distributed ACID coordinator и shim не сериализует параллельных писателей. |
| Global write operations без явного catalog context | rejected | Proxy навязывает deterministic routing и explicit namespace ownership: namespace-less service write RPC вроде `setMetaConf`, `grant_role`, `revoke_role` и `add_token` безопасно падают вместо угадывания каталога. |
| Управление каталогами (`create_catalog`, `drop_catalog`) | rejected | Ownership каталогов policy-managed в конфиге proxy, а не через клиентские RPC. |
| HDP-only front-door методы, для которых есть явный Apache bridge mapping | supported | Proxy адаптирует выбранные Hortonworks request-wrapper методы к Apache equivalents. |
| HDP-only методы, которым нужен совместимый Hortonworks runtime (`add_write_notification_log`, `get_tables_ext`, `get_all_materialized_view_objects_for_rewriting`) | passthrough | Пробрасываются только в совместимые Hortonworks backend/front door при соответствующей конфигурации. |
| HDP-only методы без безопасного Apache mapping | rejected | Proxy падает явно, а не возвращает вводящий в заблуждение success. |

## Public compatibility matrix

Это публичная сводка того, как позиционировать proxy. Матрица сгруппирована не по отдельным thrift
методам, а по типу клиента, advertised front door, backend runtime, auth mode и семействам методов.
Таблица генерируется из [capabilities.yaml](capabilities.yaml), а каждая capability привязана к
smoke-тестам в test suite.

Если нужен spreadsheet-like method-level вид по backend support, routing mode, fallback strategy и
semantic risk, смотри [COMPATIBILITY.ru.md](COMPATIBILITY.ru.md).

Обновить сгенерированную таблицу можно так:

```bash
mvn -o -q -Dtest=CapabilityMatrixDocSyncTest -Dcapabilities.updateReadme=true test
```

<!-- BEGIN GENERATED: capability-matrix -->
| Версия клиента | Профиль front door | Профиль backend | Режим auth | Семейства методов | Ожидаемый результат |
| --- | --- | --- | --- | --- | --- |
| Apache Hive / Spark клиенты, которые говорят через Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | `APACHE_3_1_3` | `NONE` или `KERBEROS` | catalog-aware read/write, legacy `catalog<separator>db` routing, view rewrite | Базовый полностью поддержанный сценарий. |
| Apache Hive / Spark клиенты, которые говорят через Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | read path, часть metadata write, где возможен fallback с `*_req` API | Поддержано через compatibility downgrade; часть вызовов работает в degraded-режиме через legacy RPC. |
| Hive `4.1.x` клиенты (beeline, JDBC, Spark, Trino-Hive), говорящие на Hive 4 request-wrapper Thrift API | `APACHE_4_1_0` | `APACHE_3_1_3`, Hortonworks `3.1.0.x` или `APACHE_4_1_0` | `NONE` или `KERBEROS` | 199 read/write методов общих с Apache 3.1.3 (binary-compatible), часть Hive 4-only `*_req` wrappers понижается до позиционных Apache 3.1.3 RPC, `lock` с Hive 4-only типом `EXCL_WRITE` понижается до `EXCLUSIVE` | Поддержано через compatibility downgrade для read + стандартного DDL; Hive 4-only API (data connectors, scheduled queries, stored procedures, packages, ACID v2 extensions) отвечают `TApplicationException UNKNOWN_METHOD`. |
| Apache Hive / Spark клиенты, которые говорят через Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | Hive `4.1.x` | `NONE` или `KERBEROS` | 199 read/write методов общих с Hive 4 через binary-compatible Thrift делегацию, позиционные `get_table` и `get_table_objects_by_name` (удалены в Hive 4) автоматически поднимаются до `*_req` эквивалентов | Поддержано через compatibility upgrade для двух позиционных методов, удалённых в Hive 4; остальные RPC проходят прозрачно. Hive 4-only API (data connectors, scheduled queries и т.п.) недоступны с Apache 3.1.3 front door, потому что у Apache 3.1.3 нет Thrift bindings для них. |
| Hortonworks клиенты, которым достаточно Hortonworks identity на фронте через `getVersion()` | `HORTONWORKS_*` без standalone jar | `APACHE_3_1_3` или Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | пересекающиеся Apache/HDP method families | Поддержано, если клиенту достаточно только смены advertised profile. |
| Hortonworks клиенты, которые вызывают HDP-only thrift request-wrapper методы | `HORTONWORKS_*` с standalone jar | Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | mapped HDP-only methods, runtime-specific passthrough methods | Поддержано при наличии совместимых Hortonworks front-door и backend runtime jar. |
| Hortonworks клиенты, которые вызывают HDP-only thrift request-wrapper методы | `HORTONWORKS_*` с standalone jar | `APACHE_3_1_3` | `NONE` или `KERBEROS` | HDP-only passthrough методы вроде `add_write_notification_log` | Явно отклоняется, если target backend не даёт совместимый Hortonworks runtime. |
| HiveServer2 / Beeline SQL workloads через несколько каталогов | `APACHE_3_1_3` или `HORTONWORKS_*` | смешанные Apache + Hortonworks backend | `NONE` или `KERBEROS` | read, DDL/DML, namespace rewrite, optional view rewrite | Поддержано, пока routing может однозначно вычислить целевой каталог. |
| HiveServer2 / direct HMS клиенты, использующие txn/lock lifecycle RPC без namespace в payload | любой | смешанные Apache + Hortonworks backend | `NONE` или `KERBEROS` | `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat` | Degraded: идут в `routing.default-catalog`; допустимые non-ACID `SELECT`, `NO_TXN` DDL и non-transactional write (`INSERT`/`UPDATE`/`DELETE`) lock всё же могут синтетически обслуживаться на non-default catalog, но в остальном это стоит считать single-catalog control plane, пока не проведена отдельная валидация. |
| Kerberized HiveServer2 / HMS клиенты, которым нужна end-user identity на backend | любой | любой | `KERBEROS` с optional impersonation | front-door SASL, local delegation-token issuance, backend `set_ugi()` impersonation | Поддержано, если правильно настроены proxy-user rules и backend impersonation permissions. |
| Клиенты, которые пытаются делать mutation без explicit namespace ownership или динамически управлять registry каталогов | любой | любой | `NONE` или `KERBEROS` | policy-guarded ambiguous mutations, `create_catalog`, `drop_catalog` | Безопасно отклоняется по design, чтобы сохранить deterministic routing, explicit namespace ownership и не допустить silent split-brain writes. |
<!-- END GENERATED: capability-matrix -->

## Важные оговорки

- legacy database references без префикса каталога идут в `routing.default-catalog`
- это сохраняет совместимость со Spark/Hive и одновременно поддерживает deterministic routing без silent split-brain metadata write
- на практике это означает, что ACID write lifecycle полноценно поддерживается только для
  `default-catalog`, если из payload нельзя извлечь namespace
- request-based ACID методы, где в payload есть `dbName` или `fullTableName`, продолжают
  маршрутизироваться по этому payload
- proxy умеет синтетически обслуживать non-ACID `SHARED_READ` `SELECT` lock, допустимые
  non-transactional `NO_TXN` DDL lock и non-transactional write lock (`INSERT`, `UPDATE`,
  `DELETE`) для non-default catalog, чтобы HS2 read path, non-ACID DDL path и non-ACID
  `INSERT` не зависели от рассинхрона backend txn state
- этот synthetic lock state по умолчанию хранится в памяти, а для multi-instance failover может
  быть вынесен в ZooKeeper, но это не делает multi-catalog ACID writes безопасными
- Hive ACID, lock, token и другие по-настоящему global metastore операции всё ещё требуют
  отдельной валидации в вашей среде перед включением их за multi-catalog proxy

## Latency-aware routing backend'ов

Proxy умеет опционально включать latency-aware обработку медленных или нестабильных backend
metastore. Этот слой по умолчанию выключен, поэтому существующие deployment не меняют поведение,
пока ты явно не включишь новые настройки.

Когда он включён, proxy может:

- держать per-catalog latency budget через `catalog.<name>.latency-budget-ms`
- считать per-backend latency EWMA и на его основе подбирать adaptive backend socket timeout
- открывать circuit после повторяющихся transport failure или time-budget breach и затем давать
  half-open retry после `routing.circuit-breaker.open-state-ms`
- polling'ом обновлять backend readiness в фоне, а не только во время запроса к `/readyz`
- запускать в parallel только безопасные read-only fanout RPC при `routing.hedged-read.enabled=true`
- опционально кэшировать fanout-ответы со списками баз через `routing.database-list-cache.ttl-ms`
- исключать degraded backend из таких safe fanout read при
  `routing.degraded-routing-policy=SAFE_FANOUT_READS`

Это намеренно узкий механизм: hedged read и degraded omission применяются только к безопасным
read-only fanout method, сейчас это `get_all_databases`, `get_databases` и `get_table_meta`.
Single-backend write и namespace-sensitive mutation по-прежнему идут по детерминированной
маршрутизации выше и не race'ят несколько metastore одновременно.
Кэш списков баз выключен по умолчанию (`ttl-ms=0`); когда он включён, повторные
`SHOW DATABASES` / `get_all_databases` / `get_databases` не ходят в backend до истечения TTL.
Ключ включает catalog, pattern и impersonated user, но видимость DDL всё равно ограничена
настроенным TTL.

## Пул shared backend-сессий

Non-impersonated вызовы к каталогу идут через borrow/return пул backend Thrift-сессий размером
`catalog.<name>.shared-session-pool-size`. Дефолт — `1`, и он сохраняет прежнее поведение с
единственной сессией: параллельные shared-вызовы упрутся в один Thrift transport. Чтобы реально
получить параллелизм, выставляй это значение явно по каталогам, например:

```properties
catalog.catalog1.shared-session-pool-size=8
```

Что учитывать при повышении:

- Больше idle Thrift-сессий держится открытыми к backend HMS (с пропорциональной стоимостью
  Kerberos, если он включён на backend).
- `reconnectShared` дренирует весь пул, поэтому большие пулы дольше пересоздаются при смене runtime
  profile или принудительном reconnect.

Если на каталоге включена impersonation, реальный аутентифицированный клиентский трафик через этот
пул не идёт — каждый caller обслуживается из per-user кэша impersonation client'ов, управляемого
`catalog.<name>.impersonation-max-clients` и `catalog.<name>.impersonation-client-idle-ttl-ms`.
Внутри кэша на каждого пользователя живёт собственный пул backend Thrift-сессий размером
`catalog.<name>.impersonation-pool-max-size` (дефолт `4`); idle-сессии внутри пула можно закрывать
по `catalog.<name>.impersonation-session-idle-ttl-ms` (дефолт `0` — никогда). Параллельные вызовы
одного пользователя (или гонящего их HiveServer2) занимают разные сессии и реально идут в параллель,
а не сериализуются на одном Thrift transport. В Grafana это видно по метрикам
`hms_proxy_impersonation_pool_users`, `hms_proxy_impersonation_pool_sessions{state=active|idle}`,
`hms_proxy_impersonation_session_acquire_timeouts_total` (timeout per-user borrow) и
`hms_proxy_impersonation_session_evictions_total{reason=idle|transport_failure|user_evicted|user_capacity}`.
В shared пуле тогда остаются только:

- внутренние proxy-driven вызовы: backend health probe и reconnect-логика;
- запросы от service principal'ов, перечисленных в `security.service-principals.*`, — они by design
  обходят impersonation и переиспользуют общую backend-сессию.

В чистом impersonation-deployment без service-principal клиентского трафика повышать
`shared-session-pool-size` бессмысленно — на throughput приложения это не влияет. Тюнить пул нужно,
если хотя бы на одном каталоге `impersonation-enabled=false` или если значимую долю запросов к
каталогу гонят service principal'ы (например, сам HiveServer2).

## Сборка

```bash
mvn -o test
mvn -o package
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
```

Версия сборки вычисляется из git. Обычные branch-сборки используют настроенный jgitver-шаблон
`1.0.<git-distance>-<short-sha>`, tag release берёт pushed-тег `vMAJOR.MINOR.PATCH`, а rolling
nightly фиксирует Maven на последнем достижимом release-теге, поэтому nightly после `v1.2.0`
собирает jar-файлы версии `1.2.0-<short-sha>`.

GitHub Actions автоматически публикует сборки:
- branch и pull-request CI загружают jar-файлы как artifacts
- каждый push в `main`, плюс ручной `workflow_dispatch`, обновляет rolling prerelease `nightly` с актуального `main`
- pushed-теги `v*` публикуют обычные releases с GitHub-generated release notes

Ручные релизы публикуются через workflow `Release`:
- запускается через `workflow_dispatch`
- передаётся только `major_minor`, например `1.7`
- workflow сам вычисляет следующий patch и публикует GitHub release с тегом вида `v1.7.0`, `v1.7.1` и далее

## Запуск

```bash
java -jar "target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar" /etc/hms-proxy/hms-proxy.properties
```

`mvn package` создаёт обычный jar и runnable fat jar с classifier `fat`.
Имя fat jar меняется на каждом новом коммите.

Для Java 17+ с Hadoop 2.x Kerberos библиотеками запускай так:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -jar "target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar" /etc/hms-proxy/hms-proxy.properties
```

### Жизненный цикл front-door сокетов и shutdown

libthrift принимает клиентские сокеты с бесконечным read timeout, поэтому клиент, исчезнувший
без FIN/RST (сбой сети, kill процесса за NAT), навсегда оставляет свой worker-поток
заблокированным в read. Пул worker'ов ограничен `server.max-worker-threads`, так что listener
медленно вымывается вплоть до полного отказа принимать новые соединения. Поэтому каждый
listener ограничивает жизненный цикл сокета:

| Ключ | По умолчанию | Смысл |
| --- | --- | --- |
| `server.client-socket-timeout-ms` | `600000` | Read timeout на принятом соединении; `0` возвращает неограниченное поведение libthrift |
| `server.tcp-keepalive` | `true` | SO_KEEPALIVE на принятых соединениях |
| `server.tcp-keepalive-idle-seconds` | `120` | Простой до первой keepalive-пробы |
| `server.tcp-keepalive-interval-seconds` | `30` | Интервал между keepalive-пробами |
| `server.tcp-keepalive-count` | `4` | Сколько проб должно провалиться, прежде чем соединение будет закрыто |
| `server.shutdown-timeout-seconds` | `30` | Ограничение на упорядоченную остановку после SIGTERM |

`client-socket-timeout-ms` — именно **read** timeout: он ограничивает, сколько worker ждёт
следующего запроса на уже установленном соединении, и не обрывает долгие серверные вызовы вроде
`drop_table` с purge, потому что во время обработки запроса чтения нет. Дефолт согласован по
порядку величины с хайвовым `hive.metastore.client.socket.timeout` (600s); клиенты, обёрнутые в
`RetryingMetaStoreClient` (HiveServer2, Spark), переподключаются прозрачно, когда idle-соединение
рециклится.

Keepalive-таймеры ограничивают детект мёртвого peer'а величиной `idle + interval * count` секунд —
4 минуты на дефолтах вместо примерно двух часов, которые дают системные `tcp_keepalive_*`.
Тюнинг таймеров требует per-socket keepalive-опций (Linux и macOS); на платформе без них proxy
пишет одну запись в лог на listener и откатывается к системным таймерам, сохраняя обычный
SO_KEEPALIVE.

По SIGTERM shutdown hook останавливает primary listener и затем ждёт, пока main-поток закроет по
порядку дополнительные frontend listener'ы, management listener, backend-ресурсы router'а и
front-door security. JVM выполняет halt сразу после возврата последнего shutdown hook, поэтому
именно это ожидание не даёт оборвать in-flight запросы дополнительных фронтендов и оставить
незакрытыми backend-ресурсы. Ожидание ограничено `server.shutdown-timeout-seconds`; если
остановка не уложилась в лимит, proxy пишет WARN и всё равно отпускает JVM.

## Валидация конфигурации

Proxy проверяет properties-файл на старте и отказывается стартовать на значении, которое не может
интерпретировать. Proxy, который запустился и тихо делает противоположное написанному в файле,
хуже, чем proxy, который не стартовал.

**Boolean-значения** принимают только `true` и `false` в любом регистре. `yes`, `on`, `1` и
опечатки вроде `ture` — ошибка старта с указанием ключа и значения. Раньше они читались как
`false`, то есть молча выключали имперсонацию, management-листенер или hedged reads.

**Enum-значения** — все ключи режимов, профилей и политик — регистронезависимы, поэтому
`access-mode=read_only` и `frontend-profile=apache_3_1_3` принимаются везде. При неизвестном
значении сообщение содержит ключ и список допустимых констант.

**Длительности** в HiveConf-ключах вроде `hive.metastore.client.socket.timeout` соответствуют
Hive: целое число с необязательным суффиксом `ns`, `us`, `ms`, `s`/`sec`, `m`/`min`, `h`/`hour`,
`d`/`day` (длинные написания вроде `600sec`, `5min` тоже работают). Число без суффикса означает
секунды. Значение, которое отверг бы и сам Hive (например `1.5s`), логируется как `WARN` с
указанием значения и применённого дефолта.

**Противоречивые комбинации** дают ошибку старта, а не тихую подмену поведения:

| Комбинация | Результат |
| --- | --- |
| `catalog.<name>.write-db-whitelist` без `access-mode=READ_WRITE_DB_WHITELIST` | ошибка: whitelist игнорировался бы, запись оставалась бы разрешена во все базы |
| `access-mode=READ_WRITE_DB_WHITELIST` с пустым whitelist | ошибка: для полного запрета записи есть `READ_ONLY` |
| `synthetic-read-lock.store.mode=IN_MEMORY` вместе с любым ключом `synthetic-read-lock.store.zookeeper.*` | ошибка: локи молча остались бы в памяти |
| Два листенера на одном `host:port` | ошибка, в том числе через wildcard bind host: `0.0.0.0:9083` конфликтует с `127.0.0.1:9083` |

Проверка конфликтов охватывает основной листенер, management-листенер (включая дефолтный
`server.port + 1000`) и каждую запись `additional-frontends.<name>`. Хосты сравниваются как
заданы, без DNS-резолва, поэтому `localhost` против `127.0.0.1` по-прежнему проявится при bind, а
не на валидации.

## Observability

### Management listener

Proxy может поднимать легковесный HTTP listener для health checks, readiness и Prometheus
метрик. По умолчанию listener выключен и автоматически включается, если задан `management.port`:

```properties
management.bind-host=0.0.0.0
management.port=19083
```

Либо его можно включить явно:

```properties
management.enabled=true
# Необязательно; по умолчанию берётся server.bind-host
management.bind-host=0.0.0.0
# Необязательно; по умолчанию server.port + 1000
management.port=19083
# Необязательно; число handler-потоков management listener, по умолчанию 4
management.threads=4
# Необязательно; сколько мс переиспользуются результаты readiness probe;
# по умолчанию 2000, значение 0 отключает кэш
management.readiness-cache-ms=2000
```

Быстрые проверки:

```bash
curl -s http://127.0.0.1:19083/healthz
curl -s http://127.0.0.1:19083/readyz
curl -s http://127.0.0.1:19083/metrics
```

Listener обслуживает запросы из небольшого выделенного пула потоков (`management.threads`), поэтому
медленный `/readyz` не блокирует `/healthz` и `/metrics`. Держи как минимум два потока.

**У management endpoints нет ни аутентификации, ни авторизации.** `/readyz` отдаёт
Kerberos-принципалы и per-backend `lastError`, в котором обычно видны внутренние hostname. Привязывай
`management.bind-host` к интерфейсу, доступному только из сети мониторинга и оркестрации, либо
ограничивай порт firewall-правилами или network policy. Не публикуй его рядом с клиентским
Thrift-портом.

### Health и readiness endpoints

Доступные endpoints:

- `/healthz` возвращает liveness процесса и uptime
- `/readyz` проверяет backend connectivity, отдаёт per-backend состояние `connected` / `degraded`,
  а также включает Kerberos login status и TGT freshness для front-door и outbound backend credentials
- `/metrics` отдаёт Prometheus text format

`/healthz` предназначен для простых liveness checks и отвечает только на вопрос, жив ли сам
процесс proxy.

`/readyz` предназначен для load balancer, orchestration probes и operational diagnostics. В ответе есть:

- общий статус readiness
- `probeAgeMs` — возраст данных probe, по которым построен ответ
- summary по backend connectivity
- по каждому backend поля `connected`, `degraded`, `lastSuccessEpochSecond`,
  `lastFailureEpochSecond`, `lastProbeEpochSecond`, `lastLatencyMs`, `latencyEwmaMs`,
  `baselineTimeoutMs`, `adaptiveTimeoutMs`, `latencyBudgetMs`, `circuitState`,
  `consecutiveFailures`, `circuitRetryAtEpochMs` и `lastError`
- Kerberos status для front door и outbound backend credentials, включая `state`
  (`DISABLED`, `PENDING`, `ACTIVE`, `STALE`, `FAILED`), `loggedIn` и `healthy`
- TGT freshness через `tgtExpiresAtEpochSecond` и `secondsUntilExpiry`, когда эти данные доступны

Kerberos status читается по уже используемым proxy login'ам. Kerberos probe не переустанавливает
process-wide security configuration и не делает собственный keytab login, поэтому не конкурирует с
живыми SASL handshake и не добавляет трафика к KDC. При этом on-demand connectivity probes выше
по-прежнему открывают backend-сессию на каждый scrape, а она при backend SASL делает keytab login;
чтобы убрать это с пути запроса, включите `routing.backend-state-polling.enabled=true`.

Readiness падает только тогда, когда front-door login user отсутствует или вообще не имеет
Kerberos credentials. Истёкший TGT отдаётся как `state=STALE` с `secondsUntilExpiry<=0` и не роняет
readiness: Hadoop не обновляет TGT keytab-логина сам по себе, а SASL acceptor продолжает
аутентифицировать клиентов по service keys из keytab. Backend Kerberos - только диагностика:
`PENDING` до открытия первой backend-сессии, `STALE` когда последний зафиксированный backend login
непригоден; каждая backend-сессия делает собственный keytab login, поэтому реальное здоровье
backend'ов показывают connectivity probes. Так как probe больше не логинится, ротацию или отзыв
keytab сам `/readyz` не обнаруживает - это проявится через отказы клиентской аутентификации или
backend connectivity.

Если включён `routing.backend-state-polling.enabled=true`, readiness отражает результат последних
фоновых probe. Иначе `/readyz` сам делает on-demand probe backend'ов и возвращает те же поля.

Дорогая часть `/readyz` — это backend- и Kerberos-probe, поэтому их результаты переиспользуются в
течение `management.readiness-cache-ms` (по умолчанию 2s) и обновляются не более чем одним запросом
одновременно: параллельные вызовы получают предыдущий результат, а не порождают новый fanout
сетевых проверок. Per-backend поля состояния всегда рендерятся из актуального in-memory runtime
state, а `probeAgeMs` показывает, насколько устарели данные probe. Чтобы делать probe на каждый
запрос, поставь `management.readiness-cache-ms=0`.

### Prometheus метрики

Текущие Prometheus метрики:

- `hms_proxy_requests_total{method,catalog,backend,status}`
- `hms_proxy_request_duration_seconds{method,catalog,backend}`
- `hms_proxy_backend_failures_total{backend,exception}`
- `hms_proxy_backend_fallback_total{method,from_api,to_api}`
- `hms_proxy_routing_ambiguous_total`
- `hms_proxy_default_catalog_routed_total{method}`
- `hms_proxy_lock_request_split_total{catalog}`
- `hms_proxy_filtered_objects_total{method,catalog,object_type}`
- `hms_proxy_synthetic_read_lock_events_total{operation,catalog,store_mode,result}`
- `hms_proxy_synthetic_read_lock_store_failures_total{operation,store_mode,exception}`
- `hms_proxy_synthetic_read_lock_handoffs_total{operation,catalog,store_mode}`
- `hms_proxy_synthetic_read_locks_active{store_mode}`
- `hms_proxy_synthetic_read_lock_store_info{store_mode}`
- `hms_proxy_backend_session_acquire_timeouts_total{catalog,operation}`
- `hms_proxy_adaptive_timeout_reconnect_total{catalog}`
- `hms_proxy_adaptive_timeout_reconnect_skipped_total{catalog,reason}`
- `hms_proxy_iceberg_pointer_guard_events_total{catalog,outcome}`
- `hms_proxy_rest_requests_total{prefix,route,status}`
- `hms_proxy_rest_request_duration_seconds{prefix,route}`
- `hms_proxy_rest_listener_info{bind_host,port}`

Пример Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: hms-proxy
    static_configs:
      - targets:
          - hms-proxy-01.example.com:19083
```

Семантика метрик:

- `status` в `hms_proxy_requests_total` принимает значения `ok`, `error`, `fallback` или `throttled`
- `catalog=all, backend=fanout` означает, что запрос был отправлен сразу в несколько backend
- `hms_proxy_backend_failures_total` считает backend-side ошибки вызова с группировкой по backend и exception type
- `hms_proxy_backend_fallback_total` считает compatibility fallback, которые proxy вернул после backend failures
- `hms_proxy_routing_ambiguous_total` считает запросы, которые proxy безопасно отклонил из-за conflicting namespace hints вместо угадывания маршрута
- `hms_proxy_default_catalog_routed_total` считает запросы, которые ушли в default catalog из-за отсутствия явного catalog namespace
- `hms_proxy_lock_request_split_total` считает lock-запросы, назвавшие несколько каталогов и ушедшие в один из них, — компоненты остальных каталогов из запроса к backend удалены
- `hms_proxy_rate_limited_total` считает запросы, отклонённые overload protection, с лейблами по limiting dimension, configured scope, method family и resolved catalog
- `hms_proxy_filtered_objects_total` считает базы или таблицы, скрытые selective federation rules до возврата клиенту
- `hms_proxy_synthetic_read_lock_events_total` отражает lifecycle synthetic lock shim: `acquire`, `check_lock`, `heartbeat`, `unlock`, `release_txn`, `cleanup`
- `hms_proxy_synthetic_read_lock_store_failures_total` считает ошибки in-memory или ZooKeeper store с группировкой по операции и exception type
- `hms_proxy_synthetic_read_lock_handoffs_total` считает случаи, когда synthetic lock, открытый через один proxy instance, продолжает обслуживаться через другой instance
- `hms_proxy_synthetic_read_locks_active` показывает текущее число synthetic lock, видимых из выбранного store backend; значение правится инкрементально на каждом acquire/release и ресинхронизируется со store фоновым expiry sweep (раз в 30s), поэтому lock-операции не платят за листинг всего store
- `hms_proxy_synthetic_read_lock_store_info` это constant-info gauge, который помечает, работает ли proxy с `in_memory` или `zookeeper` storage для synthetic lock
- `hms_proxy_backend_session_acquire_timeouts_total` считает fail-fast события, когда пул shared backend metastore session исчерпан и permit не освобождается за `latencyBudgetMs` каталога (или 30s по умолчанию); `operation=borrow` для обычной диспетчеризации RPC, `operation=reconnect` для админских реконнектов, которым не удалось quiesce пул
- `hms_proxy_adaptive_timeout_reconnect_total` считает, сколько раз adaptive socket timeout приводил к reconnect shared backend client (с принудительным сбросом impersonation-кэша); полезен для отслеживания reconnect storm при нестабильной latency
- `hms_proxy_adaptive_timeout_reconnect_skipped_total` считает adaptive-timeout правки, подавленные троттлингом (`reason=hysteresis` для дельт ниже порога, `reason=cooldown` для срабатываний раньше cooldown окна после предыдущего reconnect)
- `hms_proxy_iceberg_pointer_guard_events_total` считает решения Iceberg pointer guard по `alter_table`: `repaired` (запрос стёр бы Iceberg-состояние записи, поэтому был слит поверх неё), `forward_commit` (Iceberg-коммит, построенный на текущем указателе, пропущен как есть), `not_iceberg` (в записи нет указателя, имя запомнено как не-Iceberg), `cache_suppressed` (ответ из этой памяти, без чтения бэкенда), `read_failed` (запись прочитать не удалось, alter пропущен как есть). Всё, кроме `cache_suppressed`, стоило одного `get_table`, поэтому по одной метрике видно и добавленные round trip, и hit rate кэша
- `hms_proxy_rest_requests_total` считает HTTP-запросы Iceberg REST с группировкой по catalog prefix, route и terminal HTTP-статусу
- `hms_proxy_rest_request_duration_seconds` измеряет длительность запросов Iceberg REST с группировкой по catalog prefix и route
- `hms_proxy_rest_listener_info` это constant-info gauge, который показывает настроенные bind host и port Iceberg REST listener'а

Несмотря на исторические имена метрик `synthetic_read_lock`, этот shim теперь также обслуживает
допустимые non-transactional `NO_TXN` DDL lock и non-transactional write lock на non-default
catalog.

В комплектный Grafana dashboard `monitoring/grafana/hms-proxy-dashboard.json` добавлены панели по
synthetic lock activity, handoff, store failures и active lock counts, а также template variable
`store_mode` для быстрого переключения между режимами `in_memory` и `zookeeper`.

### Structured audit log

Proxy также пишет один structured audit log на каждый запрос через logger
`io.github.mmalykhin.hmsproxy.audit`. Каждая запись представляет собой single-line JSON с полями
`requestId`, `method`, `operationClass`, `catalog`, `backend`, `status`, `durationMs`,
`remoteAddress`, `authenticatedUser`, `routed`, `fanout`, `fallback` и
`defaultCatalogRouted`.

Значения, которые приходят от клиента (`authenticatedUser`, `remoteAddress`), экранируются по
правилам JSON, включая все управляющие символы ниже `0x20`, поэтому враждебный principal или адрес
не сломает разбор записи в log collector. Не-ASCII символы выводятся как есть, в UTF-8.

Пример:

```json
{"event":"hms_proxy_audit","requestId":42,"method":"get_table","operationClass":"metadata_read","catalog":"catalog1","backend":"catalog1","status":"ok","durationMs":8,"routed":true,"fanout":false,"fallback":false,"defaultCatalogRouted":false,"remoteAddress":"10.20.30.40","authenticatedUser":"alice@EXAMPLE.COM"}
```

В комплектном `log4j.properties` этот logger направлен в собственный appender
`logs/hms-proxy-audit.log` (rolling по 100MB, 10 backup), без префикса layout — чтобы файл оставался
валидным JSON lines, и с выключенной additivity — чтобы audit-записи не смешивались с общим логом.
Чтобы писать в другое место или отправлять записи в свой log pipeline, переопредели appender
`auditFile` в собственном `log4j.properties`. Если приглушить logger ниже `INFO`, запись вообще не
строится — ничего не вычисляется ради вывода, который никто не читает.

### Grafana dashboard

Готовый Grafana dashboard лежит в
`monitoring/grafana/hms-proxy-dashboard.json`. В нём уже есть панели по request rate, latency,
backend failures, fallbacks, default-catalog routing и ambiguous routing, а также ряд Iceberg
REST: request rate, error ratio и квантили латентности REST-listener'а, разбивки по HTTP-статусу,
catalog prefix и route, и stat «listener up».

### Selective federation exposure

Можно публиковать только часть namespace backend-каталога, не меняя routing и write-policy.
Это удобно для постепенной миграции, безопасного multi-tenant rollout и снижения риска
случайного раскрытия метаданных.

На каталог:

```properties
# Обратная совместимость по умолчанию
catalog.catalog1.expose-mode=ALLOW_ALL

# Более безопасный rollout: видны только объекты, попавшие в allowlist
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-db-patterns=sales,finance_.*
catalog.catalog1.expose-table-patterns.sales=orders_.*,events
catalog.catalog1.expose-table-patterns.finance_.*=audit_.*
```

Правила:

- regex сопоставляются case-insensitively
- матчинг идёт по backend names, а не по externalized proxy names
- сопоставление делается по всей строке: `sales` совпадает только с `sales`, а `sales_.*` совпадёт с `sales_eu`
- `catalog.<name>.expose-db-patterns` задаёт allowlist для backend database names внутри каталога
- `catalog.<name>.expose-table-patterns.<dbRegex>` задаёт allowlist для таблиц внутри баз, чьё backend db name совпало с `<dbRegex>`
- table rules сужают видимость таблиц внутри совпавших баз; unmatched tables отфильтровываются
- при `DENY_BY_DEFAULT` базы без совпавшего db rule или table rule скрываются
- совпавший table rule может сделать базу видимой даже без db-level rule, но внутри неё будут видны только совпавшие таблицы

Фильтр применяется на metadata read-path’ах вроде `get_all_databases`, `get_databases`,
`get_table*`, `get_tables*`, `get_table_meta` и Hortonworks `get_tables_ext`.

Поведение по типу API:

- list-style RPC вроде `get_all_databases`, `get_all_tables`, `get_tables`, `get_table_meta` и `get_tables_ext` молча убирают скрытые объекты из ответа
- direct lookup вроде `get_database`, `get_table` и `get_table_req` возвращает "not found", если целевой объект отфильтрован
- `hms_proxy_filtered_objects_total{method,catalog,object_type}` считает и скрытые базы, и скрытые таблицы

Примеры:

```properties
# 1. Во время миграции публикуем только одну базу
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-db-patterns=sales

# 2. Публикуем одну базу, но только часть таблиц внутри неё
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-table-patterns.sales=orders_.*,customers

# 3. Каталог по умолчанию открыт, но одну чувствительную базу сужаем
catalog.catalog1.expose-mode=ALLOW_ALL
catalog.catalog1.expose-table-patterns.audit=.*_public
```

Что это означает:

- в примере 1 видна только backend db `sales`
- в примере 2 backend db `sales` становится видимой, но внутри неё доступны только таблицы, совпавшие с `orders_.*`, плюс `customers`
- в примере 3 все базы остаются видимыми, но в backend db `audit` возвращаются только таблицы, совпавшие с `.*_public`

Для non-default catalog помните, что фильтры всё равно матчятся по backend db names вроде `sales`,
а не по внешним именам вроде `catalog2__sales`.

## Guard для transactional DDL

Proxy можно настроить так, чтобы он защищал создание и изменение managed-таблиц, если во входящем
metadata таблица помечена как transactional:

- `transactional=true`
- любое непустое значение `transactional_properties`

Правило применяется ко всем RPC `create_table*` и `alter_table*`, включая
`create_table_with_environment_context` (именно этот вызов `HiveMetaStoreClient` реально
отправляет для `createTable`), `create_table_with_constraints` и `alter_table_with_cascade`.
Обёртки `*_req` из front door Hive 4 и Hortonworks разворачиваются в эти RPC до срабатывания
guard, поэтому они тоже покрыты. Оно срабатывает только для `MANAGED_TABLE`. External-таблицы
остаются без изменений.

Режим reject:

```properties
guard.transactional-ddl.mode=REJECT_TRANSACTIONAL
```

Режим rewrite:

```properties
guard.transactional-ddl.mode=REWRITE_TRANSACTIONAL_TO_EXTERNAL
```

В режиме rewrite proxy переписывает входящую таблицу в `EXTERNAL_TABLE`, добавляет
`external.table.purge=true` и удаляет `transactional` и `transactional_properties`.
Если нужен ещё и физический delete файлов на backend Apache `3.1.3`, это надо комбинировать с
`federation.external-table-drop-purge.mode=BEST_EFFORT` и per-catalog allowlist через
`catalog.<name>.conf.hms.proxy.external-table-drop-purge.allowed-prefixes`.

Также можно ограничить его конкретными IP-адресами или CIDR-масками:

```properties
guard.transactional-ddl.mode=REJECT_TRANSACTIONAL
guard.transactional-ddl.client-addresses=10.10.0.15,10.20.0.0/16,2001:db8::/64
```

Если `guard.transactional-ddl.client-addresses` задан, проверка применяется только к совпавшим
клиентам. Если не задан, проверка действует для всех клиентов.

## Rate limiting / overload protection

Proxy также умеет отсеивать bursts до того, как они превратятся в backend overload. Эта защита
работает локально на каждом proxy instance и использует token-bucket лимиты с:

- постоянной скоростью пополнения через `requests-per-second`
- опциональным allowance для короткого burst через `burst`
- независимыми bucket, так что запрос должен пройти все настроенные scope, которые к нему относятся

Поддерживаемые scope:

- per authenticated client principal: `rate-limit.principal.*`
- per exact source IP: `rate-limit.source.*`
- per source CIDR rule: `rate-limit.source-cidr.<name>.*`
- per HMS method family: `rate-limit.method-family.<family>.*`
- per logical catalog: `rate-limit.catalog.<catalog>.*`
- per high-risk RPC class: `rate-limit.rpc-class.<class>.*`

Поддерживаемые method families:

- `metadata_read`
- `metadata_write`
- `service_global_read`
- `service_global_write`
- `acid_namespace_bound_write`
- `acid_id_bound_lifecycle`
- `admin_introspection`
- `compatibility_only_rpc`

Поддерживаемые RPC classes:

- `write`
- `ddl`
- `txn`
- `lock`

Важное поведение:

- per-principal лимиты срабатывают только когда front door даёт authenticated user, например через Kerberos/SASL
- `rate-limit.source-cidr.<name>` это aggregate bucket на всё правило CIDR, а не отдельный bucket на каждый IP
- если один source IP совпал сразу с несколькими CIDR rule, применяются все совпавшие rule
- per-catalog лимиты срабатывают в момент, когда запрос реально resolve'ится или касается конкретного catalog/backend; значит fanout read может расходовать несколько catalog bucket
- один RPC может одновременно попасть в несколько classes, например lock/txn write может считаться сразу в `write`, `txn` и `lock`
- при превышении лимита клиент получает `MetaException`, а запрос записывается со `status="throttled"`

Пример:

```properties
# Per authenticated principal
rate-limit.principal.requests-per-second=60
rate-limit.principal.burst=120

# Per exact source IP
rate-limit.source.requests-per-second=30
rate-limit.source.burst=60

# Aggregate bucket на целую подсеть или клиентский pool
rate-limit.source-cidr.hs2-pool.cidrs=10.10.0.0/16,10.20.0.0/16
rate-limit.source-cidr.hs2-pool.requests-per-second=200
rate-limit.source-cidr.hs2-pool.burst=300

# Shaping по семейству методов
rate-limit.method-family.metadata_read.requests-per-second=600
rate-limit.method-family.metadata_read.burst=1000

# Per catalog
rate-limit.catalog.catalog1.requests-per-second=300
rate-limit.catalog.catalog2.requests-per-second=120

# Дополнительная защита для high-risk RPC classes
rate-limit.rpc-class.write.requests-per-second=80
rate-limit.rpc-class.ddl.requests-per-second=15
rate-limit.rpc-class.txn.requests-per-second=30
rate-limit.rpc-class.lock.requests-per-second=50
```

Нормальная production starting point:

- используй `principal` против runaway HS2 session или плохих end-user
- используй `source` и `source-cidr` против tooling, scanner и больших client pool
- используй `method-family.metadata_read`, чтобы ограничить scan-heavy metadata discovery
- используй `catalog.<name>`, чтобы один hot catalog не выедал остальные
- для `ddl`, `txn` и `lock` обычно стоит держать лимиты строже, чем для обычных metadata read

## Frontend profile и runtime jars

Можно выбрать, какую версию HMS proxy объявляет наружу:

```properties
compatibility.frontend-profile=APACHE_3_1_3
```

или для Hortonworks клиентов:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
```

или для клиентов HDP `3.1.0.3.1.5.6150-1`:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
```

### Несколько front-end listener'ов на разных портах

В Thrift-протоколе нет version-negotiation handshake, поэтому proxy не может по первому
запросу определить версию клиента (так же как HiveServer2 разделяет binary и HTTP по портам).
Когда нужны два разных типа клиентов с разными `getVersion()` identity или разными runtime
jars — поднимаем **несколько Thrift listener'ов на разных портах**:

```properties
server.port=9083
compatibility.frontend-profile=APACHE_3_1_3

additional-frontends=hdp
additional-frontends.hdp.port=9084
additional-frontends.hdp.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
additional-frontends.hdp.standalone-metastore-jar=hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

Primary listener живёт на `server.port` со своим `compatibility.frontend-profile`. Каждый
блок `additional-frontends.<name>.*` задаёт независимый listener:

| Ключ | Обязательный | По умолчанию |
| --- | --- | --- |
| `additional-frontends.<name>.port` | да | — |
| `additional-frontends.<name>.frontend-profile` | да | — |
| `additional-frontends.<name>.standalone-metastore-jar` | да для не-`APACHE_3_1_3` | — |
| `additional-frontends.<name>.bind-host` | нет | `server.bind-host` |
| `additional-frontends.<name>.min-worker-threads` | нет | `server.min-worker-threads` |
| `additional-frontends.<name>.max-worker-threads` | нет | `server.max-worker-threads` |
| `additional-frontends.<name>.client-socket-timeout-ms` | нет | `server.client-socket-timeout-ms` |
| `additional-frontends.<name>.tcp-keepalive` | нет | `server.tcp-keepalive` |
| `additional-frontends.<name>.tcp-keepalive-idle-seconds` | нет | `server.tcp-keepalive-idle-seconds` |
| `additional-frontends.<name>.tcp-keepalive-interval-seconds` | нет | `server.tcp-keepalive-interval-seconds` |
| `additional-frontends.<name>.tcp-keepalive-count` | нет | `server.tcp-keepalive-count` |

Все listener'ы используют один `RoutingMetaStoreProxy`, federation, security
(`FrontDoorSecurity` включая SASL/Kerberos), audit и Prometheus метрики. Отличается
только wire-level Thrift API на конкретном порту.

Дополнительные listener'ы работают на daemon-потоках и останавливаются раньше, чем
закрываются router и front-door security, поэтому авария при старте одного listener'а не
оставит JVM живой с занятым портом. См.
[Жизненный цикл front-door сокетов и shutdown](#жизненный-цикл-front-door-сокетов-и-shutdown).

У каждого listener должен быть собственный `host:port`. Старт падает, если additional frontend
конфликтует с основным listener, с другим additional frontend или с management-листенером —
включая дефолтный `server.port + 1000`, — а wildcard bind host вроде `0.0.0.0` считается
конфликтом с любым хостом на том же порту.

Для полноценного Hortonworks frontend нужно указать HDP `standalone-metastore` jar:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

Для HDP `3.1.0.3.1.5.6150-1` используй соответствующий jar:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

Для isolated Hortonworks backend runtime можно указать backend jar:

```properties
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

или:

```properties
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

Backend runtime задаётся явно по каталогу. Если `catalog.<name>.runtime-profile` не указан, для
этого каталога используется `APACHE_3_1_3`:

```properties
catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_0_78
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar

catalog.apache.runtime-profile=APACHE_3_1_3
```

Для HDP `3.1.0.3.1.5.6150-1` профиль и jar задаются аналогично:

```properties
catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

С этим jar proxy может открыть выбранный Hortonworks backend runtime в isolated classloader.
Выбор runtime не autodetect'ится по версии backend сервера, а берётся из
`catalog.<name>.runtime-profile`.

Для front door proxy поднимает Hortonworks thrift `Processor` в isolated classloader и автоматически
бриджит общие RPC в внутренний Apache `3.1.3` handler. Поддержанные HDP-only методы:

- `get_database_req` -> `get_database` для HDP `3.1.0.3.1.5.6150-1`
- `create_table_req` -> `create_table` / `create_table_with_environment_context` / `create_table_with_constraints`
- `truncate_table_req` -> `truncate_table`
- `alter_table_req` -> `alter_table` / `alter_table_with_environment_context`
- `alter_partitions_req` -> `alter_partitions` / `alter_partitions_with_environment_context`
- `rename_partition_req` -> `rename_partition`
- `get_partitions_by_names_req` -> `get_partitions_by_names`
- `update_table_column_statistics_req` -> `set_aggr_stats_for`
- `update_partition_column_statistics_req` -> `set_aggr_stats_for`
- `add_write_notification_log` -> прямой Hortonworks passthrough только в Hortonworks backend
- `get_tables_ext` -> прямой Hortonworks passthrough только в Hortonworks backend `3.1.0.3.1.5.6150-1`
- `get_all_materialized_view_objects_for_rewriting` -> прямой Hortonworks passthrough только в Hortonworks backend `3.1.0.3.1.5.6150-1` через `routing.default-catalog`

Замечания по view / materialized view:
- переписывание SQL работает только при `federation.view-text-rewrite.mode=REWRITE`
- rewrite сделан intentionally parser-less: лексический сканер отслеживает table-позиции
  (`FROM`, `JOIN`, `INTO`, `TABLE`, `UPDATE`) и переписывает только database-квалификатор ссылки,
  стоящей в такой позиции; полный Hive SQL grammar не разбирается
- string literals, комментарии `--` и `/* */`, числа и идентификаторы в backquote пропускаются,
  поэтому их содержимое никогда не переписывается; квалификаторы колонок и алиасы таблиц
  (`t.col` в `select t.col from sales.orders t`) не трогаются, даже если совпадают с именем БД
- ссылки вида `catalog.db.table` сохраняют catalog-префикс: на выходе схлопывается только
  `<backend catalog>.<db>.<table>` в имя внешней БД, а любой другой catalog-префикс остаётся
  нетронутым, а не переписывается молча на другой namespace
- входящие cross-catalog ссылки вроде `catalog2__dim.table_x` internalize'ятся для backend, но на
  выходе гарантированно переписывается только namespace текущей таблицы
- всё, что нельзя разрешить однозначно, остаётся нетронутым и логируется на уровне `DEBUG` в
  `ViewDefinitionCompatibility`; непереписанная ссылка проявится как явная ошибка backend, а не
  как молча испорченное определение вью
- по умолчанию переписывается только `viewExpandedText`
  (`federation.view-text-rewrite.preserve-original-text=true`), то есть клиентский
  `viewOriginalText` не мутируется; поставь `false`, если нужно переписывать и сохранённый
  оригинальный SQL
- диалектные конструкции (подстановки вроде `${hiveconf:db}`, макросы, engine-specific hints) вне
  области rewrite — их по-прежнему стоит проверить отдельным smoke тестом в вашей среде

## ACID / txn / lock policy

- request-based ACID методы с routable namespace в payload, например
  `get_valid_write_ids`, `allocate_table_write_ids`, `compact`, `compact2`,
  `add_dynamic_partitions`, `fire_listener_event`, `repl_tbl_writeid_state`,
  маршрутизируются по payload
- id-only lifecycle методы, например `open_txns`, `commit_txn`, `abort_txn`,
  `abort_txns`, `check_lock`, `unlock`, `heartbeat`, `heartbeat_txn_range`,
  привязаны к `routing.default-catalog`
- это осознанная модель: proxy не пытается быть distributed ACID coordinator между
  несколькими backend metastore

## Логирование

Комплектный `log4j.properties` — это конфигурация по умолчанию; proxy подхватывает её в runtime,
если appender'ы не сконфигурированы, так что обычный запуск `java -jar ...` тоже пишет логи.
Дефолты:

- root logger на `INFO`, вывод в stderr и в `logs/hms-proxy.log` (rolling по 50MB, 10 backup)
- пакет proxy `io.github.mmalykhin.hmsproxy` на `INFO`
- audit logger `io.github.mmalykhin.hmsproxy.audit` на `INFO` в отдельный
  `logs/hms-proxy-audit.log`

Любую часть можно переопределить своим `log4j.properties` на classpath.

Binding — `slf4j-reload4j`. reload4j это drop-in форк EOL-версии log4j 1.2.17 с вырезанными
известными CVE, поэтому формат конфигурации остаётся log4j 1.x и существующие `log4j.properties`
продолжают работать без изменений. Единственное ограничение: то, что из reload4j убрали —
`org.apache.log4j.jmx.*`, viewer `lf5` и `NTEventLogAppender`, — в своей конфигурации больше
использовать нельзя.

### Debug tracing

Per-request debug tracing **по умолчанию выключен**: он рендерит через `DebugLogUtil` все аргументы
запроса и все backend-ответы, а это реальные CPU и аллокации на каждый RPC. Включай его осознанно —
для конкретного окружения или на время инцидента:

```properties
log4j.logger.io.github.mmalykhin.hmsproxy=DEBUG
```

Когда он включён, каждый клиентский вызов получает `requestId`, а в логах есть:

- входящий HMS method и аргументы
- выбранный backend catalog
- proxied thrift method и переписанные аргументы
- backend response или backend error
- итоговый client response или client-visible error

Рендер значений ограничен (10 элементов на коллекцию, глубина 3, ~4000 символов на запись), но на
нагруженном proxy объём всё равно заметный. По окончании возвращай уровень на `INFO`.

Учти, что на `INFO` для пакета proxy остаются trace-строки write-пути
`trace stage=client-request` / `backend-request`, поэтому для большинства операционных разборов
`DEBUG` не нужен.

## HiveServer2

Укажи в HiveServer2 `hive.metastore.uris` на proxy вместо одного backend HMS.
Для multi-catalog deployment лучше использовать Hive/клиентов, которые сохраняют catalog fields.

Для Beeline/HS2 обычно удобнее separator `__`, чем `.`:

```properties
routing.catalog-db-separator=__
```

Это меняет только внешний legacy формат имени из канонической модели маршрутизации выше.

Если metadata writes через proxy ведут себя не так, как напрямую против backend HMS, можно
попробовать:

```properties
federation.preserve-backend-catalog-name=true
```

Это меняет только возвращаемые `catName`/`catalogName`, обычно на backend-значения вроде `hive`.
Выбор backend по-прежнему следует канонической модели маршрутизации выше.

Если нагрузки используют Hive views или materialized views между несколькими catalog, имеет смысл
сразу прогонять и такой режим:

```properties
federation.view-text-rewrite.mode=REWRITE
```

Это переписывает SQL внутри view между внешними и внутренними именами. `viewOriginalText` по
умолчанию сохраняется (`federation.view-text-rewrite.preserve-original-text=true`); ставь `false`
только если нужно переписывать и сохранённый оригинальный SQL. На выбор backend для самого RPC ни
одна из настроек не влияет.

Если нужно, чтобы proxy физически удалял данные external table на backend Apache `3.1.3`
после `DROP TABLE`, включи:

```properties
federation.external-table-drop-purge.mode=BEST_EFFORT
catalog.catalog2.conf.hms.proxy.external-table-drop-purge.allowed-prefixes=hdfs://ns-catalog2/tmp/,hdfs://ns-catalog2/data/external/
```

Сейчас этот хук работает только для `drop_table` и `drop_table_with_environment_context`,
только для backend runtime `APACHE_3_1_3`, только если таблица уже `EXTERNAL_TABLE` с
`external.table.purge=true`, и только если квалифицированный `LOCATION` попадает в allowlist.
Удаление данных происходит только после успешного удаления метаданных в backend metastore;
ошибка purge логируется и не откатывает metadata delete. Allowlist лучше держать узким, потому
что подходящие пути удаляются рекурсивно через Hadoop `FileSystem`.

Рекурсивное удаление выполняется в небольшом фоновом пуле (потоки с именами
`hms-proxy-drop-purge-*`), поэтому `drop_table` отвечает сразу после удаления метаданных и не
держит Thrift worker всё время удаления. Значит, успешный `DROP TABLE` ещё не гарантирует, что
данные уже удалены — результат purge смотри в логе proxy. При остановке proxy запущенные purge
дожидаются завершения; оставшиеся в очереди логируются и пропускаются.

## Iceberg REST Catalog frontend

Proxy дополнительно умеет поднять параллельный HTTP listener со спецификацией
Iceberg REST Catalog, использующий тот же routing/federation pipeline что и
Thrift HMS front door. Статус: **экспериментально**; вся write-поверхность,
которую выставляет `RESTCatalogAdapter`, — write таблиц (create, commit, drop,
rename, register), write view (create, commit, drop, rename) и namespace DDL
(create, update properties, drop), а также multi-table transaction commit —
поддержана, но **только когда целевой namespace резолвится в
`routing.default-catalog`**. Iceberg-клиенты (PyIceberg, Spark `iceberg-rest`,
Trino `iceberg-rest`) могут discover и load Iceberg-таблицы, хранящиеся в HMS
через стандартный параметр `metadata_location`.

Write таблиц и gate «только default-каталог» появились первыми; write view и
multi-table transaction commit к этому моменту уже были достижимы — их
REST-путь идёт через тот же общий `RoutingHiveCatalog`/`RoutingMetaStoreClient`,
которым пользуется write таблиц, и `WriteRouteGate` уже классифицировал все
тринадцать роутов как write — их просто ещё не объявляли в `GET /v1/config` и
не покрывали smoke. Namespace DDL — по-настоящему новое: `RoutingMetaStoreClient`
не реализовывал `createDatabase`, `alterDatabase` и `dropDatabase` до сих пор,
поэтому `POST /v1/{prefix}/namespaces` и соседние роуты отвечали
`UnsupportedOperationException` независимо от каталога.

**Почему writes работают только в default-каталоге:** реальным HMS-локом
подкреплены только таблицы дефолтного каталога (см. [ZooKeeper storage для
synthetic read locks](#zookeeper-storage-для-synthetic-read-locks));
любой другой каталог обслуживается синтетическим lock shim, который выдаёт
`EXCLUSIVE`-лок безусловно, без проверки конфликтов. Commit, направленный
туда, решил бы, что владеет таблицей, молча гоняясь наперегонки с — и,
возможно, проигрывая — конкурентным writer'ом, что портит `metadata.json` без
единого сообщения о конфликте. `WriteRouteGate` проверяет это на
**резолвленном** каталоге, а не на prefix из URL запроса: prefix дефолтного
каталога тоже выставляет базы всех остальных каталогов под federated-именами
`<catalog><separator><db>` (см. [Поддерживаемые endpoint'ы](#поддерживаемые-endpointы)
ниже), так что create по
`/v1/{default-prefix}/namespaces/apache__default/tables` отказывается точно
так же, как прямой create по `/v1/apache/namespaces/default/tables` — оба
резолвятся в каталог `apache` и получают один и тот же `403`
(`ForbiddenException`). Это касается всех тринадцати write-роутов из таблицы
ниже одинаково — write таблиц, view и namespace DDL, transaction commit.
`GET /v1/config` и `GET /v1/{prefix}/config` объявляют эту асимметрию
напрямую: в `endpoints` дефолтного каталога перечислены все тринадцать
write-роутов из таблицы ниже, у любого другого каталога — только девять
read-роутов, так что спецификация-совместимый клиент узнаёт об ограничении
из discovery, а не из проваленного запроса.

Включается так:

```properties
rest-catalog.enabled=true
rest-catalog.port=9183
# Опционально, но рекомендуется для прода: SPNEGO. Требует security.mode=KERBEROS.
rest-catalog.kerberos.principal=HTTP/_HOST@EXAMPLE.COM
rest-catalog.kerberos.keytab=/etc/security/keytabs/spnego.service.keytab
# Опционально: ограничить то, что вправе удалить purge (см. раздел про purge ниже).
rest-catalog.purge.mode=ALLOWLIST
rest-catalog.purge.allowed-prefixes=hdfs://ns-default/warehouse/tablespace/
```

`rest-catalog.purge.allowed-prefixes` обязателен для `ALLOWLIST` и отвергается
при двух других режимах; обе противоречивые комбинации роняют прокси на старте,
а не игнорируются.

### Как сохранить Iceberg-таблицу читаемой для Hive

Iceberg на каждом коммите пишет один из двух storage descriptor'ов. С включённым
Hive-движком — `storage_handler` и конкретные
`HiveIcebergInputFormat`/`OutputFormat`/`SerDe`; с выключенным — абстрактные
`FileInputFormat`/`FileOutputFormat`/`LazySimpleSerDe`, а `storage_handler`
удаляет. Выбор определяется свойством самой таблицы `engine.hive.enabled`, а
если таблица его не задаёт — значением `iceberg.engine.hive.enabled` в
конфигурации того движка, который коммитит.

Таблица, созданная через `STORED BY ICEBERG` в Hive 4, такого свойства не несёт,
поэтому один-единственный коммит мог оставить её нечитаемой для клиентов Hive
3.1: они падают с `Cannot create an instance of InputFormat class
org.apache.hadoop.mapred.FileInputFormat`. Этому мешают две настройки, по одной
на каждый путь записи, обе включены по умолчанию:

| Ключ | Какой путь закрывает |
| --- | --- |
| `rest-catalog.hive-engine-descriptor` | Собственные REST-коммиты прокси: они пишут Hive-совместимый дескриптор. Таблица, задавшая `engine.hive.enabled` сама, всегда перевешивает эту настройку, а явный `catalog.<name>.conf.iceberg.engine.hive.enabled` уважается, а не перетирается. |
| `routing.iceberg-pointer-guard.hive-engine-descriptor` | Коммиты, приходящие по Thrift от движка, который прокси настроить не может, — например `INSERT` от Hive 3.1. Pointer guard сохраняет дескриптор, уже лежащий в записи метастора, вместо того чтобы дать запросу его обнулить. |

Guard дескриптор только **сохраняет** и никогда не навязывает: запись без
`storage_handler` — это таблица, которой нечего терять, и её alter'ы проходят
нетронутыми. Таблица, испорченная до появления этих настроек, чинится сама на
следующем же коммите.

Запросы к этому listener'у покрыты Prometheus-метриками из раздела
[Prometheus-метрики](#prometheus-метрики): `hms_proxy_rest_requests_total`,
`hms_proxy_rest_request_duration_seconds` и `hms_proxy_rest_listener_info`.

### Поддерживаемые endpoint'ы

| Endpoint                                              | Статус                                  |
| ----------------------------------------------------- | --------------------------------------- |
| `GET /v1/config`                                      | поддержан                               |
| `GET /v1/{prefix}/config`                             | поддержан                               |
| `GET /v1/{prefix}/namespaces`                         | поддержан                               |
| `GET /v1/{prefix}/namespaces/{ns}`                    | поддержан                               |
| `GET /v1/{prefix}/namespaces/{ns}/tables`             | поддержан (только Iceberg-таблицы)      |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{tbl}`       | поддержан (только Iceberg-таблицы)      |
| `GET /v1/{prefix}/namespaces/{ns}/views`              | поддержан (реальный листинг; пустой, если Iceberg-view нет) |
| `GET /v1/{prefix}/namespaces/{ns}/views/{view}`       | поддержан (только Iceberg-view)          |
| `HEAD /v1/{prefix}/namespaces/{ns}`                    | поддержан (204, если существует, 404 — если нет) |
| `HEAD /v1/{prefix}/namespaces/{ns}/tables/{tbl}`      | поддержан (204, если существует, 404 — если нет) |
| `HEAD /v1/{prefix}/namespaces/{ns}/views/{view}`      | поддержан (204, если существует, 404 — если нет) |
| `POST /v1/{prefix}/namespaces/{ns}/tables`             | поддержан только для дефолтного каталога (create); иначе `403` |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}`       | поддержан только для дефолтного каталога (commit/update); иначе `403` |
| `DELETE /v1/{prefix}/namespaces/{ns}/tables/{tbl}`    | поддержан только для дефолтного каталога (drop, включая `?purgeRequested=true`); иначе `403` |
| `POST /v1/{prefix}/tables/rename`                      | поддержан только для дефолтного каталога (rename); иначе `403` |
| `POST /v1/{prefix}/namespaces/{ns}/register`           | поддержан только для дефолтного каталога (register); иначе `403` |
| `POST /v1/{prefix}/namespaces/{ns}/views`               | поддержан только для дефолтного каталога (create); иначе `403` |
| `POST /v1/{prefix}/namespaces/{ns}/views/{view}`       | поддержан только для дефолтного каталога (commit/update); иначе `403` |
| `DELETE /v1/{prefix}/namespaces/{ns}/views/{view}`     | поддержан только для дефолтного каталога (drop); иначе `403` |
| `POST /v1/{prefix}/views/rename`                        | поддержан только для дефолтного каталога (rename); иначе `403` |
| `POST /v1/{prefix}/namespaces`                          | поддержан только для дефолтного каталога (create); иначе `403` |
| `POST /v1/{prefix}/namespaces/{ns}/properties`          | поддержан только для дефолтного каталога (update properties); иначе `403` |
| `DELETE /v1/{prefix}/namespaces/{ns}`                   | поддержан только для дефолтного каталога (drop); иначе `403` |
| `POST /v1/{prefix}/transactions/commit`                 | поддержан только для дефолтного каталога (multi-table commit); иначе `403` |

`{prefix}` — любой каталог, перечисленный в `catalogs=`: каждый настроенный
каталог получает собственный REST prefix, `/v1/<catalog>/...`. `GET
/v1/config` поддерживает warehouse discovery: передайте `?warehouse=<catalog>`,
и в ответе `overrides.prefix` назовёт этот каталог, так что клиент может
привязаться к нужному prefix, не зашивая его в конфигурацию (см. примеры
клиентов ниже). Без `warehouse` `/v1/config` объявляет `routing.default-catalog`
— как и в phase 1; неизвестное значение `warehouse` возвращает HTTP 400
(`BadRequestException`). Поле `endpoints` в ответе перечисляет девять
read-роутов из таблицы выше для любого каталога (list/load namespace +
namespace-exists, list/load table + table-exists, list/load view +
view-exists); у дефолтного каталога `endpoints` дополнительно несёт все
тринадцать write-роутов из таблицы выше (write таблиц, view и namespace DDL,
transaction commit), так что современный клиент может обнаружить
write/read-асимметрию между каталогами через discovery, а не из
провалившегося запроса. `GET /v1/{prefix}/config` отвечает так же, но из
собственного handler'а прокси — `overrides.prefix` называет каталог из
сегмента пути, а не из query-параметра `warehouse`, — и неизвестный prefix
здесь по-прежнему даёт 404.

Отказ в write отвечает `403` (`ForbiddenException`) с сообщением, называющим
резолвленный каталог, например: `Writes are only supported in the default
catalog 'hdp'; namespace 'apache__default' belongs to catalog 'apache',
which is served by the synthetic lock shim and provides no writer
isolation.` Это проверяется на namespace, в который запрос **резолвится**, а
не на prefix из URL — см. [Почему writes работают только в
default-каталоге](#iceberg-rest-catalog-frontend) выше.

`DELETE .../tables/{tbl}?purgeRequested=true` обслуживается как настоящий
purge — так же, как его обслуживает собственный REST-каталог Iceberg: прокси
дропает таблицу в metastore, а затем сам удаляет её data- и metadata-файлы,
обходя манифесты таблицы, чтобы их найти. Удаление идёт внутри JVM прокси под
его собственными credentials и завершается до отправки `204`. Write-гейт
удерживает purge внутри дефолтного каталога: purge, чей namespace резолвится в
другой каталог, отказывается тем же `403`, что и любой другой write, до того как
будет тронут хоть один файл.

Что именно purge вправе удалить внутри этого каталога, ограничивает
`rest-catalog.purge.mode`:

| Режим | Поведение |
| --- | --- |
| `ALLOW` (по умолчанию) | Удаляет всё, на что указывают метаданные и манифесты таблицы, — так же, как REST-каталоги Iceberg. Поведение прежних версий не меняется. |
| `ALLOWLIST` | Purge разрешён только под префиксами `rest-catalog.purge.allowed-prefixes`. |
| `REFUSE` | Отвечает `403` на любой purge; `DELETE` без `purgeRequested` по-прежнему дропает таблицу и оставляет её файлы. |

Purge, который политика не разрешает, отклоняется **до дропа таблицы**, поэтому
после отказа целы и таблица, и её файлы, а клиент может повторить запрос без
`purgeRequested`.

`ALLOWLIST` проверяет границу дважды. До дропа с префиксами сверяются location
таблицы и её `metadata.json`; выход за границу любого из них даёт `403`. Во
время удаления каждый путь, который Iceberg просит удалить — data-файлы,
delete-файлы, манифесты, manifest list, metadata-файлы, — сверяется снова, и
путь вне префиксов пропускается с WARN вместо удаления. Именно вторая проверка
важна для недоверенных клиентов: в REST-протоколе манифесты пишет клиент,
поэтому коммит может указать снапшот на файлы в чужом дереве, а эти пути
становятся известны только при обходе манифестов — уже после того, как
предварительная проверка прошла.

Error-ответы несут смапленный HTTP-статус, `type` и `message`, но никогда —
server stack trace, потому что этот listener может быть доступен без
аутентификации. Тело запроса, которое не удаётся распарсить, отвечает 400
(`BadRequestException`) вместо падения в 500; это касается любого роута,
принимающего тело. `HEAD`-ответ никогда не пишет тело, как того требует RFC
9110, — включая error-статусы, — так что exists-check на отсутствующий
namespace, таблицу или view возвращает обычный 404 без тела, а не 404 с
JSON-телом. Диспетчер запросов ловит `Throwable`, а не только `Exception`:
любой `Error`, который ускользнул бы из обработки (например,
`NoSuchMethodError` от несовпадения версий зависимостей, всплывший глубоко
внутри write), теперь маппится в обычный error-ответ, а не улетает мимо
handler'а, оставляя соединение клиента висеть без ответа навсегда.

Prefix дефолтного каталога сохраняет federated-представление из phase 1: его
собственные базы плюс базы всех остальных каталогов под именами
`<catalog><separator><db>` (см. [HiveServer2](#hiveserver2) выше). Любой
другой prefix — чистое представление: только собственные базы этого
каталога, под их внутренними именами — federated-имена
`<catalog><separator><db>` в non-default prefix не просачиваются. На
неизвестный prefix по-прежнему отвечает 404 (`NoSuchCatalogException`).

### Настройка SPNEGO

SPNEGO по RFC требует principal вида `HTTP/<host>@REALM`. Это **отдельный**
principal от `security.server-principal` (обычно `hms/<host>@REALM` для
Thrift listener). Оба могут лежать в одном keytab или в двух разных. REST
listener делает `UserGroupInformation.loginUserFromKeytabAndReturnUGI`, чтобы
получить отдельный UGI и не перезаписать Thrift'овый — оба сосуществуют в
одном JVM.

### Примеры клиентов

PyIceberg:

```python
from pyiceberg.catalog.rest import RestCatalog
catalog = RestCatalog("my-catalog", **{
    "uri": "http://hms-proxy:9183",
})
```

Spark:

```properties
spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.my_catalog.uri=http://hms-proxy:9183
```

Чтобы обратиться к non-default каталогу, передайте `warehouse=<catalog>`:
клиент отправит его в `GET /v1/config` при discovery и привяжется к prefix
этого каталога для всех последующих запросов:

```python
catalog = RestCatalog("sales-catalog", **{
    "uri": "http://hms-proxy:9183",
    "warehouse": "sales",
})
```

```properties
spark.sql.catalog.sales_catalog.warehouse=sales
```

### Особенности и ограничения

- Iceberg REST по дизайну работает **только с Iceberg-таблицами**. Native
  Hive-таблицы (parquet/orc/text без `metadata_location`) HiveCatalog
  отфильтровывает, и через REST их не видно. Для native Hive продолжайте
  использовать Thrift listener.
- `RoutingHiveCatalog` использует reflection на private поле
  `HiveCatalog.clients`, привязанное к Iceberg `1.9.2`. При апгрейде Iceberg
  обязательно прогнать `RoutingHiveCatalogTest`, чтобы убедиться, что inject
  ещё работает.
- Write таблицы открывает output stream в HDFS (`metadata.json`) прямо из
  JVM прокси — это первый путь в прокси, который так делает; чтение идёт по
  другому, незатронутому classpath. Это требует, чтобы `hadoop-hdfs` и
  `hadoop-common` были одной версии в дереве зависимостей. Мавеновская
  медиация никогда их не сравнивала (это разные artifact ID), поэтому
  `orc-core` (приходит транзитивно через `hive-standalone-metastore`) тянул
  устаревший `hadoop-hdfs:2.2.0` рядом с `hadoop-common:2.6.0` в другом месте
  дерева, и каждый write падал с `NoSuchMethodError:
  FSOutputSummer.<init>` глубоко внутри `DFSOutputStream`. `pom.xml` теперь
  исключает этот транзитивный `hadoop-hdfs` и напрямую зависит от
  `hadoop-hdfs:2.6.0`, чтобы совпасть с `hadoop-common`. Держите обе версии
  согласованными, если переопределяете любую из них.
- Purge читает манифесты таблицы через Avro. `iceberg-core:1.9.2` собран
  против `avro:1.12.0`, а `hadoop-mapreduce-client-core:2.6.5` тянет
  `avro:1.7.4` на той же глубине дерева — Maven выбирал 1.7.4 по порядку
  объявления, и любой purge падал с `NoClassDefFoundError:
  org/apache/avro/LogicalTypes`. `pom.xml` пинит `avro:1.12.0`; больше никто на
  этом classpath не вызывает Avro API. Кодеки snappy, xz и zstd объявлены в
  Avro как optional и в fat jar намеренно не попадают: манифест всегда пишется
  с deflate, который Avro сжимает штатным JDK-шным Deflater. Свойство
  `write.avro.compression-codec` этого не меняет — оно управляет data-файлами и
  delete-файлами, которые прокси не читает, а `ManifestWriter` в Iceberg вообще
  не передаёт свойства таблицы в writer манифеста. Это закреплено тестом
  (`dropTableWithPurgeReadsManifestsOfATableAskingForSnappy`): если будущий
  Iceberg начнёт учитывать свойство для манифестов, упадёт тест, а не прод.

## Безопасность

### Без Kerberos

```properties
server.port=9083
security.mode=NONE

catalogs=warehouse
catalog.warehouse.conf.hive.metastore.uris=thrift://hms-backend:9083
routing.default-catalog=warehouse
```

### С Kerberos

Безопасность делится на две независимые части: front door (клиенты -> proxy) и backend
connections (proxy -> HMS).

**Front door**:

```properties
security.mode=KERBEROS
security.server-principal=hive/_HOST@REALM.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab
security.client-principal=hive/_HOST@REALM.COM
security.client-keytab=/etc/security/keytabs/hms-proxy-client.keytab
```

`security.server-principal` и `security.keytab` обязательны при `security.mode=KERBEROS`.
`_HOST` разворачивается в каноническое имя хоста proxy перед Kerberos login. Если DNS-имя
хоста не совпадает с principal в keytab/KDC, используй явный FQDN.

Когда Kerberos включён на фронте, delegation-token методы
(`get_delegation_token`, `renew_delegation_token`, `cancel_delegation_token`)
обслуживаются локально самим proxy.

### Front-door proxy-user rules для delegation-token issuance

`hadoop.proxyuser.<service>.*` не относится к подключению к ZooKeeper. Эта настройка нужна только
в сценарии, когда service principal вроде HiveServer2 просит proxy выдать delegation token для
конечного пользователя через `get_delegation_token("alice", ...)`.

Пример:

```properties
security.front-door-conf.hadoop.proxyuser.hive.hosts=hs2-1.example.com,hs2-2.example.com
security.front-door-conf.hadoop.proxyuser.hive.groups=*
```

### ZooKeeper token storage

Для persistent storage delegation tokens можно включить `ZooKeeperTokenStore` через обычный
`HiveConf` или напрямую в `hms-proxy.properties`:

```properties
security.front-door-conf.hive.cluster.delegation.token.store.class=org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString=zk1:2181,zk2:2181,zk3:2181
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.znode=/hms-proxy-delegation-tokens
# Опционально: ACL для новых znode, которые создаёт token store.
# security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.acl=sasl:hive:cdrwa
# Опционально: максимальный lifetime токена в миллисекундах.
# security.front-door-conf.hive.cluster.delegation.token.max-lifetime=604800000
```

Если при этом включён `security.mode=KERBEROS`, proxy автоматически прокинет
`hive.metastore.kerberos.principal` и `hive.metastore.kerberos.keytab.file` в front-door `HiveConf`
из `security.server-principal` и `security.keytab`, чтобы встроенный `ZooKeeperTokenStore`
аутентифицировался в ZooKeeper по SASL/Kerberos. Если для ZooKeeper нужны отдельные credentials,
задай эти `hive.metastore.kerberos.*` явно через `security.front-door-conf.*`.

То есть по умолчанию для подключения к ZooKeeper используются:

- principal: `security.server-principal`
- keytab: `security.keytab`

Это именно front-door credentials proxy. Они не берутся из
`security.client-principal` и `security.client-keytab`, потому что те параметры относятся к
исходящим подключениям proxy к backend HMS.

При старте proxy также заранее настраивает JAAS entry `HiveZooKeeperClient` для front-door
token store из этих же credentials до запуска локального delegation-token manager. Обычно это
означает, что отдельный `-Djava.security.auth.login.config` только для ZooKeeper не нужен.

Если для ZooKeeper нужен другой principal/keytab, их можно переопределить явно:

```properties
security.front-door-conf.hive.metastore.kerberos.principal=hive-zk/_HOST@REALM.COM
security.front-door-conf.hive.metastore.kerberos.keytab.file=/etc/security/keytabs/hms-proxy-zk.keytab
```

### ZooKeeper storage для synthetic read locks

У proxy есть узкий synthetic lock shim для non-default catalog. Он покрывает non-ACID
`SHARED_READ` `SELECT` lock, допустимые non-transactional `NO_TXN` DDL lock вроде
`CREATE TABLE` и partition rename/drop, которые Hive всё равно ведёт через txn/lock API, и
non-transactional write lock (`INSERT`, `UPDATE`, `DELETE`) — те самые, которые Hive берёт на
`INSERT` в non-ACID таблицу non-default каталога. Такие lock proxy обслуживает локально, когда
backend txn id рассинхронизированы между каталогами. Это не превращает proxy в distributed ACID
coordinator.

Write lock не ограничены по типу лока: при дефолтном `hive.txn.strict.locking.mode=true` Hive
берёт на `INSERT` в non-ACID таблицу `EXCLUSIVE` lock, `SHARED_WRITE` — только при выключенном
strict locking, а `INSERT OVERWRITE` всегда `EXCLUSIVE`. Компонент с `isTransactional=true`
никогда не обслуживается синтетически: он относится к ACID-таблице, write id для которой умеет
выдавать только TxnHandler default-каталога, поэтому запрос уходит в backend и падает там. ACID
таблиц в non-default каталоге всё равно быть не может — proxy отклоняет `create_table` с
`transactional=true` вне default-каталога и не пропускает `allocate_table_write_ids` /
`get_valid_write_ids` для non-default каталогов.

**Shim выдаёт локи, не проверяя их конфликты.** Он сразу отвечает `ACQUIRED` и не смотрит на
другие живые локи, поэтому параллельные `INSERT`, `INSERT OVERWRITE` и DDL по одной таблице
non-default каталога не сериализуются друг относительно друга — как и клиенты, которые ходят в
этот metastore мимо proxy. То есть non-default каталог не даёт ни ACID-гарантий, ни изоляции
писателей; если нагрузке нужно взаимное исключение, держите её в `routing.default-catalog` или
сериализуйте писателей сами. Access mode каталога при этом соблюдается: write lock для
`READ_ONLY` каталога или для базы вне `catalog.<name>.write-db-whitelist` отклоняется
`MetaException`, а не синтезируется.

Пригодность определяется по всем `LockComponent` запроса, а не только по первому. Вызов `lock`
берётся, маршрутизируется и подтверждается целиком и возвращает один lock id, поэтому его нельзя
переслать больше чем в один backend. Но запрос, читающий несколько каталогов — или всего лишь две
базы одного каталога, — приходит именно в таком виде: одним запросом, компоненты которого
принадлежат разным namespace. Proxy расщепляет его: компоненты одного каталога уходят в его
metastore, каждый переписанный в свою backend-базу, а компоненты остальных каталогов из запроса к
backend удаляются.

Целью маршрутизации выбирается default catalog, если он присутствует: ему принадлежит TxnHandler,
и его локи — настоящие. Отброшенные компоненты при этом не теряют ничего, что было бы удержано:
non-default каталог обслуживается описанным выше shim, который записывает лок и сразу отвечает
`ACQUIRED`, ни разу не проверив конфликт. Отброшенный компонент теряет запись в этом журнале, а не
гарантию. Проверка access mode за решением о маршрутизации не следует — запись в `READ_ONLY`
каталог отклоняется независимо от того, пережил ли её компонент расщепление. Каждое расщепление
пишется в лог и считается метрикой `hms_proxy_lock_request_split_total`.

Единственное исключение — плейсхолдер Hive для `INSERT ... VALUES` (`_dummy_database`/
`_dummy_table`, константа `SemanticAnalyzer.DUMMY_DATABASE`): он не существует ни в одном
metastore и не относится ни к какому каталогу, поэтому его компоненты не выбирают каталог, не
считаются вторым namespace и не переписываются в реальную backend-базу. Запрос, состоящий
только из таких компонентов, уходит в default catalog, которому принадлежит TxnHandler.
Единственное исключение — псевдоисточник Hive `_dummy_database._dummy_table`: `INSERT ... VALUES`
и запросы без `FROM` берут lock на нём вместе с реальной целевой таблицей, поэтому такой запрос
всегда называет две базы. Этой псевдотаблицы нет ни в одном metastore и блокировать на ней нечего,
поэтому proxy пропускает её и при выборе namespace, и при проверке пригодности для shim. Запрос,
в котором есть только псевдоисточник, по-прежнему идёт по пину в default catalog.

`synthetic-read-lock.store.mode` обязан быть задан явно — default'а нет. Используйте `IN_MEMORY`
для одиночного инстанса proxy (synthetic-локи на non-default каталогах теряются при рестарте или
failover через load balancer, и proxy пишет `WARN` в лог на старте, чтобы это было видно), либо
`ZOOKEEPER` для HA / load-balanced deployment, чтобы `check_lock`, `unlock`, `heartbeat`,
`commit_txn` и `abort_txn` продолжали работать через соседний proxy после падения первого.

`mode=IN_MEMORY` вместе с любым заданным `synthetic-read-lock.store.zookeeper.*` — ошибка старта:
ZooKeeper-настройки были бы проигнорированы, а локи молча остались бы в памяти. Обратное
направление сохранено: ZooKeeper-ключи без явного `mode` включают `ZOOKEEPER`.

Пример:

```properties
synthetic-read-lock.store.mode=ZOOKEEPER
synthetic-read-lock.store.zookeeper.connect-string=zk1:2181,zk2:2181,zk3:2181
synthetic-read-lock.store.zookeeper.znode=/hms-proxy-synthetic-read-locks
# synthetic-read-lock.store.zookeeper.connection-timeout-ms=15000
# synthetic-read-lock.store.zookeeper.session-timeout-ms=60000
# synthetic-read-lock.store.zookeeper.base-sleep-ms=1000
# synthetic-read-lock.store.zookeeper.max-retries=3
```

Если включён `security.mode=KERBEROS`, synthetic read-lock store по умолчанию использует те же
`security.server-principal` и `security.keytab` для ZooKeeper SASL/Kerberos, что и front door,
по той же модели, что и delegation-token store выше.

Store держит под настроенным znode два поддерева:

```
/hms-proxy-synthetic-read-locks/locks/lock-<sequence>   сериализованное состояние lock, ключ - lock id
/hms-proxy-synthetic-read-locks/txns/<txnId>/<lockId>   пустая нода-указатель, ключ - транзакция
```

`check_lock`, `unlock` и `heartbeat` приходят только с lock id, поэтому состояние lock обязано
храниться с ключом по lock id; поддерево `txns` - вторичный индекс, который позволяет `commit_txn`
и `abort_txn` освободить локи транзакции одним `getChildren` вместо скана всех живых lock. Коммит
транзакции, которая не брала synthetic lock, стоит один `getChildren` с ответом `NoNode`.

Истёкшие lock убирает фоновый sweep раз в 30s (никогда не в потоке клиентского запроса); он же
удаляет индексные записи, у которых пропала lock-нода, и пустые per-transaction каталоги. Lock
всегда пишется раньше своей индексной записи, поэтому недоделанный create не может спрятать lock
от `commit_txn`.

Rolling upgrade: у lock, записанных proxy старее этого layout, индексной записи нет, поэтому новый
инстанс не освободит их по `commit_txn`/`abort_txn` - вместо этого они истекут через sweep за
`metastore.txn.timeout`. Synthetic lock ничего не блокирует на backend, так что вся цена - znode,
живущий чуть дольше. Ручная миграция и downtime не нужны, старые и новые инстансы могут работать
против одного znode одновременно.

Таймаут lock берётся из `metastore.txn.timeout` default-каталога и понимает каноническую форму Hive
с суффиксом (`600s`, `10m`); голое число по-прежнему читается как секунды. Нераспознанное значение
откатывается к 300s и логируется как `WARN` на старте.

### Kerberos impersonation

Если хочешь, чтобы backend HMS вызовы выполнялись от имени аутентифицированного пользователя, а не
от service principal proxy:

```properties
security.impersonation-enabled=true
```

Или только для конкретных backend:

```properties
# Дефолт для каталогов, у которых нет собственной настройки.
security.impersonation-enabled=false

catalog.catalog1.impersonation-enabled=true
catalog.catalog2.impersonation-enabled=false
```

Глобальный ключ работает именно как дефолт `catalog.<name>.impersonation-enabled`: в рантайме
имперсонацию включает per-catalog флаг, который наследует глобальное значение, если не задан явно.

Это требует:

- `security.mode=KERBEROS` на фронте
- proxy-user impersonation rules на backend HMS для `security.client-principal`

Если backend HMS настроен на Kerberos/SASL:

```properties
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```

Когда для любого backend включён `hive.metastore.sasl.enabled=true`, proxy открывает outbound HMS
соединения под `security.client-principal` и `security.client-keytab`.

Для каждого каталога можно отдельно ограничить write RPC:

```properties
catalog.catalog1.access-mode=READ_ONLY
catalog.catalog2.access-mode=READ_WRITE_DB_WHITELIST
catalog.catalog2.write-db-whitelist=sales,analytics
```

Для каждого каталога можно отдельно задать latency budget для latency-aware routing слоя:

```properties
catalog.catalog1.latency-budget-ms=1500
catalog.catalog2.latency-budget-ms=5000
```

И независимо от write-policy можно ограничить видимость метаданных:

```properties
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-db-patterns=sales
catalog.catalog1.expose-table-patterns.sales=orders_.*,events
```

Поддерживаются режимы:

- `READ_WRITE`: поведение по умолчанию
- `READ_ONLY`: для каталога разрешены только read RPC
- `READ_WRITE_DB_WHITELIST`: write RPC разрешены только для баз из
  `catalog.<name>.write-db-whitelist`

`access-mode` и `write-db-whitelist` задаются только вместе. Whitelist при любом другом
access-mode, как и `READ_WRITE_DB_WHITELIST` без whitelist, — ошибка старта, а не каталог, который
молча разрешает любую запись.

Режимы selective exposure:

- `ALLOW_ALL`: поведение по умолчанию для `catalog.<name>.expose-mode`
- `DENY_BY_DEFAULT`: metadata скрывается, если объект не совпал с
  `catalog.<name>.expose-db-patterns` или `catalog.<name>.expose-table-patterns.<dbRegex>`

Latency-aware routing, background backend polling, adaptive timeout, circuit breaker, safe hedged
fanout read и degraded omission настраиваются через `routing.*` properties:

```properties
routing.backend-state-polling.enabled=true
routing.backend-state-polling.interval-ms=10000
routing.backend-state-polling.probe-timeout-ms=5000
routing.backend-state-polling.max-parallelism=8
routing.adaptive-timeout.enabled=true
routing.adaptive-timeout.initial-ms=5000
routing.adaptive-timeout.min-ms=1000
routing.adaptive-timeout.max-ms=60000
routing.adaptive-timeout.multiplier=4.0
routing.adaptive-timeout.alpha=0.2
routing.adaptive-timeout.reconnect-cooldown-ms=30000
routing.circuit-breaker.enabled=true
routing.circuit-breaker.failure-threshold=3
routing.circuit-breaker.open-state-ms=30000
routing.hedged-read.enabled=true
routing.hedged-read.max-parallelism=8
routing.database-list-cache.ttl-ms=2000
routing.database-list-cache.max-entries=1000
routing.degraded-routing-policy=SAFE_FANOUT_READS
```

Adaptive timeout стартует с `routing.adaptive-timeout.initial-ms`, затем следует за backend
latency EWMA в заданных min/max пределах. Каждое изменение таймаута приводит к reconnect
shared backend client и сбросу кэша impersonation-клиентов (Kerberos re-login), поэтому proxy
применяет hysteresis (`max(2s, 25 % от текущего значения)`) и cooldown
(`routing.adaptive-timeout.reconnect-cooldown-ms`, по умолчанию 30 s) перед повторным
реконнектом. Счётчики `hms_proxy_adaptive_timeout_reconnect_total` и
`hms_proxy_adaptive_timeout_reconnect_skipped_total{reason="hysteresis"|"cooldown"}` показывают,
сколько реконнектов сработало и сколько было подавлено троттлингом. Если reconnect не успел
квиесцировать shared pool, client socket timeout откатывается к значению, с которым работают живые
сессии, и cooldown всё равно запускается — перегруженный backend не квиесцируется заново на каждом
следующем запросе. Transport failure и превышение latency budget
учитываются в circuit breaker. Когда backend достигает
`routing.circuit-breaker.failure-threshold`, proxy начинает fail-fast для этого backend до конца
open-window, а потом пускает один half-open retry, который либо закрывает circuit, либо снова
открывает его.

**Iceberg pointer guard** — `INSERT` в Iceberg-таблицу из HiveServer2 открывается
`alter_table_with_environment_context` с объектом `Table`, снятым на этапе компиляции запроса, а
метастор применяет эти параметры целиком, поэтому стирается каждый Iceberg-ключ, который есть в
записи и отсутствует в запросе, — в первую очередь `metadata_location`. Прокси — единственное
место, где встречаются Hive- и Iceberg-пути записи, поэтому такой alter сливается поверх записи,
которую метастор держит сейчас, а не пересылается как есть; честный Iceberg-коммит опознаётся по
`previous_metadata_location`, равному текущему указателю, и пропускается без изменений. На Hive
4-бэкендах починенный alter дополнительно несёт
`expected_parameter_key`/`expected_parameter_value` — метастор применит его только пока указатель
всё ещё тот, который был прочитан.

```properties
routing.iceberg-pointer-guard.enabled=true
routing.iceberg-pointer-guard.table-cache-ttl-ms=30000
routing.iceberg-pointer-guard.table-cache-max-entries=10000
routing.iceberg-pointer-guard.lock-enabled=true
routing.iceberg-pointer-guard.lock-acquire-timeout-ms=10000
```

Чтение записи и запись alter'а — два разных вызова, поэтому починка держит тот самый табличный
лок, который берёт сам Iceberg (EXCLUSIVE, уровень таблицы — форма запроса из
`org.apache.iceberg.hive.MetastoreLock`), на всё перечитывание, слияние и `alter_table` бэкенда.
Лок запрашивается **только когда починка действительно нужна**: честный Iceberg-коммит шлёт свой
`alter_table` изнутри этого же лока, поэтому запрос лока до решения означал бы ожидание того, кто
ждёт ответа на обслуживаемый вызов. Не полученный за `lock-acquire-timeout-ms` лок никогда не
отменяет запись — alter уходит починенным, но без защиты, и это считается как
`repair_lock_timeout`: уронить обычную запись Hive из-за икоты таблицы локов метастора хуже, чем
оставить окно открытым. `0` означает одну попытку без ожидания. Non-default каталоги не блокируются
никогда: их писателей обслуживает synthetic lock shim, который выдаёт локи без проверки конфликтов,
поэтому за объект этого лока никто не борется.

Iceberg-ность таблицы определяется по записи метастора, а не по запросу — в форме, которую
присылает HiveServer2, нет ни одного Iceberg-ключа, — и это стоит одного `get_table` на каждый
`alter_table`. `table-cache-ttl-ms` ограничивает цену: имя, про которое метастор ответил, что это
не Iceberg-таблица, запоминается на это время, поэтому обычные Hive-таблицы (где и сосредоточен
объём `alter_table`) платят одно чтение и дальше ничего. Iceberg-таблицы не кэшируются никогда —
их текущий указатель обязан читаться заново; `0` полностью отключает кэш. Таблица, ставшая
Iceberg-таблицей вне прокси, не защищена не дольше одного TTL: `create_table` или `alter_table` с
указателем сбрасывает запомненный ответ сразу. `hms_proxy_iceberg_pointer_guard_events_total`
показывает и починки, и сэкономленные кэшем чтения.

## Пример mixed config: Hortonworks front + hdp backend + apache backend + Kerberos

```properties
server.name=hms-proxy
server.bind-host=0.0.0.0
server.port=9083

routing.default-catalog=hdp
routing.catalog-db-separator=__

compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar

security.mode=KERBEROS
security.server-principal=hive/_HOST@EXAMPLE.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab
security.client-principal=hive/_HOST@EXAMPLE.COM
security.client-keytab=/etc/security/keytabs/hms-proxy-client.keytab
security.impersonation-enabled=true

catalogs=hdp,apache
routing.backend-state-polling.enabled=true
routing.adaptive-timeout.enabled=true
routing.circuit-breaker.enabled=true
routing.hedged-read.enabled=true
routing.degraded-routing-policy=SAFE_FANOUT_READS

catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
catalog.hdp.impersonation-enabled=true
catalog.hdp.conf.hive.metastore.uris=thrift://hdp-hms.example.com:9083
catalog.hdp.conf.hive.metastore.sasl.enabled=true
catalog.hdp.conf.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM
catalog.hdp.latency-budget-ms=1500

catalog.apache.runtime-profile=APACHE_3_1_3
catalog.apache.impersonation-enabled=true
catalog.apache.conf.hive.metastore.uris=thrift://apache-hms.example.com:9083
catalog.apache.conf.hive.metastore.sasl.enabled=true
catalog.apache.conf.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM
catalog.apache.latency-budget-ms=5000
```

## Ручной HMS smoke client

Для сценариев из [SMOKE.ru.md](SMOKE.ru.md) в репозитории теперь есть runnable-клиент прямых HMS
RPC:

- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn`
- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock`
- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification`

Если прогон `txn` или `lock` падает уже после `open_txns`, клиент делает best-effort `abort_txn`
перед пробросом ошибки, в том числе при `--close-txn none`: упавший smoke не должен оставлять
транзакцию, удерживающую watermark `ValidTxnList` до истечения heartbeat-timeout.

Текущее smoke-покрытие сведено в test matrix в [SMOKE.ru.md](SMOKE.ru.md) по полям:

- client version
- front-door profile
- backend profile
- auth mode
- method families
- expected result

Сначала собери jar:

```bash
mvn -DskipTests package
```

Для Java 17+ в Kerberos-окружении с Hadoop 2.x используй те же JVM-флаги, что и для proxy:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn \
  --uri thrift://proxy-host:9083 \
  --auth kerberos \
  --server-principal hive/proxy-host.example.com@REALM.COM \
  --client-principal alice@REALM.COM \
  --keytab /etc/security/keytabs/alice.keytab \
  --krb5-conf /etc/krb5.conf \
  --db hdp__default \
  --table smoke_txn_tbl
```

Этот режим последовательно вызывает:

- `open_txns`
- `allocate_table_write_ids`
- `lock`
- `check_lock`
- `get_valid_write_ids`
- `commit_txn`

Режим `lock` нужен для прямой проверки lock lifecycle, в первую очередь synthetic shim на
non-default catalog. Он открывает один txn, берет запрошенный lock, вызывает `check_lock`,
при необходимости делает `heartbeat`, при необходимости вызывает `unlock`, а затем завершает
txn через `abort` или `commit`.

Пример DB lock в стиле `CREATE TABLE` на non-default catalog:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock \
  --uri thrift://proxy-host:9083 \
  --db apache__default \
  --lock-type SHARED_READ \
  --lock-level DB \
  --operation-type NO_TXN \
  --transactional false
```

Пример partition lock в стиле rename/drop на non-default catalog:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
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

Режим `notification` нужен для Hortonworks-only RPC `add_write_notification_log`, поэтому ему
дополнительно нужен HDP standalone metastore jar:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification \
  --uri thrift://proxy-host:9083 \
  --auth kerberos \
  --server-principal hive/proxy-host.example.com@REALM.COM \
  --client-principal alice@REALM.COM \
  --keytab /etc/security/keytabs/alice.keytab \
  --krb5-conf /etc/krb5.conf \
  --db hdp__default \
  --table smoke_txn_tbl \
  --txn-id 1001 \
  --write-id 2001 \
  --files-added hdfs:///warehouse/tablespace/managed/hive/smoke_txn_tbl/delta_1001_1001_0000/bucket_00000 \
  --hdp-standalone-metastore-jar /opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

Что важно:

- `--server-principal` должен указывать на front-door principal самого proxy, а не backend HMS
- `--client-principal` и `--keytab` это Kerberos credentials клиента, которым запускается smoke
- дополнительные HiveConf overrides можно передавать через повторяющийся `--conf key=value`
- `lock` это самый короткий путь воспроизвести non-default catalog `NO_TXN` shim кейсы вроде
  `CREATE TABLE` и partition rename/drop без Beeline
- `notification` должен проходить для Hortonworks-routed каталога и падать с
  `requires a Hortonworks backend runtime` для Apache-routed каталога

### Automated Real-installation Smoke

Для регулярных проверок на реальной инсталляции теперь есть два отдельных runner'а:

- [`scripts/run-real-installation-smoke-simple.sh`](scripts/run-real-installation-smoke-simple.sh)
- [`scripts/run-real-installation-smoke-kerberos.sh`](scripts/run-real-installation-smoke-kerberos.sh)

Это обёртки над тем же smoke client, которые fail-fast запускают сценарий из:

- optional Beeline / HiveServer2 SQL smoke из [SMOKE.ru.md](SMOKE.ru.md)
- direct txn/ACID smoke
- DB lock smoke на non-default catalog
- optional partition lock smoke
- optional Hortonworks notification smoke

Стартовать удобнее с соответствующего example-конфига:

```bash
cp scripts/hms-real-installation-smoke.simple.env.example scripts/hms-real-installation-smoke.simple.env
cp scripts/hms-real-installation-smoke.kerberos.env.example scripts/hms-real-installation-smoke.kerberos.env
```

Дальше поправь `HMS_SMOKE_*` значения и запускай:

```bash
scripts/run-real-installation-smoke-simple.sh --scenario all
scripts/run-real-installation-smoke-kerberos.sh --scenario all
```

Либо более узкий прогон:

```bash
scripts/run-real-installation-smoke-simple.sh --scenario sql
scripts/run-real-installation-smoke-simple.sh --scenario locks
scripts/run-real-installation-smoke-kerberos.sh --scenario notification
```

Что важно:

- по умолчанию runner берёт самый свежий `target/hms-proxy-*-fat.jar`
- путь к jar можно переопределить через `HMS_SMOKE_FAT_JAR`
- если задан `HMS_SMOKE_BEELINE_JDBC_URL`, в `all` дополнительно запускается Beeline / HiveServer2 SQL smoke из `SMOKE.ru.md`
- SQL smoke использует `HMS_SMOKE_HDP_READ_TABLE` / `HMS_SMOKE_APACHE_READ_TABLE`, по умолчанию проверяет view rewrite и permanent UDF, а при необходимости умеет запускать transactional SQL и materialized-view checks
- если proxy специально поднят без `federation.view-text-rewrite.mode=REWRITE`, выставь `HMS_SMOKE_SQL_RUN_VIEW_REWRITE=false`; для UDF можно выставить `HMS_SMOKE_SQL_RUN_UDF=false` или переопределить `HMS_SMOKE_SQL_UDF_CLASS` вместе с `HMS_SMOKE_SQL_UDF_EXPECTED_RESULT`, если HS2 classpath отличается
- если заданы `HMS_SMOKE_TXN_SECONDARY_DB` и `HMS_SMOKE_TXN_SECONDARY_TABLE`, runner делает второй direct txn smoke
- если `HMS_SMOKE_NOTIFICATION_*` не настроены, notification шаг в `all` будет пропущен
- если заданы `HMS_SMOKE_NOTIFICATION_NEGATIVE_DB` и `HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE`, runner дополнительно запускает negative notification check для Apache backend из `SMOKE.ru.md`
- simple runner автоматически подхватывает `scripts/hms-real-installation-smoke.simple.env`
- Kerberos runner автоматически подхватывает `scripts/hms-real-installation-smoke.kerberos.env`
