# HMS Proxy

[![CI](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml)

English documentation: [README.md](README.md), [SMOKE.md](SMOKE.md)

Java proxy для Hive Metastore, который поднимает один внешний HMS Thrift endpoint и маршрутизирует
запросы в несколько backend metastore по каталогу.

## Основные сценарии

### 1. Multi-catalog federation

Один production-facing HMS Thrift endpoint для HiveServer2 и прямых HMS API клиентов, который
маршрутизирует запросы в несколько backend metastore по явному `catName` или по legacy
database name в формате `catalog<separator>db`.

### 2. Apache ↔ HDP compatibility bridge

Apache Hive Metastore `3.1.3` на фронте, compatibility downgrade для старых Hortonworks
`3.1.0.x` backend RPC и, при необходимости, полноценный Hortonworks front door через
HDP `standalone-metastore` jar.

### 3. Kerberized proxy for HS2/HMS

Proxy как security boundary между клиентами и backend metastore: Kerberos/SASL на фронте,
опциональный outbound Kerberos к backend и опциональная impersonation аутентифицированного
Kerberos пользователя.

## Матрица поведения RPC

| Группа RPC | Статус | Поведение |
| --- | --- | --- |
| Metadata read/write RPC с явным namespace (`catName`, `dbName`, `fullTableName`) | supported | Нормально маршрутизируются в нужный catalog/backend. |
| Legacy read/write RPC с database name в формате `catalog<separator>db` | supported | Маршрутизируются по externalized database name; table/database объекты переписываются на обратном пути. |
| Apache `3.1.3` wrapper RPC против старых Hortonworks `3.1.0.x` backend | degraded | Proxy повторяет часть `*_req` API через старые legacy методы вроде `get_table`. |
| Session-level и global read-only RPC без catalog context | degraded | Идут в `routing.default-catalog`, включая `getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns` и `get_open_txns_info`. |
| Read-only service API, которых нет на backend (`TApplicationException` на notifications, privilege refresh/introspection, token/key listings кроме delegation-token issuance, txn/lock/compaction status) | degraded | Proxy возвращает empty compatibility response вместо ошибки. |
| ACID / txn / lock lifecycle RPC без routable namespace (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`) | degraded | Пинятся к `routing.default-catalog`; в multi-catalog режиме это нужно отдельно валидировать. |
| Global write operations без явного catalog context | rejected | Proxy отклоняет запрос, если настроено больше одного каталога, включая namespace-less service write RPC вроде `setMetaConf`, `grant_role`, `revoke_role` и `add_token`. |
| Управление каталогами (`create_catalog`, `drop_catalog`) | rejected | Каталоги задаются в конфиге proxy, а не через клиентские RPC. |
| HDP-only front-door методы, для которых есть явный Apache bridge mapping | supported | Proxy адаптирует выбранные Hortonworks request-wrapper методы к Apache equivalents. |
| HDP-only методы, которым нужен совместимый Hortonworks runtime (`add_write_notification_log`, `get_tables_ext`, `get_all_materialized_view_objects_for_rewriting`) | passthrough | Пробрасываются только в совместимые Hortonworks backend/front door при соответствующей конфигурации. |
| HDP-only методы без безопасного Apache mapping | rejected | Proxy падает явно, а не возвращает вводящий в заблуждение success. |

## Public compatibility matrix

Это публичная сводка того, как позиционировать proxy. Матрица сгруппирована не по отдельным thrift
методам, а по типу клиента, advertised front door, backend runtime, auth mode и семействам методов.

| Client version | Front-door profile | Backend profile | Auth mode | Method families | Expected result |
| --- | --- | --- | --- | --- | --- |
| Apache Hive / Spark клиенты, которые говорят через Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | `APACHE_3_1_3` | `NONE` или `KERBEROS` | catalog-aware read/write, legacy `catalog<separator>db` routing, view rewrite | Базовый полностью поддержанный сценарий. |
| Apache Hive / Spark клиенты, которые говорят через Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | read path и часть metadata write, где возможен fallback с `*_req` API | Поддержано через compatibility downgrade; часть вызовов работает в degraded-режиме через legacy RPC. |
| Hortonworks клиенты, которым достаточно Hortonworks identity на фронте через `getVersion()` | `HORTONWORKS_*` без standalone jar | `APACHE_3_1_3` или Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | пересекающиеся Apache/HDP method families | Поддержано, если клиенту достаточно только смены advertised profile. |
| Hortonworks клиенты, которые вызывают HDP-only thrift request-wrapper методы | `HORTONWORKS_*` с standalone jar | Hortonworks `3.1.0.x` | `NONE` или `KERBEROS` | mapped HDP-only methods, runtime-specific passthrough methods | Поддержано при наличии совместимых Hortonworks front-door и backend runtime jar. |
| Hortonworks клиенты, которые вызывают HDP-only thrift request-wrapper методы | `HORTONWORKS_*` с standalone jar | `APACHE_3_1_3` | `NONE` или `KERBEROS` | HDP-only passthrough методы вроде `add_write_notification_log` | Явно отклоняется, если target backend не даёт совместимый Hortonworks runtime. |
| HiveServer2 / Beeline SQL workloads через несколько каталогов | `APACHE_3_1_3` или `HORTONWORKS_*` | смешанные Apache + Hortonworks backend | `NONE` или `KERBEROS` | read, DDL/DML, namespace rewrite, optional view rewrite | Поддержано, пока routing может однозначно вычислить целевой каталог. |
| HiveServer2 / direct HMS клиенты, использующие txn/lock lifecycle RPC без namespace в payload | любой | смешанные Apache + Hortonworks backend | `NONE` или `KERBEROS` | `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat` | Degraded: идут в `routing.default-catalog`; считать это single-catalog control plane, пока не проведена отдельная валидация. |
| Kerberized HiveServer2 / HMS клиенты, которым нужна end-user identity на backend | любой | любой | `KERBEROS` с optional impersonation | front-door SASL, local delegation-token issuance, backend `set_ugi()` impersonation | Поддержано, если правильно настроены proxy-user rules и backend impersonation permissions. |
| Клиенты, которые пытаются делать global write без resolvable catalog или динамически управлять registry каталогов | любой | любой | `NONE` или `KERBEROS` | ambiguous writes, `create_catalog`, `drop_catalog` | Rejected by design. |

## Важные оговорки

- legacy database references без префикса каталога идут в `routing.default-catalog`
- это сохраняет совместимость со Spark/Hive, но не открывает путь к неоднозначным metadata write
- на практике это означает, что ACID write lifecycle полноценно поддерживается только для
  `default-catalog`, если из payload нельзя извлечь namespace
- request-based ACID методы, где в payload есть `dbName` или `fullTableName`, продолжают
  маршрутизироваться по этому payload

## Сборка

```bash
mvn -o test
mvn -o package
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
```

Версия сборки теперь вычисляется из git на каждом коммите в формате `0.1.<git-distance>-<short-sha>`.

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
```

Быстрые проверки:

```bash
curl -s http://127.0.0.1:19083/healthz
curl -s http://127.0.0.1:19083/readyz
curl -s http://127.0.0.1:19083/metrics
```

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
- summary по backend connectivity
- по каждому backend поля `connected`, `degraded`, `lastSuccessEpochSecond`,
  `lastFailureEpochSecond`, `lastProbeEpochSecond` и `lastError`
- Kerberos status для front door и outbound backend credentials
- TGT freshness через `tgtExpiresAtEpochSecond` и `secondsUntilExpiry`, когда эти данные доступны

### Prometheus метрики

Текущие Prometheus метрики:

- `hms_proxy_requests_total{method,catalog,backend,status}`
- `hms_proxy_request_duration_seconds{method,catalog,backend}`
- `hms_proxy_backend_failures_total{backend,exception}`
- `hms_proxy_backend_fallback_total{method,from_api,to_api}`
- `hms_proxy_routing_ambiguous_total`
- `hms_proxy_default_catalog_routed_total{method}`

Пример Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: hms-proxy
    static_configs:
      - targets:
          - hms-proxy-01.example.com:19083
```

Семантика метрик:

- `status` в `hms_proxy_requests_total` принимает значения `ok`, `error` или `fallback`
- `catalog=all, backend=fanout` означает, что запрос был отправлен сразу в несколько backend
- `hms_proxy_backend_failures_total` считает backend-side ошибки вызова с группировкой по backend и exception type
- `hms_proxy_backend_fallback_total` считает compatibility fallback, которые proxy вернул после backend failures
- `hms_proxy_routing_ambiguous_total` считает запросы, отклонённые из-за conflicting namespace hints
- `hms_proxy_default_catalog_routed_total` считает запросы, которые ушли в default catalog из-за отсутствия явного catalog namespace

### Structured audit log

Proxy также пишет один structured audit log на каждый запрос через logger
`io.github.mmalykhin.hmsproxy.audit`. Каждая запись представляет собой single-line JSON с полями
`requestId`, `method`, `catalog`, `backend`, `status`, `durationMs`, `remoteAddress`,
`authenticatedUser`, `routed`, `fanout`, `fallback` и `defaultCatalogRouted`.

Пример:

```json
{"event":"hms_proxy_audit","requestId":42,"method":"get_table","catalog":"catalog1","backend":"catalog1","status":"ok","durationMs":8,"routed":true,"fanout":false,"fallback":false,"defaultCatalogRouted":false,"remoteAddress":"10.20.30.40","authenticatedUser":"alice@EXAMPLE.COM"}
```

### Grafana dashboard

Готовый Grafana dashboard лежит в
`monitoring/grafana/hms-proxy-dashboard.json`. В нём уже есть панели по request rate, latency,
backend failures, fallbacks, default-catalog routing и ambiguous routing.

## Модель маршрутизации

- catalog-aware клиенты могут отправлять `catName=dbCatalog, dbName=sales`
- legacy клиенты могут использовать database names вроде `catalog1.sales`
- `get_all_databases()` возвращает префиксированные имена вроде `catalog1.sales`
- table objects на выходе переписываются обратно во внешний namespace
- если запрос несёт не-proxy `catName`, например стандартный Hive `hive`, proxy для
  совместимости пытается маршрутизировать по `dbName` / `default-catalog`
- по умолчанию externalized HMS objects используют proxy catalog ids в `catName`/`catalogName`
- для старых HiveServer2 сценариев можно включить
  `federation.preserve-backend-catalog-name=true`, чтобы во внешних объектах сохранялся
  backend catalog name, например `hive`, а `dbName` при этом оставался proxy namespace
- опционально proxy умеет переписывать SQL внутри `viewExpandedText` и `viewOriginalText`
  для `VIRTUAL_VIEW` / `MATERIALIZED_VIEW`:

```properties
federation.view-text-rewrite.mode=rewrite
federation.view-text-rewrite.preserve-original-text=true
```

При `mode=rewrite` proxy переписывает ссылки вида `catalog<separator>db.table` на пути в backend,
а на пути обратно разворачивает ссылки текущего backend db в внешний namespace. Если
`preserve-original-text=true`, переписывается только `viewExpandedText`, а `viewOriginalText`
остаётся как прислал клиент.

Разделитель каталога и базы настраивается:

```properties
routing.catalog-db-separator=__
```

Тогда legacy names будут выглядеть как `catalog1__sales`, а не `catalog1.sales`.

## Guard для transactional DDL

Proxy можно настроить так, чтобы он защищал создание и изменение managed-таблиц, если во входящем
metadata таблица помечена как transactional:

- `transactional=true`
- любое непустое значение `transactional_properties`

Правило применяется к `create_table`, `alter_table` и `alter_table_with_environment_context`.
Оно срабатывает только для `MANAGED_TABLE`. External-таблицы остаются без изменений.

Режим reject:

```properties
guard.transactional-ddl.mode=reject
```

Режим rewrite:

```properties
guard.transactional-ddl.mode=rewrite
```

В режиме rewrite proxy переписывает входящую таблицу в `EXTERNAL_TABLE`, добавляет
`external.table.purge=true` и удаляет `transactional` и `transactional_properties`.

Также можно ограничить его конкретными IP-адресами или CIDR-масками:

```properties
guard.transactional-ddl.mode=reject
guard.transactional-ddl.client-addresses=10.10.0.15,10.20.0.0/16,2001:db8::/64
```

Если `guard.transactional-ddl.client-addresses` задан, проверка применяется только к совпавшим
клиентам. Если не задан, проверка действует для всех клиентов.

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
- переписывание SQL работает только при `federation.view-text-rewrite.mode=rewrite`
- rewrite сделан intentionally parser-less: proxy переписывает ссылки вида `db.table`, а не
  пытается разобрать весь Hive SQL grammar
- входящие cross-catalog ссылки вроде `catalog2__dim.table_x` internalize'ятся для backend, но на
  выходе гарантированно переписывается только namespace текущей таблицы
- comments, string literals и экзотические quoted identifiers лучше проверить отдельным smoke
  тестом в вашей среде

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

Для пакета proxy по умолчанию включён подробный debug tracing через bundled `log4j.properties`.
Каждый клиентский вызов получает `requestId`, а в логах есть:

- входящий HMS method и аргументы
- выбранный backend catalog
- proxied thrift method и переписанные аргументы
- backend response или backend error
- итоговый client response или client-visible error

Если логов слишком много, переопредели уровень через свой `log4j.properties`.

## HiveServer2

Укажи в HiveServer2 `hive.metastore.uris` на proxy вместо одного backend HMS.
Для multi-catalog deployment лучше использовать Hive/клиентов, которые сохраняют catalog fields.

Для Beeline/HS2 обычно удобнее separator `__`, чем `.`:

```properties
routing.catalog-db-separator=__
```

Если metadata writes через proxy ведут себя не так, как напрямую против backend HMS, можно
попробовать:

```properties
federation.preserve-backend-catalog-name=true
```

Тогда `catName`/`catalogName` будет сохраняться с backend стороны, обычно это `hive`, а routing
по-прежнему будет идти по externalized `dbName`.

Если нагрузки используют Hive views или materialized views между несколькими catalog, имеет смысл
сразу прогонять и такой режим:

```properties
federation.view-text-rewrite.mode=rewrite
federation.view-text-rewrite.preserve-original-text=true
```

Такой набор сохраняет пользовательский `viewOriginalText`, но всё ещё переписывает
`viewExpandedText` для совместимости с backend.

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

### Kerberos impersonation

Если хочешь, чтобы backend HMS вызовы выполнялись от имени аутентифицированного пользователя, а не
от service principal proxy:

```properties
security.impersonation-enabled=true
```

Или только для конкретных backend:

```properties
security.impersonation-enabled=false

catalog.catalog1.impersonation-enabled=true
catalog.catalog2.impersonation-enabled=false
```

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

Поддерживаются режимы:

- `READ_WRITE`: поведение по умолчанию
- `READ_ONLY`: для каталога разрешены только read RPC
- `READ_WRITE_DB_WHITELIST`: write RPC разрешены только для баз из
  `catalog.<name>.write-db-whitelist`

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

catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
catalog.hdp.impersonation-enabled=true
catalog.hdp.conf.hive.metastore.uris=thrift://hdp-hms.example.com:9083
catalog.hdp.conf.hive.metastore.sasl.enabled=true
catalog.hdp.conf.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM

catalog.apache.runtime-profile=APACHE_3_1_3
catalog.apache.impersonation-enabled=true
catalog.apache.conf.hive.metastore.uris=thrift://apache-hms.example.com:9083
catalog.apache.conf.hive.metastore.sasl.enabled=true
catalog.apache.conf.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM
```

## Ручной HMS smoke client

Для сценариев из [SMOKE.ru.md](SMOKE.ru.md) в репозитории теперь есть runnable-клиент прямых HMS
RPC:

- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn`
- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification`

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
- `notification` должен проходить для Hortonworks-routed каталога и падать с
  `requires a Hortonworks backend runtime` для Apache-routed каталога
