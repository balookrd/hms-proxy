# Changelog

Этот changelog суммирует всю историю коммитов репозитория от первого коммита до `2026-04-29`.
Записи сгруппированы по датам коммитов и сфокусированы на заметных для пользователей изменениях.
Первый тегированный релиз — `v1.0.0`, выпущен 2026-04-29.

English version: [CHANGELOG.md](CHANGELOG.md).

## 2026-05-02

### Изменено

- Prometheus-метрики теперь ограничивают cardinality лейблов. Лейбл `exception` у
  `hms_proxy_backend_failures_total` и `hms_proxy_synthetic_read_lock_store_failures_total`
  нормализуется по whitelist известных исключений; неизвестные классы складываются в
  `other`. Каждая метрика дополнительно имеет soft-cap в 5000 различных серий — после
  достижения порога новые комбинации лейблов направляются в единую серию `overflow`,
  а не растят внутреннюю карту и Prometheus output без границ.

- Adaptive socket timeout теперь троттлит reconnect backend, чтобы избежать reconnect storm
  при нестабильной latency. Hysteresis расширен с фиксированных 1 s до
  `max(2 s, 25 % от текущего применённого таймаута)`, плюс добавлен настраиваемый cooldown
  (`routing.adaptive-timeout.reconnect-cooldown-ms`, по умолчанию 30 s), блокирующий
  reconnect подряд. Раньше каждый reconnect сбрасывал кэш impersonation-клиентов и заставлял
  заново выполнять Kerberos login, что делало осцилляцию дорогой.

### Добавлено

- Два новых Prometheus счётчика отражают динамику adaptive timeout:
  `hms_proxy_adaptive_timeout_reconnect_total{catalog}` для применённых реконнектов и
  `hms_proxy_adaptive_timeout_reconnect_skipped_total{catalog,reason}` для событий,
  подавленных hysteresis или cooldown. В Grafana dashboard добавлены три новых панели —
  общий rate реконнектов, per-catalog timeseries и стек подавленных событий по reason.

## 2026-04-29

### Добавлено

- Вывод логов в консоль теперь дополнительно пишется в два файловых appender'а:
  `logs/hms-proxy.log` (rolling по 50MB, 10 backup) и `logs/hms-proxy-daily.log` (с суффиксом
  даты). История логов переживает рестарт и доступна для оффлайн-анализа.

### Изменено

- Grafana dashboard переписан и теперь покрывает все 13 экспортируемых метрик, сгруппированных
  в шесть секций — Requests & Latency, Backend Operations, Routing, Rate Limiting, Metadata
  Filtering, Synthetic Read Locks. Добавлены панели для ранее не покрытых
  `hms_proxy_rate_limited_total`, `hms_proxy_filtered_objects_total` и
  `hms_proxy_synthetic_read_lock_store_info`.
- GitHub Actions release-воркфлоу перестроены вокруг переиспользуемого `_release-build.yml`.
  Ручной `Release` dispatch теперь только считает следующий `vX.Y.Z` и печатает инструкции для
  создания подписанного тега локально; push тега запускает `Tag Release`, который собирает и
  публикует релиз. Push в `main` создаёт rolling `nightly` prerelease вместо прежних per-commit
  `build-*` и дневных `nightly-*` релизов.

### Исправлено

- Версия Maven-артефакта на тегированном коммите теперь соответствует тегу (например,
  `hms-proxy-1.0.0.jar` для тега `v1.0.0`), а не snapshot-паттерну. jgitver-параметр
  `tagVersionPattern` был хардкоднут тем же выражением, что и для нетегированных коммитов;
  теперь он установлен в дефолтный `${v}`. Snapshot-сборки на нетегированных коммитах сохраняют
  прежнее имя `0.1.<distance>-<sha>`.

## 2026-04-28

### Исправлено

- Если management HTTP или metastore Thrift listener не может забиндить заданный `host:port`
  (например, порт уже занят), proxy теперь логирует явный ERROR с указанием, какой listener упал
  и по какой причине, перед тем как exception пробрасывается дальше, а не выдаёт один raw stack
  trace перед ненулевым exit'ом.

## 2026-04-20

### Изменено

- Per-catalog shared backend session и `synchronized` вокруг вызовов заменены на borrow/return пул
  размером `catalog.<name>.shared-session-pool-size` (default `1`). Non-impersonated вызовы к одному
  каталогу теперь идут параллельно до размера пула, а не сериализуются через единственный Thrift
  transport. **Внимание:** дефолт `1` сохраняет прежнее сериализованное поведение — чтобы реально
  получить параллелизм, нужно явно выставить `catalog.<name>.shared-session-pool-size` (например,
  `8` или `16`) на каталог. Большие значения держат больше idle Thrift-сессий к backend HMS
  (с пропорциональной стоимостью Kerberos, если включён) и удлиняют дренаж в `reconnectShared`.
- **Breaking:** `synthetic-read-lock.store.mode` теперь обязательно задавать явно — как в
  properties-конфиге, так и при программной сборке `ProxyConfig`. Прежний молчаливый default
  `IN_MEMORY` был небезопасен для multi-instance deployment — synthetic SELECT-локи на non-default
  каталогах терялись при рестарте proxy или failover через load balancer без сигнала на старте.
  Выбирайте `IN_MEMORY` для одиночного инстанса (стартовый `WARN` про потерю SELECT-локов
  по-прежнему пишется) или `ZOOKEEPER` для HA. Если сконфигурированы
  `synthetic-read-lock.store.zookeeper.*`, `ZOOKEEPER` выводится автоматически. Для in-process
  builder'ов добавлен хелпер `ProxyConfig.SyntheticReadLockStoreConfig.inMemory()`.
- Проведён крупный внутренний рефакторинг конфигурации и operation-policy: вложенные config-record
  вынесены в top-level типы, пакет `config` разложен на тематические подпакеты, а реестр HMS
  операций разбит на per-category contributors. Внешнее поведение не меняется, кроме явного
  требования настроить synthetic-read-lock store.

### Исправлено

- Однократный retry на транспортной ошибке теперь дискардит только ту сессию, которая упала, а не
  пересоздаёт весь shared connection.
- `TApplicationException` больше не считается backend transport failure, поэтому proxy не делает
  лишние retry к живому серверу при dispatch-level ошибках вроде unsupported HDP wrapper RPC.
- Убраны некритичные compile/test warnings и добавлена минимальная test logging configuration,
  чтобы в тестах не шумело сообщение `"No appenders could be found"`.

### Документация

- Добавлены рекомендации по тюнингу shared-session pool, включая необходимость явно задавать
  `catalog.<name>.shared-session-pool-size` для реального параллелизма и описание компромиссов по
  idle session, Kerberos-cost и времени дренажа при reconnect.

## 2026-04-19

### Изменено

- Усилены backend health probe и `/readyz`: probe теперь используют ephemeral sessions, `/readyz`
  проверяет backend'ы параллельно, а JSON escaping теперь полностью покрывает control characters.
- Routing и config internals разбиты на более мелкие компоненты: `RoutingMetaStoreHandler`
  переименован в `RoutingMetaStoreProxy`, `BackendCallDispatcher`, `RoutingHandler` и
  `ProxyConfigLoader` разложены на focused collaborators и parsers, а per-RPC metadata сведена в
  декларативные policy registry.
- Снижен package coupling между routing, config, backend, frontend, federation и utility слоями:
  общие типы перенесены в более подходящие пакеты, а routing теперь завязан на интерфейс
  `FederationOperations`.

### Исправлено

- Исправлен head-of-line blocking в parallel fanout: готовые futures теперь собираются в рамках
  общего deadline, а не через последовательное ожидание каждого backend.
- Backend health probe больше не меняют живые shared sessions и не выбрасывают impersonation
  clients при дрейфе probe timeout.
- Исправлены устаревшие ссылки в `capabilities.yaml` после переименования в
  `RoutingMetaStoreProxy`.

## 2026-04-18

### Добавлено

- Добавлены настройки deadline для per-request hedged-read fanout и backend-state probe, а также
  максимального размера и idle TTL для impersonation client cache.

### Изменено

- Ускорены hot path в routing: синхронизированный rate limiter заменён на lock-free GCRA, а
  Thrift reflection в namespace translation и table-name extraction теперь кэшируется.
- Routing, namespace translation и wiring handler'ов разбиты на более мелкие компоненты, чтобы
  уменьшить package coupling и упростить unit tests.

### Исправлено

- Parallel fanout и backend-state probe теперь жёстко ограничены по времени, чтобы зависшие
  backend'ы не блокировали запросы и не подвешивали single-thread poller.
- При timeout fanout теперь отменяются все pending futures, чтобы не истощать thread pool.
- Убран blocking reconnect I/O под `synchronized` в backend client path.
- Исправлена утечка `ThreadLocal` в `RoutingMetaStoreHandler`.
- В namespace translation добавлено обнаружение циклов, чтобы избежать бесконечной рекурсии на
  циклических графах Thrift-объектов.

## 2026-04-15

### Добавлено

- Добавлена best-effort очистка данных при удалении external table из файловой системы routed
  catalog, если опция включена.

### Документация

- Задокументированы конфигурация и поведение external table drop purge в обоих README и в example
  properties.

## 2026-04-14

### Добавлено

- Добавлен rewrite location для external table на routed catalog; если исходная filesystem явно не
  задана, по умолчанию используется filesystem default catalog.

### Изменено

- Разбор config mode сделан регистронезависимым, включая режимы view rewrite.

### Исправлено

- Исправлена обработка запросов статистики с union payload в view-definition compatibility layer.

## 2026-04-13

### Документация

- Расширены smoke guides и smoke-покрытие для view rewrite и UDF-сценариев, включая
  real-installation smoke script.

## 2026-04-07

### Добавлено

- Добавлены режимы transactional DDL guard `REWRITE_TO_NON_TRANSACTIONAL` и
  `REWRITE_MANAGED_TO_EXTERNAL`; существующие режимы переименованы в более явные варианты.

### Исправлено

- Убраны ложные SASL `ERROR`-логи от probe-соединений, которые открывают сокет, но не отправляют
  SASL payload.

## 2026-04-06

### Изменено

- Пакетированная сборка теперь кладёт runtime-зависимости в отдельный каталог `lib/`.

### Исправлено

- Исправлена совместимость Hortonworks frontend: исключения из HDP-only методов и payload
  `alter_partitions_req` теперь корректно переводятся через границу classloader'ов.
- Скорректирован routing ACID-операций: `allocate_table_write_ids` и `get_valid_write_ids`
  направляются в default backend, а создание transactional tables на non-default catalog теперь
  завершается понятным `MetaException`.

## 2026-04-05

### Изменено

- Обработка запросов перестроена в interceptor chain, который разделяет rate limiting, lock
  handling, compatibility adaptation и routing.

### Исправлено

- Исправлена потеря request context в parallel fanout tasks и убрана неограниченная fanout queue.

## 2026-04-04

### Добавлено

- Добавлен latency-aware routing backend'ов с per-catalog latency budget, adaptive timeout,
  circuit breaker с half-open retry, optional backend-state polling и degraded routing для safe
  read-only fanout RPC.
- Добавлена request overload protection на основе token-bucket rate limits для client principal,
  source IP, source CIDR pool, HMS method families, catalog и high-risk RPC classes.
- Добавлены отдельные защитные классы для `write`, `ddl`, `txn` и `lock` RPC.
- Добавлена Prometheus observability для throttled requests через
  `hms_proxy_rate_limited_total` и `status="throttled"` в `hms_proxy_requests_total`.

### Документация

- Задокументированы latency-aware routing knobs, per-catalog latency budget и расширенный payload
  `/readyz` в обоих README и в example properties.
- Задокументированы конфигурация overload protection и её operating model в обоих README и в
  example properties.

## 2026-04-03

### Добавлено

- Добавлен synthetic proxy read-lock shim для non-ACID `SELECT` на non-default catalog.
- Добавлено прямое smoke-покрытие для synthetic non-transactional `NO_TXN` lock сценариев,
  включая DB lock в стиле `CREATE TABLE` и partition lock в стиле rename/drop на non-default catalog.
- Добавлено хранение synthetic read-lock state в ZooKeeper, чтобы после падения одного proxy
  транзакции и lock lifecycle могли продолжаться через соседний instance.
- Добавлена observability для synthetic lock: Prometheus-метрики, gauge активных lock,
  счётчики handoff между proxy instance, счётчики store failures и панели Grafana.

### Изменено

- Persistent token-store RPC теперь обрабатываются локально в proxy, а не проксируются дальше.
- Backend lock failures теперь поднимаются клиенту как `MetaException`, чтобы поведение было
  прозрачнее и стабильнее.

### Исправлено

- Исправлена policy для namespace-less HMS routing.
- Исправлена обработка synthetic lock для non-transactional `NO_TXN` DDL lock на non-default
  catalog, включая `CREATE TABLE` и partition rename, которые Hive ведёт через txn/lock API.
- Front-door security теперь стартует раньше backend runtimes.
- Убран нежелательный UGI fallback до keytab login на front door.
- ZooKeeper SASL JAAS теперь настраивается до старта token manager.
- Исправлен ZooKeeper integration test: в средах без права bind локального порта embedded
  `TestingServer` теперь корректно пропускается, а не валит весь suite.

### Документация

- Задокументированы ZooKeeper token-store credentials, overrides, текущее поведение
  namespace-less routing и расширенные synthetic `NO_TXN` smoke-сценарии.

## 2026-04-02

### Добавлено

- Добавлены management HTTP endpoints для health, readiness и metrics.
- Добавлены Prometheus-метрики и стартовый Grafana dashboard.
- Добавлены structured audit log и Kerberos readiness checks.
- Добавлены per-catalog access modes.
- Добавлена поддержка Hortonworks `3.1.5` metastore runtime.
- Добавлен HDP passthrough для table extensions и materialized views.
- Добавлен compatibility layer для rewrite view definitions.
- Добавлен GitHub Actions CI.

### Изменено

- Compatibility и federation layers разделены, чтобы упростить routing и translation flow.
- Routing policy отвязан от compatibility bridge.
- Расширены compatibility fallback path для HDP-запросов.
- Добавлен cache для unsupported wrapper RPC на Hortonworks backend.
- В fat JAR выровнены Curator dependencies.

### Исправлено

- Включена Kerberos authentication для `ZooKeeperTokenStore`.
- Front-door ZooKeeper token store теперь использует keytab login user.
- Transactional DDL mode ограничен managed tables.

### Документация

- Расширена observability documentation.
- Добавлены compatibility и test matrices.
- Уточнена разница между proxyuser и ZooKeeper configuration.
- Обновлена общая документация по management и compatibility функциям.

## 2026-04-01

### Добавлено

- Добавлен manual HMS smoke client.
- Добавлен transactional DDL guard.

### Изменено

- Унифицированы конфигурация и поведение transactional DDL guard.
- Обобщена обработка HDP compatibility requests.
- Улучшены smoke tests и их покрытие.
- Добавлена `jgitver`-based versioning схема.

### Исправлено

- Исправлен набор routing edge cases в metastore path.

## 2026-03-31

### Добавлено

- Добавлен Hortonworks frontend compatibility bridge.
- Добавлены русская документация и двуязычные smoke guides.
- Добавлены vendored standalone metastore JAR для поддерживаемых runtime.

### Изменено

- Переработаны metastore runtimes и расширено покрытие Hortonworks bridge.
- Уточнена txn routing policy для multi-catalog режима.
- ACID lifecycle RPC прибиты к default catalog.
- Репозиторий реорганизован по модулям и пакетам.
- Исходники и тесты разнесены по package-based layout.
- Для части HDP сценариев добавлен fallback на Apache runtime.

### Исправлено

- Добавлен корректный `_HOST` Kerberos principal resolution.
- Исправлен isolated Hive class loading.
- Исправлены регрессии в HDP isolation после рефакторинга.
- Исправлен package для application main class.

## 2026-03-30

### Документация

- Уточнены требования к proxyuser для front-door delegation-token path.

## 2026-03-28

### Изменено

- Сужен compatibility routing, который отправлял часть запросов в default backend.

## 2026-03-27

### Добавлено

- Добавлена поддержка managed и ACID tables вместе с regression coverage.
- Добавлены shared backend `HiveConf` overrides.

### Изменено

- Сохранено backend catalog name при compatibility internalize.
- Default catalog names больше не префиксуются при namespace translation.

### Исправлено

- Внесён пакет исправлений для multi-catalog routing и compatibility path.

## 2026-03-26

### Добавлено

- Добавлено ZooKeeper-backed storage для token-related state.
- Добавлена настройка `routing.catalog-db-separator`.

### Изменено

- Логика impersonation разделена на более явные path, проведён сопутствующий рефакторинг.

### Исправлено

- Внесён большой пакет исправлений вокруг token storage, routing и request handling.

## 2026-03-25

### Добавлено

- Добавлен per-user cache для impersonation flows.
- Добавлена front-door delegation-token поддержка.
- Добавлены тесты для global-function handling.

### Исправлено

- Исправлены `get_all_functions()` и связанные global-function path.
- Исправлены keytab handling и ряд проблем в delegation-token / impersonation сценариях.

## 2026-03-23

### Добавлено

- Добавлена поддержка client keytab.
- Добавлена начальная поддержка impersonation.

### Исправлено

- Внесён первый пакет стабилизационных исправлений для authentication и request flow.

## 2026-03-19

### Изменено

- Добавлен debug logging и доработана logging configuration.
- Обновлён набор зависимостей для fat JAR сборки.

### Исправлено

- Исправлены проблемы logging configuration, найденные на раннем этапе упаковки.

## 2026-03-17

### Добавлено

- Добавлена сборка fat JAR через Maven Shade Plugin.

### Документация

- Расширен security section примерами Kerberos и non-Kerberos конфигурации.

### Исправлено

- Убрана лишняя runtime-зависимость от tools.

## 2026-03-16

### Исправлено

- Внесён ранний пакет стабилизационных исправлений после первичного bootstrap.

## 2026-03-12

### Добавлено

- Первичный bootstrap репозитория.
- Первый рабочий implementation commit.
