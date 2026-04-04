# Changelog

Этот changelog суммирует всю историю коммитов репозитория от первого коммита до `2026-04-04`.
Тегированных релизов у проекта пока нет, поэтому записи сгруппированы по датам коммитов и
сфокусированы на заметных для пользователей изменениях.

English version: [CHANGELOG.md](CHANGELOG.md).

## 2026-04-04

### Добавлено

- Добавлена request overload protection на основе token-bucket rate limits для client principal,
  source IP, source CIDR pool, HMS method families, catalog и high-risk RPC classes.
- Добавлены отдельные защитные классы для `write`, `ddl`, `txn` и `lock` RPC.
- Добавлена Prometheus observability для throttled requests через
  `hms_proxy_rate_limited_total` и `status="throttled"` в `hms_proxy_requests_total`.

### Документация

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
