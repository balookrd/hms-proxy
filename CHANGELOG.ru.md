# Changelog

Этот changelog суммирует всю историю коммитов репозитория от первого коммита до `2026-07-25`.
Записи сгруппированы по датам коммитов и сфокусированы на заметных для пользователей изменениях.
Первый тегированный релиз — `v1.0.0`, выпущен 2026-04-29.

English version: [CHANGELOG.md](CHANGELOG.md).

## 2026-07-25

### Добавлено

- Ограниченный жизненный цикл front-door клиентских сокетов. Принятые соединения
  теперь получают read timeout (`server.client-socket-timeout-ms`, по умолчанию
  `600000`, `0` отключает) и настраиваемый TCP keepalive (`server.tcp-keepalive`,
  `server.tcp-keepalive-idle-seconds`, `server.tcp-keepalive-interval-seconds`,
  `server.tcp-keepalive-count`). Раньше libthrift принимал сокеты с бесконечным
  read timeout и системными keepalive-таймерами, поэтому клиент, умерший без
  FIN/RST, держал worker-поток заблокированным, пока ОС не сдастся, — с медленным
  вымыванием `server.max-worker-threads`. Дополнительные frontend listener'ы
  наследуют значения primary и могут переопределить каждый ключ по отдельности.
- `server.shutdown-timeout-seconds` (по умолчанию `30`) ограничивает упорядоченную
  остановку по SIGTERM.

### Изменено

- `federation.view-text-rewrite.preserve-original-text` теперь по умолчанию `true`. При
  `mode=REWRITE` переписывается только `viewExpandedText`, пока свойство явно не выставлено в
  `false`, то есть клиентский `viewOriginalText` больше не мутируется по умолчанию.
- Из дефолтной конфигурации логирования убран `DailyRollingFileAppender`
  `logs/hms-proxy-daily.log`. У него не было лимита на число файлов и он рос неограниченно, а с
  тремя root appender'ами каждая строка сторонних библиотек писалась трижды. Теперь по умолчанию это
  stderr плюс ограниченный по размеру `logs/hms-proxy.log`.
- `/readyz` кэширует результаты backend- и Kerberos-probe на `management.readiness-cache-ms`
  (по умолчанию 2000) и обновляет их в режиме single-flight, поэтому частые scrape больше не
  порождают по раунду сетевых проверок на каждый запрос. В ответе появилось поле `probeAgeMs`;
  значение `0` возвращает probe на каждый запрос. Per-backend поля состояния по-прежнему рендерятся
  из актуального runtime state при каждом вызове.
- Задокументировано, что management endpoints не имеют аутентификации, а `/readyz` отдаёт
  Kerberos-принципалы и детали backend-ошибок, поэтому порт нужно выносить в изолированную сеть
  мониторинга.

### Исправлено

- Ответы front-door мостов больше не рвут клиентское соединение и не падают на
  сериализации. `Hive4FrontendBridge` собирал response wrapper для
  `get_partitions_by_filter_req`, `get_partition_names_req` и
  `drop_partition_req`, которые в Hive 4 типизированы как `List<Partition>`,
  `List<String>` и `boolean` — каждый вызов бросал исключение, доходившее до
  клиента как обрыв соединения. Теперь значение возвращается напрямую.
  `get_partitions_req`, `get_partitions_by_names_req` и `get_fields_req` (а
  также `get_partitions_by_names_req` в `HortonworksFrontendBridge`) клали
  Apache-объекты из parent classloader в изолированные response-структуры, что
  роняло сгенерированную write-схему с `ClassCastException`; элементы списков
  теперь сначала конвертируются в classloader фронтенда.
- `get_databases_req` возвращает структуры `Database` вместо простого списка
  имён, который раньше попадал в поле `List<Database>`. Имена по-прежнему
  берутся через `get_all_databases`/`get_databases`, после чего каждая база
  запрашивается через `get_database`.
- `get_partition_names_req` учитывает `expr` из запроса, а не игнорирует его.
  При непустом выражении мост вызывает `get_partitions_by_expr` (передавая
  `expr`, `defaultPartitionName` и `maxParts`) и восстанавливает имена
  партиций по partition keys таблицы; при пустом выражении сохраняется прежний
  путь через `get_partition_names`.
- Guard для transactional DDL (`guard.transactional-ddl.*`) теперь покрывает
  все RPC `create_table*` / `alter_table*` вместо фиксированного списка из трёх
  методов. В частности, под guard попали `create_table_with_environment_context` —
  RPC, который `HiveMetaStoreClient` 3.1.x реально отправляет для `createTable`
  и в который оба frontend-моста разворачивают свой `create_table_req`, — а
  также `create_table_with_constraints` и `alter_table_with_cascade`. Политики
  REJECT/REWRITE теперь применяются к основному пути создания таблиц.
- Классификация записей в реестре операций: `refresh_privileges` (bulk
  grant/revoke), `get_lock_materialization_rebuild` (берёт rebuild-lock),
  `check_lock` (делает heartbeat txn/lock в `TxnHandler`), `cm_recycle`,
  `map_schema_version_to_serde`, `put_file_metadata`, `clear_file_metadata` и
  `cache_file_metadata` теперь классифицируются как mutating writes, поэтому
  режимы доступа `READ_ONLY` и `READ_WRITE_DB_WHITELIST` их отклоняют. Удалена
  мёртвая запись реестра `rollback_txn` (такого RPC нет ни в одном
  поддерживаемом Iface; откат — это `abort_txn`).
- Identity клиента (`ClientRequestContext.remoteAddress`/`remoteUser`) теперь считывается
  внутри SASL-процессора, а не вокруг него. Hive'овский `TUGIAssumingProcessor` кладёт
  remote address и remote user текущего запроса в статические ThreadLocal уже внутри своего
  `process()` и никогда их не чистит, поэтому прежняя внешняя обёртка читала то, что оставило
  предыдущее соединение, обслуженное этим worker-потоком `TThreadPoolServer`. В результате
  первый RPC каждого нового соединения выполнялся с identity предыдущего клиента этого потока.
  Это влияло на решение `guard.transactional-ddl.client-addresses` (залипший разрешённый адрес
  обходил guard, залипший чужой адрес блокировал легитимного клиента), на учёт токенов
  rate-limit (`rate-limit.principal.*`, `rate-limit.source.*`, `rate-limit.source-cidrs.*`
  списывались из чужого bucket) и на поля `remoteAddress`/`authenticatedUser` в audit-логе.
  Impersonation на бэкенд затронута не была: пользователь берётся из UGI-контекста `doAs`,
  который SASL-процессор выставляет корректно.
- Rewrite текста вью (`federation.view-text-rewrite.mode=REWRITE`) стал контекстно-безопасным.
  Прежний regex матчил любую пару `x.y` в любом месте SQL, поэтому определение вью могли испортить
  алиас таблицы, совпавший с именем БД, значение с точкой внутри строкового литерала или
  комментария, а также cross-catalog ссылки вроде `other_cat.sales.t`, у которых молча пропадал
  catalog-префикс. Теперь лексический сканер пропускает строковые литералы, комментарии `--` и
  `/* */`, числа и идентификаторы в backquote, а переписывает только database-квалификатор ссылки,
  стоящей в table-позиции (`FROM`, `JOIN`, `INTO`, `TABLE`, `UPDATE`). Трёхчастные ссылки
  `catalog.db.table` сохраняют catalog-префикс: на выходе схлопывается только
  `<backend catalog>.<db>.<table>` в имя внешней БД. Всё, что нельзя разрешить однозначно,
  остаётся нетронутым и логируется на уровне `DEBUG`.
- Shutdown hook теперь дожидается полной упорядоченной остановки. Раньше он
  останавливал только primary listener и сразу завершался, из-за чего JVM успевала
  сделать halt до того, как main-поток закроет дополнительные frontend listener'ы,
  management listener, backend-ресурсы router'а и front-door security.
- `MetastoreThriftServer.stop()` больше не гоняется с `serve()`. Остановка, попавшая
  в окно до сброса внутреннего флага `stopped_` в libthrift, раньше либо пропускалась
  целиком (guard `isServing()`), либо затиралась, оставляя accept-цикл крутиться на
  закрытом сокете. Потоки дополнительных listener'ов стали daemon-потоками, поэтому
  авария при старте одного listener'а больше не может оставить зомби-JVM с занятыми
  портами.
- `MetastoreThriftServer.stop()` больше не закрывает общий `FrontDoorSecurity`.
  Остановка одного listener'а раньше глушила потоки delegation-token secret manager'а
  для всех остальных; теперь закрывает тот компонент, который его создал.

### Производительность

- Rewrite текста вью больше не обходит весь граф результата через рефлексию. Thrift-поля, из
  которых транзитивно достижим `Table`, кэшируются по классу, поэтому поддеревья без view-текста
  (партиции, storage descriptors, column statistics) пропускаются целиком: ответ `get_partitions` с
  тысячами партиций больше не стоит тысяч reflective-вызовов на запрос. Переписывание найденной
  ссылки тоже больше не компилирует новый `Pattern` на каждый матч.
- Комплектный дефолтный `log4j.properties` больше не выбрасывает вывод proxy молча. Logger пакета
  proxy стоял на `DEBUG` с `additivity=false` и без собственных appender'ов, поэтому каждая строка
  proxy — включая structured audit record — рендерилась и отправлялась в никуда. У audit logger
  теперь свой appender `logs/hms-proxy-audit.log` (rolling по 100MB, 10 backup, без префикса
  layout, чтобы файл оставался валидным JSON lines).
- Per-request debug tracing выключен по умолчанию. Пакет proxy теперь логируется на `INFO`, так что
  `DebugLogUtil` больше не рендерит все аргументы запроса и backend-ответы на каждый RPC. Чтобы
  вернуть прежнее поведение, поставь `log4j.logger.io.github.mmalykhin.hmsproxy=DEBUG`.
- Management HTTP listener обслуживает запросы из выделенного пула потоков (`management.threads`,
  по умолчанию 4) вместо единственного встроенного dispatcher-потока. Вызов `/readyz`, залипший на
  недоступном backend или KDC, больше не блокирует liveness-проверки `/healthz` и scrape
  `/metrics`.
- `DebugLogUtil` рендерит в единый буфер с общим лимитом, поэтому коллекция больших Thrift-объектов
  перестаёт материализоваться, как только исчерпан бюджет ~4000 символов, вместо того чтобы строить
  и обрезать каждый элемент целиком.

## 2026-05-26

### Добавлено

- Hive 4.1.x backend adapter. `APACHE_4_1_0` теперь принимается как backend
  runtime profile per-catalog (`catalog.<name>.runtime-profile=APACHE_4_1_0`),
  когда внешний HMS этого каталога уже работает на Hive 4. Новый
  `Hive4BackendAdapter` поднимает два positional read метода, удалённых в
  Hive 4 (`get_table`, `get_table_objects_by_name`), до их `*_req`
  эквивалентов и разворачивает ответ обратно в Apache 3.1.3 return type;
  всё остальное идёт через стандартный изолированный `IMetaStoreClient` и
  binary-compatible Thrift делегацию. `BackendRuntime` и
  `BackendInvocationSession` теперь активируют изолированный classloader
  для любого профиля, у которого новый `MetastoreRuntimeProfile#requiresIsolation()`
  возвращает true (Hortonworks 3.1.0.x или Hive 4.1.0).

## 2026-05-25

### Добавлено

- Несколько Thrift front-end listener'ов на разных портах через
  `additional-frontends.<name>.*`. Каждый дополнительный listener выставляет
  свой `frontend-profile` (и использует свой `standalone-metastore-jar` для
  не-`APACHE_3_1_3` профилей), но шарит общий `RoutingMetaStoreProxy`,
  federation, security, audit и Prometheus стек с primary listener'ом.
  Позволяет, например, поднять в одном JVM Apache 3.1.3 listener на 9083 и
  Hortonworks 3.1.0.x на 9084; клиенты должны коннектиться на нужный порт,
  потому что Thrift-протокол не имеет version-negotiation handshake.
  Валидация: уникальные имена listener'ов, уникальные `bindHost:port`,
  коллизия с primary портом отклоняется, jar required для не-Apache профилей.
- Hive 4.1.x front-door bridge (`compatibility.frontend-profile=APACHE_4_1_0`).
  Принимает Hive 4 Thrift-клиентов и обслуживает их против Apache 3.1.3 backend
  через изолированный classloader и динамический Proxy — симметрично уже
  существующему `HortonworksFrontendBridge`. Покрывает 199 методов, общих с
  Apache 3.1.3, через binary-compatible Thrift делегацию плюс explicit
  positional mapping для Hive 4-only `*_req` wrappers, которые большинство
  клиентов вызывают на read/стандартном DDL (`get_database_req`,
  `get_databases_req`, `get_table_req`, `get_partition*_req`, `get_fields_req`,
  `create_table_req`, `drop_table_req`, `alter_table_req`, `truncate_table_req`
  и т.п.). Truly Hive 4-only API (data connectors, scheduled queries, stored
  procedures, packages, ACID v2 extensions) отвечают
  `TApplicationException UNKNOWN_METHOD`.
- `hive-metastore/hive-standalone-metastore-common-4.1.0.jar` добавлен в
  bundle для isolated frontend runtime.
- `APACHE_4_1_0` enum value в `FrontendProfile` и `MetastoreRuntimeProfile`.
  Последний запрещает использовать себя как backend (`BackendAdapterFactory`
  throws) — Hive 4 поддержан только как front-door profile.

## 2026-05-19

### Изменено

- Изолированный backend classloader теперь переиспользуется между перезагрузками
  `BackendRuntime` для одной и той же пары profile + jar, а не пересоздаётся каждый раз.
  Снижает classloader churn (и соответствующее давление на metaspace), когда несколько
  каталогов делят один изолированный runtime, и сокращает latency reconnect/reload.

## 2026-05-03

### Добавлено

- Серия версий nightly-сборок повышена с `0.1.x` до `1.0.x`: jgitver теперь выпускает
  `hms-proxy-1.0.<distance>-<sha>.jar` вместо `0.1.<distance>-<sha>.jar`, соответствуя
  заданной release-серии после `v1.0.0`. Тегированные сборки не затронуты
  (`hms-proxy-1.0.0.jar` для `v1.0.0`).

### Изменено

- Пробы `/readyz` к бэкендам теперь выполняются на выделенном bounded executor, размер
  которого задаётся через `routing.backend-state-polling.max-parallelism`, под общим
  дедлайном, прокинутым в `probeConnectivity(timeoutMs)` — так что таймаут на самом сокете
  тоже учитывает probe-бюджет. Раньше при выключенном backend-state polling каждый запрос
  readiness фанаутил `checkConnectivity()` через общий `ForkJoinPool` и джойнил без
  таймаута, из-за чего медленный или зависший HMS превращал `/readyz` в источник нагрузки
  и истощал общий пул. Probe-executor останавливается вместе с management-сервером.

### Исправлено

- Параллельные fanout-воркеры больше не мутируют родительский `RequestObservation` через
  ThreadLocal-пропагацию. У каждого воркера теперь собственный одноразовый observation, а
  сигнал compat-fallback возвращается родителю через `FanoutTaskResult`, так что родительский
  observation обновляется только в потоке запроса (раньше compat-fallback пути из
  воркер-потоков могли гонять non-volatile state).

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

- **Impersonation:** для каждого пользователя теперь поднимается персональный borrow/return
  пул backend Thrift-сессий вместо одной общей сессии, сериализованной через один транспорт.
  Размер пула и idle TTL настраиваются per-catalog: `catalog.<name>.impersonation-pool-max-size`
  (дефолт `4`) и `catalog.<name>.impersonation-session-idle-ttl-ms` (дефолт `0` — никогда не
  закрывать idle). Borrow-таймаут ограничен `latency-budget-ms` каталога; transport-фейл
  отбрасывает только сбойную сессию и retry-once делается на свежей. Adaptive-timeout
  reconnect и LRU-вытеснение per-user закрывают все сессии затронутого пользователя.

- Borrow из shared backend session pool теперь fail-fast, а не ждёт бесконечно. Borrow-путь
  использует `tryAcquire` с границей `latencyBudgetMs` каталога (или 30 s по умолчанию);
  exhaustion логируется WARN и поднимается клиенту как `MetaException`. Те же bounded
  `tryAcquire` применены в `reconnectShared()` и `close()`, чтобы admin-операции не зависали
  на in-flight RPC, удерживающих permits.

- Backend health-polling теперь параллельный и ограниченный. Новый параметр
  `routing.backend-state-polling.max-parallelism` (дефолт: число каталогов) задаёт размер
  выделенного `ThreadPoolExecutor`, который сабмитит все пробы параллельно под общим
  дедлайном — вместо последовательных `Future.get` по каждому бэкенду. На 20 бэкендах с
  таймаутом 5 s цикл polling падает с примерно 100 s до примерно 5 s.

### Добавлено

- Два новых Prometheus счётчика отражают динамику adaptive timeout:
  `hms_proxy_adaptive_timeout_reconnect_total{catalog}` для применённых реконнектов и
  `hms_proxy_adaptive_timeout_reconnect_skipped_total{catalog,reason}` для событий,
  подавленных hysteresis или cooldown. В Grafana dashboard добавлены три новых панели —
  общий rate реконнектов, per-catalog timeseries и стек подавленных событий по reason.

- Новые Prometheus-метрики для per-user impersonation pool:
  `hms_proxy_impersonation_pool_users{catalog}` (распределённые пользователи, кэшированные
  сейчас), `hms_proxy_impersonation_pool_sessions{catalog,state=active|idle}` (сессии по
  состоянию), `hms_proxy_impersonation_session_acquire_timeouts_total{catalog}` (per-user
  borrow-таймауты) и
  `hms_proxy_impersonation_session_evictions_total{catalog,reason=idle|transport_failure|user_evicted|user_capacity}`.
  В Grafana dashboard добавлена секция "Impersonation Pool" с четырьмя панелями по этим
  метрикам.

- Новый Prometheus-счётчик
  `hms_proxy_backend_session_acquire_timeouts_total{catalog,operation}` для fail-fast
  событий на shared backend session pool. `operation=borrow` — обычная RPC-диспетчеризация;
  `operation=reconnect` — admin reconnect, который не смог quiesce пул. В Grafana dashboard
  добавлены соответствующие панели.

### Исправлено

- Значения `Gauge` теперь хранятся как `AtomicLong` (через `Double.doubleToRawLongBits`)
  вместо `DoubleAdder`, что даёт lock-free атомарный `set()`/read. Прежний путь через
  `DoubleAdder` использовал `add(-current); add(value)` под локом, и конкурентные читатели
  могли видеть частичное обновление между двумя add.
- Кэш рефлексии в `ThriftReflectionCache` переехал со статической `ConcurrentHashMap` по
  ключу `Class<?>` на `ClassValue`, чтобы записи были привязаны к жизненному циклу `Class`
  и освобождались при перезагрузке изолированного runtime. Предотвращает classloader-утечки
  при повторных reload изолированного runtime.
- `IsolatedInvocationBridge` и cross-classloader конвертация `TBase` теперь кэшируют
  поиски `Method` и `Constructor`, убирая повторные `getMethod`/`getConstructor` на горячих
  путях.

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
