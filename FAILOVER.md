# Отказоустойчивость и обработка недоступности Hive Metastore

English version: [FAILOVER.en.md](FAILOVER.en.md)

В этом руководстве описано, как `hms-proxy` обрабатывает недоступность, сетевые сбои и деградацию удалённых бэкендов Hive Metastore (HMS) на разных этапах жизненного цикла.

---

## 1. Запуск прокси при недоступном бэкенде

При запуске приложения ([`HmsProxyApplication`](src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java)):
1. Прокси читает конфигурацию и инициализирует роутер каталогов ([`CatalogRouter`](src/main/java/io/github/mmalykhin/hmsproxy/routing/CatalogRouter.java)).
2. Для каждого сконфигурированного каталога (`catalog.<name>.*`) создаётся экземпляр [`CatalogBackend`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java) и открывается runtime-слой ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)).
3. Дальнейшее поведение зависит от настройки `catalog.<name>.startup-mode`:

| Режим | Поведение при недоступном бэкенде на старте |
| --- | --- |
| `STRICT` (по умолчанию) | Фабрика сессий сразу создаёт первичное соединение (`initialSession`). Если бэкенд недоступен, прокси исчерпывает попытки переподключения, выбрасывает `MetaException` и завершается с ненулевым кодом выхода (startup fail-fast), не открывая клиентские порты. Для дефолтного каталога (`routing.default-catalog`) действует только `STRICT`. |
| `LENIENT` (мягкий запуск) | При сбое подключения к удалённому или вторичному бэкенду прокси пишет предупреждение `proceeding in degraded mode (startup-mode=LENIENT)`, продолжает запуск и открывает клиентские порты. Соединения к такому каталогу создаются лениво: при первом запросе или после восстановления сети. |

> [!NOTE]
> Режим `catalog.<name>.startup-mode=LENIENT` рекомендуется для вторичных каталогов в других ЦОД или за WAN-каналами: тогда временная авария межгородского канала не мешает запуску локального прокси и обслуживанию основного ЦОД.

---

## 2. Сбой бэкенда во время работы прокси

Если бэкенд стал недоступен в процессе работы прокси, действуют следующие механизмы:

### 2.1. Пулы сессий и однократный повтор вызова
* Все сетевые вызовы к бэкенду обслуживаются пулами сессий:
  * Общим пулом shared-сессий ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)) размером `catalog.<name>.shared-session-pool-size`.
  * Пулом impersonation-сессий пользователя ([`CatalogBackend.ImpersonationClient`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java)) размером `catalog.<name>.impersonation-pool-max-size`.
* При транспортной ошибке (`TTransportException`, разрыв TCP-соединения, сброс сокета) повреждённая сессия признаётся невалидной и уничтожается (`discard`).
* Прокси берёт или создаёт из пула свежую сессию и выполняет ровно один автоматический повтор (`retrying once`), но только если повтор безопасен: вызов не меняет состояние метастора (чтение) либо `TTransportException` имеет тип `NOT_OPEN`, то есть запрос не был отправлен. Пишущий вызов (`create_table`, `add_partition`, `open_txns`, `commit_txn`, `lock` и т. п.), у которого потерялся ответ (таймаут чтения, сброс соединения, EOF), не повторяется: бэкенд мог его уже применить. Ошибка уходит клиенту, а решение о повторе принимает сам клиент (`RetryingMetaStoreClient`). Решение принимает `ThriftFailureClassifier.isSafeToReplay` по `HmsOperationPolicy.replayableAfterLostReply`; неизвестный прокси метод считается пишущим.
* Перед повтором прокси закрывает все простаивающие сессии того же пула (для impersonation — пула этого пользователя). Метастор обычно рвёт соединения разом, и без этого повтор брал из пула следующую мёртвую сессию: при трёх мёртвых сессиях чтение падало, хотя бэкенд уже отвечал. Исключение — таймаут чтения (`SocketTimeoutException`): соединение было живым, остальные сессии пула остаются. Закрытые так impersonation-сессии видны как `hms_proxy_impersonation_session_evictions_total{reason="stale_idle"}`.
* Вызов, который нельзя повторить, не отправляется по сессии, простоявшей в пуле дольше `catalog.<name>.session-validate-after-idle-ms` (дефолт `5000`, `0` отключает проверку), без проверки: прокси сначала шлёт по ней `getStatus`. Если проверка упала с транспортной ошибкой, сессия закрывается вместе с остальными простаивающими, и вызов уходит один раз по новой сессии. Чтения не проверяются — их покрывает повтор. Исход проверок — в `hms_proxy_backend_session_validations_total{catalog,pool="shared|impersonation",outcome="alive|stale"}`.
* Проверка не закрывает два случая. Соединение, оборванное между `getStatus` и самим вызовом, роняет вызов как раньше; такие отказы считает `hms_proxy_backend_calls_not_replayed_total{catalog,method,phase="send|receive|unknown"}`. Соединение, которое сеть молча перестала доставлять (без FIN/RST), держит `getStatus` до `hive.metastore.client.socket.timeout` каталога.
* Если повторный вызов также завершается ошибкой, сбой регистрирует подсистема контроля допуска запросов (admission control).

### 2.2. Circuit breaker
Если задано `routing.circuit-breaker.enabled=true`:
1. Учёт сбоев. Ошибки соединения (`TTransportException`), таймауты сокета (`SocketTimeoutException`) и рассинхронизация протокола (`TApplicationException` типов `WRONG_METHOD_NAME`, `BAD_SEQUENCE_ID`, `INVALID_MESSAGE_TYPE`) учитываются в состоянии бэкенда ([`ProxyRuntimeState`](src/main/java/io/github/mmalykhin/hmsproxy/observability/ProxyRuntimeState.java)). Остальные `TApplicationException` — это ответ живого бэкенда (`UNKNOWN_METHOD` при согласовании возможностей, `INTERNAL_ERROR`, которым libthrift заворачивает любую ошибку метода без объявленных исключений), поэтому они не открывают контур и не помечают бэкенд недоступным.
2. Переход в `OPEN`. После серии ошибок подряд (`routing.circuit-breaker.failure-threshold`, по умолчанию `3`) состояние контура переходит в `OPEN`.
3. Немедленный отказ клиентам (fast-fail). Все последующие запросы к этому каталогу отклоняются сразу, без блокировки потоков и ожидания сетевого таймаута:
   ```text
   MetaException: Backend catalog '<name>' rejected method '<method>' because circuit_open; next retry window in <X>ms
   ```
4. Пробный переход в `HALF_OPEN`. По истечении интервала ожидания (`routing.circuit-breaker.open-state-ms`, по умолчанию `30000` мс) контур переходит в состояние `HALF_OPEN`. Ровно один клиентский запрос пропускается для проверки доступности:
   * При успешном ответе контур возвращается в `CLOSED`, счётчик сбоев сбрасывается.
   * При повторном сбое контур снова переходит в `OPEN` на заданный интервал.

### 2.3. Адаптивный таймаут сокета
Если задано `routing.adaptive-timeout.enabled=true`:
* Прокси рассчитывает скользящее среднее (EWMA) задержки ответов бэкенда.
* При росте задержек или единичных таймаутах прокси меняет сокет-таймаут клиента в пределах `[min-timeout-ms, max-timeout-ms]`, чтобы соединения не обрывались по таймауту при временной нагрузке на удалённый HMS.

### 2.4. Нормализация сетевых ошибок в `MetaException`
* В Hive Thrift IDL инфраструктурные сетевые исключения (`TTransportException`, `TApplicationException`) не объявлены в секции `throws` большинства методов. Без специальной обработки библиотека Thrift перехватывала бы их и возвращала клиенту обезличенное сообщение `TApplicationException("Internal error processing <method>")`, скрывая первопричину сбоя.
* [`BackendErrorNormalizer`](src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendErrorNormalizer.java) перехватывает сетевые ошибки и преобразует их в `MetaException`, который объявлен в `throws` методов Hive:
   ```text
   MetaException: Backend catalog 'hdp' failed in method 'get_table' with TTransportException: java.net.SocketException: Connection reset
   ```
  Клиенты (Spark, HiveServer2, Trino, Impala) получают в тексте исключения исходную причину сетевого отказа.
* Отказ подключиться к бэкенду клиент метастора сам сообщает как `MetaException("Could not connect to meta store using any of the URIs provided…")`. В нём есть хост, но нет каталога, поэтому прокси оборачивает и его:
   ```text
   MetaException: Backend catalog 'apache' failed in method 'create_table' with MetaException: Could not connect to meta store using any of the URIs provided. Most recent failure: org.apache.thrift.transport.TTransportException: java.net.UnknownHostException: hms-apache
   ```
  Остальные `MetaException` и другие исключения из `org.apache.hadoop.hive.metastore.api` (`NoSuchObjectException` и т. п.) уходят клиенту без изменений.

### 2.5. Пустые ответы сервисных методов при сбое
* Для вспомогательных и диагностических вызовов метастора, сбой которых не нарушает консистентность данных (например, `get_active_resource_plan`, `get_all_resource_plans`, `get_runtime_stats`), слой совместимости ([`CompatibilityLayer`](src/main/java/io/github/mmalykhin/hmsproxy/compatibility/CompatibilityLayer.java)) перехватывает сбой и возвращает пустой валидный ответ, и пользовательская сессия не падает.
* Для критичных методов (чтение схемы, блокировки, транзакции, права) пустые ответы ошибку не маскируют: клиент получает явное исключение.

### 2.6. Кэши метаданных таблиц и разделов, режим serve-stale-on-error
* Для снижения числа запросов через межгородские каналы (WAN) с высокой задержкой есть потокобезопасные кэши:
  * [`TableMetadataCache`](src/main/java/io/github/mmalykhin/hmsproxy/routing/TableMetadataCache.java) (`routing.cache.table-metadata.*`) для вызовов `get_table`.
  * [`PartitionMetadataCache`](src/main/java/io/github/mmalykhin/hmsproxy/routing/PartitionMetadataCache.java) (`routing.cache.partition-metadata.*`) для вызовов `get_partition*`, `get_part_specs*`.
* SingleFlight: параллельные запросы к одной таблице объединяются в один запрос к бэкенду, поэтому промах кэша не порождает шквал запросов в WAN-канал (cache stampede).
* DDL-инвалидация: операции изменения (`alter_table*`, `drop_table*`, `truncate_table*`, `drop_database`) сразу сбрасывают закэшированные записи для затронутых таблиц, разделов и баз данных.
* Распределённая DDL-инвалидация через ZooKeeper для нескольких экземпляров прокси (`routing.cache.distributed-invalidation.mode=ZOOKEEPER`):
  * При горизонтальном масштабировании `hms-proxy` на группу реплик за L4/L7-балансировщиком DDL-мутация, поступившая на любой из инстансов, публикует событие инвалидации в ZooKeeper znode (`routing.cache.distributed-invalidation.base-path`, по умолчанию `/hms-proxy-cache-invalidation`).
  * Все подключённые реплики `hms-proxy` получают уведомление через Apache Curator `PathChildrenCache` и сбрасывают локальные кэши затронутых таблиц, разделов и баз данных.
  * Подавление эха: каждый инстанс идентифицируется через `instance-id` и отфильтровывает собственные события инвалидации.
  * Сброс при потере ZooKeeper: при разрыве связи с ZooKeeper (`LOST` или `SUSPENDED`) прокси очищает все локальные кэши, чтобы изолированная нода не отдавала устаревшие метаданные.
  * Очистка старых событий: фоновый процесс периодически (`purge-interval-ms`) удаляет отработавшие события старше окна хранения (`event-retention-ms`, по умолчанию 5 минут).
* Serve-stale-on-error (`routing.cache.serve-stale-on-error=true`): при аварии сетевого подключения или недоступности удалённого HMS прокси отдаёт устаревшие метаданные из кэша в течение льготного периода (`routing.cache.stale-grace-period-ms`, по умолчанию 5 минут). Запросы чтения при кратковременном разрыве WAN-связи получают метаданные из кэша вместо ошибки.

### 2.7. Ограничение одновременных вызовов к каталогу
Чтобы медленный межкластерный канал не перегружался, а пулы сессий не исчерпывались, у каждого каталога можно ограничить число одновременных in-flight RPC:

| Параметр | По умолчанию | Что делает |
| --- | --- | --- |
| `catalog.<name>.max-concurrent-calls` | `0` (без ограничений) | Максимум одновременных вызовов к каталогу. |
| `catalog.<name>.concurrency-timeout-ms` | `10000` мс | Сколько вызов ждёт свободный слот. |

Вызовы сверх лимита ждут в очереди на семафоре. Если за время таймаута слот не освободился, запрос отклоняется с `MetaException`, и поток клиента не висит дольше этого таймаута.

### 2.8. Переключение чтения на теневую реплику каталога
Если для удалённого каталога настроена локальная копия метаданных (например, реплицируемая база MySQL/PostgreSQL metastore-бэкенда), прокси может переключать на неё чтение:

| Параметр | Значение | Что делает |
| --- | --- | --- |
| `catalog.<name>.fallback-catalog` | `<shadow_catalog_name>` | Каталог-реплика, на который уходит чтение. |
| `catalog.<name>.fallback-on-outage` | `true` | Включает переключение при аварии. |

* Переключение чтения: при аварии (открытый circuit breaker, `TTransportException`, таймауты) методы чтения метаданных (`get_table`, `get_database`, `get_partitions`, `get_fields` и т. д.) перенаправляются на локальный теневой каталог.
* Запрет записи (no split-brain): мутирующие запросы (`create_*`, `alter_*`, `drop_*`, `truncate_*`, запись Iceberg) на реплику не перенаправляются и завершаются явной ошибкой, поэтому реплика не расходится с основным каталогом.
* Каждое переключение на реплику фиксируется в метрике `hms_proxy_catalog_fallback_total`.

---

## 3. Fanout-запросы по всем каталогам (`SHOW DATABASES`, `get_table_meta`)

Операции, которые опрашивают все сконфигурированные бэкенды параллельно или последовательно ([`FanoutExecutor`](src/main/java/io/github/mmalykhin/hmsproxy/routing/FanoutExecutor.java)), ведут себя по-разному в зависимости от `routing.degraded-routing-policy`:

| Значение `routing.degraded-routing-policy` | Поведение при сбое одного из каталогов |
| --- | --- |
| `STRICT` (по умолчанию) | Сбой любого из опрашиваемых каталогов приводит к ошибке всей операции. Клиент получает `MetaException` с именем отказавшего бэкенда. |
| `SAFE_FANOUT_READS` | Прокси исключает недоступный каталог из выборки и возвращает агрегированный результат от остальных каталогов. В лог пишется предупреждение `omitting degraded backend catalog=<name> from safe fanout method=<method>`, метрика запроса помечается `degraded=true`. |

`SAFE_FANOUT_READS` действует только на операции чтения метаданных `get_all_databases`, `get_databases`, `get_table_meta`. Операции записи и обращения к конкретному каталогу остаются строгими.

---

## 4. Последствия отказа `default-catalog` и вторичных каталогов

Последствия недоступности бэкенда зависят от его роли в конфигурации.

### Недоступен вторичный (secondary) каталог
* Пострадают только запросы, адресующие базы данных и таблицы этого каталога (например, `catalog2__analytics.events`).
* Запросы к `default-catalog` и другим работоспособным каталогам продолжают обслуживаться штатно.

### Недоступен `default-catalog`
* Отказ control-plane:
  * Все глобальные Thrift RPC без квалифицированного имени БД (`getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns`, `get_open_txns_info`) жёстко привязаны к `default-catalog`. При его отказе эти вызовы невозможно обслужить.
  * Механизм транзакций и распределённых блокировок Hive (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `heartbeat`) управляется бэкендом `default-catalog`. При его недоступности транзакционные DDL/DML блокируются.
  * Все операции изменения схемы и данных через шлюз Iceberg REST Catalog (`WriteRouteGate`) разрешены только для таблиц `default-catalog`. Соответственно, запись в Iceberg-таблицы прекращается.

---

## 5. Ответы Iceberg REST Catalog при недоступном бэкенде

* При недоступности бэкенда, обслуживающего Iceberg-каталог, HTTP-обработчик ([`IcebergHttpHandler`](src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java)) транслирует ошибки метастора в спецификацию REST Catalog:
  * Клиент получает статус HTTP 500 (Internal Server Error) или HTTP 503 (Service Unavailable).
  * Ответ содержит структурированный JSON объекта `ErrorResponse` с типом ошибки и сообщением в формате, который разбирают REST-клиенты Iceberg (Spark, Trino, Flink, PyIceberg).

---

## 6. Health checks и метрики состояния бэкендов

Служебный HTTP-сервер ([`ManagementHttpServer`](src/main/java/io/github/mmalykhin/hmsproxy/app/ManagementHttpServer.java)) отдаёт эндпоинты и метрики состояния бэкендов.

### 6.1. Эндпоинты `/healthz` и `/readyz`
* `/healthz` (liveness probe):
  * Всегда возвращает HTTP 200 OK (`{"status":"ok","alive":true,...}`), если процесс JVM запущен и способен принимать HTTP-запросы. Служит для K8s liveness probe.
* `/readyz` (readiness probe):
  * Проверяет состояние соединений сконфигурированных бэкендов; набор учитываемых каталогов настраивается:
    * По умолчанию прокси проверяет готовность всех каталогов.
    * Для вторичных или удаленных каталогов можно задать `catalog.<name>.required-for-readiness=false`. В этом случае недоступность такого каталога не блокирует готовность прокси: `/readyz` продолжает возвращать HTTP 200 OK (`{"status":"ready",...}`), а в массиве бэкендов этот каталог помечается как `"requiredForReadiness":false,"connected":false`. Так авария в удалённом ЦОД не выводит локальный прокси из ротации локального балансировщика нагрузки.
    * Глобальный флаг `management.readyz.require-all-catalogs=false`, делает готовность инстанса зависящей только от доступности `default-catalog`.
  * Эндпоинт возвращает HTTP 503 Service Unavailable с `"status":"degraded"`, если хотя бы один обязательный бэкенд (`requiredForReadiness=true`) не подключён, помечен `degraded` или его circuit breaker не в состоянии `CLOSED` (`OPEN` либо `HALF_OPEN`), а также если Kerberos front door или бэкенда нездоров. `"backendConnectivity":false` выставляется только в первом случае, когда обязательный бэкенд не подключён.
  * Балансировщики нагрузки (HAProxy, Envoy, Kubernetes Ingress/Service) по этому ответу выводят инстанс прокси из пула активной маршрутизации.
  * Без фонового опроса (`routing.backend-state-polling.enabled=false`) `/readyz` сам пробует бэкенды, не чаще раза в `management.readiness-cache-ms`. Пробы идут по тем же правилам, что и в фоновом опросе (общий [`BackendProbeRunner`](src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendProbeRunner.java)): проба, завершившаяся до конца ожидания, засчитывается по своему результату, даже если общий дедлайн выбрала зависшая проба другого бэкенда, а бэкенд с ещё не вернувшейся прошлой пробой повторно не пробуется и отмечается `previous probe still running`. Раньше в этом пути зависшая проба удалённого каталога выбирала общий дедлайн, и `/readyz` около 25 секунд отвечал 503 с `default-catalog` в `lastError: TimeoutException: readiness probe deadline exceeded`.

### 6.2. Фоновый опрос доступности бэкендов
* Если задано `routing.backend-state-polling.enabled=true`, отдельный планировщик периодически проверяет доступность каждого бэкенда вызовом `getStatus` с таймаутом `probe-timeout-ms`.
* Авария или восстановление метастора обнаруживаются без клиентских запросов, а внешние проверки `/readyz` не ждут сетевого вызова к бэкенду.

### 6.3. Метрики Prometheus (`/metrics`)
При сбоях бэкендов обновляются метрики:

| Метрика | Что показывает |
| --- | --- |
| `hms_proxy_backend_failures_total{backend="<name>", error="<class>"}` | Счётчик сбоев бэкенда по типам ошибок. |
| `hms_proxy_backend_status{backend="<name>", state="connected\|degraded"}` | Текущий статус подключения. |
| `hms_proxy_circuit_state{backend="<name>"}` | Текущее состояние circuit breaker (`0 = CLOSED`, `1 = OPEN`, `2 = HALF_OPEN`). |
| `hms_proxy_backend_session_acquire_timeouts_total{backend="<name>", reason="borrow\|reconnect"}` | Таймауты ожидания сессии из-за перегрузки или недоступности бэкенда. |
| `hms_proxy_impersonation_session_evictions_total{catalog="<name>", reason="transport_failure\|stale_idle\|stale_probe"}` | Сессии пользователей, закрытые из-за обрыва: `transport_failure` — на ней упал вызов, `stale_idle` — простаивала в пуле в момент обрыва, `stale_probe` — не прошла проверку перед записью. |
| `hms_proxy_backend_session_validations_total{catalog="<name>", pool, outcome="alive\|stale"}` | Проверки простаивавшей сессии перед вызовом, который нельзя повторить. |
| `hms_proxy_backend_calls_not_replayed_total{catalog="<name>", method, phase}` | Вызовы, отданные клиенту с транспортной ошибкой без повтора. Рост означает, что клиенты видят отказы записи. |
