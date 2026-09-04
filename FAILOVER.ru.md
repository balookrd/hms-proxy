# Отказоустойчивость и обработка недоступности Hive Metastore

English version: [FAILOVER.md](FAILOVER.md)

В этом документе описано, как **hms-proxy** обрабатывает недоступность, сетевые сбои и деградацию удалённых бэкендов Hive Metastore (HMS) на различных этапах жизненного цикла.

---

## 1. Поведение при запуске прокси (Startup)

При запуске приложения ([`HmsProxyApplication`](src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java)):
1. Прокси читает конфигурацию и инициализирует роутер каталогов ([`CatalogRouter`](src/main/java/io/github/mmalykhin/hmsproxy/routing/CatalogRouter.java)).
2. Для каждого сконфигурированного каталога (`catalog.<name>.*`) создаётся экземпляр [`CatalogBackend`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java) и открывается runtime-слой ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)).
3. В рамках инициализации пула сессий фабрика сессий немедленно пытается создать первичное соединение (`initialSession`) через `HiveMetaStoreClient` или изолированный клиент.

> [!WARNING]
> **Startup Fail-Fast**: Если хотя бы один из сконфигурированных бэкендов недоступен на этапе старта, `HiveMetaStoreClient` исчерпывает попытки переподключения (`hive.metastore.connect.retries`), после чего выбрасывает `MetaException`. В результате прокси аварийно завершает работу с ненулевым кодом выхода и **не открывает клиентские порты**.

---

## 2. Поведение во время работы (Runtime) при сбое бэкенда

Если бэкенд стал недоступен в процессе работы прокси, вступают в действие следующие механизмы защиты:

### 2.1. Пул сессий и автоматический однократный Retry
* Все сетевые вызовы к бэкенду обслуживаются пулами сессий:
  * Общим пулом shared-сессий ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)) размером `catalog.<name>.shared-session-pool-size`.
  * Пулом impersonation-сессий пользователя ([`CatalogBackend.ImpersonationClient`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java)) размером `catalog.<name>.impersonation-pool-max-size`.
* При возникновении транспортной ошибки (`TTransportException`, разрыв TCP-соединения, сброс сокета) повреждённая сессия признаётся невалидной и уничтожается (`discard`).
* Прокси берет/создает из пула свежую сессию и выполняет **ровно один автоматический повтор** (`retrying once`).
* Если повторный вызов также завершается ошибкой, сбой регистрируется подсистемой контроля admission.

### 2.2. Circuit Breaker (Предохранитель)
Если включен механизм защиты `routing.circuit-breaker.enabled=true`:
1. **Регистрация сбоев**: Ошибки соединения (`TTransportException`), таймауты сокета (`SocketTimeoutException`) и сбои протокола учитываются в состоянии бэкенда ([`ProxyRuntimeState`](src/main/java/io/github/mmalykhin/hmsproxy/observability/ProxyRuntimeState.java)).
2. **Переход в `OPEN`**: При накоплении серии ошибок подряд (`routing.circuit-breaker.failure-threshold`, по умолчанию `3`), состояние контура переходит в `OPEN`.
3. **Fast-Fail клиентов**: Все последующие запросы к данному каталогу отклоняются мгновенно без блокировки потоков и ожидания сетевого таймаута:
   ```text
   MetaException: Backend catalog '<name>' rejected method '<method>' because circuit_open; next retry window in <X>ms
   ```
4. **Пробный переход `HALF_OPEN`**: По истечении интервала ожидания (`routing.circuit-breaker.open-state-ms`, по умолчанию `30000` мс) контур переходит в состояние `HALF_OPEN`. Ровно один клиентский запрос пропускается для проверки доступности:
   * При успешном ответе контур возвращается в `CLOSED`, счётчик сбоев сбрасывается.
   * При повторном сбое контур снова переходит в `OPEN` на заданный интервал.

### 2.3. Адаптивный таймаут сокета (Adaptive Socket Timeout)
Если включен `routing.adaptive-timeout.enabled=true`:
* Прокси рассчитывает скользящее среднее (EWMA) задержки ответов бэкенда.
* При обнаружении роста задержек или единичных таймаутов прокси динамически корректирует сокет-таймаут клиента в пределах `[min-timeout-ms, max-timeout-ms]`, предотвращая преждевременные обрывы при временных нагрузках на удаленный HMS.

### 2.4. Нормализация ошибок для Thrift-клиентов
* В Hive Thrift IDL инфраструктурные сетевые исключения (`TTransportException`, `TApplicationException`) не объявлены в секции `throws` большинства методов. Без специальной обработки библиотека Thrift перехватывала бы их и возвращала клиенту обезличенное сообщение `TApplicationException("Internal error processing <method>")`, скрывая первопричину сбоя.
* Для прозрачности диагностики [`BackendErrorNormalizer`](src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendErrorNormalizer.java) перехватывает сетевые ошибки и нормализует их в стандартное для экосистемы Hive `MetaException`:
   ```text
   MetaException: Backend catalog 'hdp' failed in method 'get_table' with TTransportException: java.net.SocketException: Connection reset
   ```
  Это позволяет клиентам (Spark, HiveServer2, Trino, Impala) логировать точную причину сетевого отказа.

### 2.5. Compatibility Fallbacks для сервисных методов
* Для вспомогательных и диагностических вызовов метастора, сбой которых не нарушает консистентность данных (например, `get_active_resource_plan`, `get_all_resource_plans`, `get_runtime_stats`), слой совместимости ([`CompatibilityLayer`](src/main/java/io/github/mmalykhin/hmsproxy/compatibility/CompatibilityLayer.java)) перехватывает сбой и возвращает пустой валидный ответ вместо падения всей пользовательской сессии.
* Для критичных методов (чтение схемы, блокировки, транзакции, права) пустые ответы никогда не маскируют ошибку — клиент гарантированно получает явное исключение.

---

## 3. Федеративные и Fanout-запросы (`SHOW DATABASES`, `get_table_meta`)

При выполнении операций, которые опрашивают сразу все сконфигурированные бэкенды в параллельном или последовательном режиме ([`FanoutExecutor`](src/main/java/io/github/mmalykhin/hmsproxy/routing/FanoutExecutor.java)):

* **Режим по умолчанию — `STRICT` (`routing.degraded-routing-policy=STRICT`)**:
  Сбой любого из опрашиваемых каталогов приводит к ошибке всей операции. Клиент получает `MetaException`, сигнализирующий о сбое конкретного бэкенда.
* **Режим деградации — `SAFE_FANOUT_READS` (`routing.degraded-routing-policy=SAFE_FANOUT_READS`)**:
  Прокси исключает недоступный каталог из выборки и возвращает агрегированный результат от всех живых каталогов.
  * В лог прокси пишется предупреждение: `omitting degraded backend catalog=<name> from safe fanout method=<method>`.
  * Метрика запроса помечается флагом `degraded=true`.
  * Этот режим действует исключительно на безопасные операции чтения метаданных (`get_all_databases`, `get_databases`, `get_table_meta`). Любые операции записи и точечные обращения остаются строгими.

---

## 4. Разделение ролей: `default-catalog` и secondary-каталоги

Последствия недоступности бэкенда принципиально зависят от его роли в конфигурации:

### Недоступен вторичный (secondary) каталог
* Пострадают только запросы, адресующие базы данных и таблицы этого каталога (например, `catalog2__analytics.events`).
* Запросы к `default-catalog` и другим работоспособным каталогам продолжают обслуживаться штатно.
* Изоляция гарантирует, что сбой внешней или аналитической метабазы не парализует работу основного кластера.

### Недоступен `default-catalog`
* **Критический отказ control-plane**:
  * Все глобальные Thrift RPC без квалифицированного имени БД (`getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns`, `get_open_txns_info`) жестко привязаны к `default-catalog`. При его отказе данные вызовы не могут быть обслужены.
  * Механизм транзакций и распределенных блокировок Hive (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `heartbeat`) управляется бэкендом `default-catalog`. При его недоступности транзакционные DDL/DML блокируются.
  * Все операции изменения схемы и данных через шлюз Iceberg REST Catalog (`WriteRouteGate`) разрешены только для таблиц `default-catalog`. Соответственно, запись в Iceberg-таблицы прекращается.

---

## 5. Поведение шлюза Iceberg REST Catalog

* При недоступности бэкенда, обслуживающего Iceberg-каталог, HTTP-обработчик ([`IcebergHttpHandler`](src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java)) транслирует ошибки метастора в спецификацию REST Catalog:
  * Клиент получает статус **HTTP 500 (Internal Server Error)** или **HTTP 503 (Service Unavailable)**.
  * Ответ содержит структурированный JSON объекта `ErrorResponse` с типом ошибки и подробным сообщением, понятным REST-клиентам Iceberg (Spark, Trino, Flink, PyIceberg).

---

## 6. Мониторинг, Health Checks и наблюдаемость

Служебный HTTP-сервер ([`ManagementHttpServer`](src/main/java/io/github/mmalykhin/hmsproxy/app/ManagementHttpServer.java)) предоставляет точки мониторинга состояния бэкендов:

### 6.1. Эндпоинты проверки жизнеспособности
* **`/healthz` (Liveness probe)**:
  * Всегда возвращает **HTTP 200 OK** (`{"status":"ok","alive":true,...}`), если процесс JVM запущен и способен принимать HTTP-запросы. Служит для K8s liveness probe.
* **`/readyz` (Readiness probe)**:
  * Опрашивает состояние соединений всех бэкендов.
  * Если хотя бы один бэкенд недоступен, отключен или его Circuit Breaker находится в состоянии `OPEN`, эндпоинт возвращает **HTTP 503 Service Unavailable** (`{"status":"degraded","backendConnectivity":false,...}`).
  * Балансировщики нагрузки (HAProxy, Envoy, Kubernetes Ingress/Service) используют этот сигнал для автоматического вывода инстанса прокси из пула активной маршрутизации.

### 6.2. Фоновый опрос доступности (Background Polling)
* При включении `routing.backend-state-polling.enabled=true` отдельный планировщик периодически проверяет доступность каждого бэкенда легковесным вызовом `getStatus` с настраиваемым таймаутом `probe-timeout-ms`.
* Это позволяет обнаружить аварию или восстановление метастора заранее, не дожидаясь клиентских запросов и не задерживая внешние проверки `/readyz`.

### 6.3. Метрики Prometheus (`/metrics`)
При сбоях бэкендов обновляются следующие ключевые метрики:
* `hms_proxy_backend_failures_total{backend="<name>", error="<class>"}` — счётчик сбоев бэкенда по типам ошибок.
* `hms_proxy_backend_status{backend="<name>", state="connected|degraded"}` — текущий статус подключения.
* `hms_proxy_circuit_state{backend="<name>"}` — текущее состояние Circuit Breaker (`0 = CLOSED`, `1 = OPEN`, `2 = HALF_OPEN`).
* `hms_proxy_backend_session_acquire_timeouts_total{backend="<name>", reason="borrow|reconnect"}` — таймауты ожидания сессии из-за перегрузки или недоступности бэкенда.
* `hms_proxy_impersonation_session_evictions_total{backend="<name>", reason="transport_failure"}` — сбросы сессий пользователей из-за сетевых разрывов.
