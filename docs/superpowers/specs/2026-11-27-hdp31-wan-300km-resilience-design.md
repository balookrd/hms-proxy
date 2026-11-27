# Оптимизация и отказоустойчивость HMS Proxy для удалённого кластера `hdp31` (300 км / WAN)

## 1. Контекст и цель

Кластер `hdp` (`HORTONWORKS_3_1_0_3_1_0_78`) переносится на удалённую площадку на расстоянии ~300 км от основного кластера `janus` (`APACHE_3_1_3`), где расположены инстансы `hms-proxy` (`janus-m02`, `janus-m03`) и расчётные YARN/Spark/Hive/Trino-клиенты.
- Профиль нагрузки на `hdp`: **в основном чтение метаданных** (БД, таблицы, партиции, статистика), редкие записи; **все обращения идут через `hms-proxy`**.
- Требование при сбоях связи или просадках WAN-канала: **максимальная живучесть** (`serve-stale-on-error` + `SAFE_FANOUT_READS` + быстрый fail-fast и bulkhead-изоляция при промахах кэша, чтобы проблемы на 300-км канале никогда не блокировали расчёты локального `janus`).

## 2. Выявленные пробелы в кодовой базе

1. **`*_req`-вызовы чтения таблиц и партиций обходят `TableMetadataCache` и `PartitionMetadataCache`**:
   - При `compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78` клиенты HDP 3.1 / Hive 3 / Spark обращаются к метастору через `get_table_req`, `get_partition_req`, `get_partitions_req`, `get_partitions_by_names_req`, `get_partitions_by_filter_req`.
   - Сейчас `TableMetadataCache` и `PartitionMetadataCache` подключены только к позиционным RPC в `RoutingHandler` (`routeByDbStringArgument`, `routeByDbFirstStringArguments`), тогда как `GetTableReqHandler`, `GetPartitionReqHandler`, `GetPartitionsReqHandler`, `GetPartitionsByNamesReqHandler` и `GetPartitionsByFilterReqHandler` ходят напрямую в бэкенд мимо кэша.
2. **Опасность кеширования `get_table_req` с транзакционным `validWriteIdList`**:
   - Если запрос `get_table_req` или `get_partitions_by_names_req` содержит непустой `validWriteIdList` (ACID-чтение), кэшировать ответ под общим ключом таблицы без учёта `validWriteIdList` нельзя. Для таблиц: если `validWriteIdList` пуст (`null` или `isBlank()`), используем общий `TableMetadataCache`; если `validWriteIdList` задан — обходим `TableMetadataCache`. Для партиций: включаем все значимые поля запроса (`validWriteIdList`, `get_col_stats`/`getColStats`, `processorCapabilities`, `partVals`/`names`/`filter`/`maxParts`) в `querySignature` для `PartitionMetadataCache`.
3. **Холодный старт и фоновое обновление таблицы маршрутизации `merge-with-default` (`CatalogRouter`)**:
   - После старта `hms-proxy` (до первого `SHOW DATABASES`) таблица баз `mergedCatalogDatabases` в `CatalogRouter` пуста, из-за чего первые обращения к БД `hdp` вызывают синхронные on-demand пробы `get_database` через WAN.
   - Когда `DatabaseCacheRefresher` в фоне обновляет `get_all_databases` в `DatabaseListCache`, обновлённый список баз не передаётся в `CatalogRouter`.

## 3. Архитектура изменений в коде

### 3.1. Кэширование в `GetTableReqHandler`
- В `GetTableReqHandler.handle`:
  - Проверяем, содержит ли запрос непустой `validWriteIdList` (`ThriftReflectionCache.readString(request, "getValidWriteIdList")`) или непустой `capabilities`? Примечание: `processorCapabilities` в Hive 3 влияет на трансформацию таблицы только если бэкенд сам выполняет `HMSHandler.get_table_req`; однако для HDP 3.1 и Apache 3.1 без `validWriteIdList` возвращаемый `Table` внутри `GetTableResult` идентичен. Чтобы исключить любые расхождения и при этом переиспользовать общий `TableMetadataCache` между `get_table` и `get_table_req`:
    - Если `validWriteIdList == null || validWriteIdList.isBlank()` и `!backend.runtimeProfile().isHive4()`:
      загружаем `Table` через `support.tableMetadataCache.get(namespace.catalogName(), namespace.backendDbName(), tblName, impersonation, ...)` и оборачиваем полученную копию `Table` в `new GetTableResult(table)`.
    - Если `validWriteIdList` задан (или бэкенд `Hive4` с нестандартным классом запроса): выполняем прямой вызов бэкенда без `TableMetadataCache`.

### 3.2. Кэширование в `GetPartitionReqHandler`, `GetPartitionsReqHandler`, `GetPartitionsByNamesReqHandler`, `GetPartitionsByFilterReqHandler`
- В `RoutingHandler` уже есть `encodeQuerySignature(String methodName, Object[] args, int fromIndex)` (переносим его как package-private метод `RoutingSupport.encodeQuerySignature(String methodName, Object... params)` для переиспользования).
- В каждом из четырёх хендлеров (`GetPartitionReqHandler`, `GetPartitionsReqHandler`, `GetPartitionsByNamesReqHandler`, `GetPartitionsByFilterReqHandler`):
  - Разрешаем `ImpersonationContext impersonation = support.impersonationResolver.resolve().orElse(null);`.
  - Строим детерминированную сигнатуру запроса `querySignature` по всем значимым полям `routedRequest`:
    - `GetPartitionReqHandler`: `partVals`;
    - `GetPartitionsReqHandler`: `maxParts`;
    - `GetPartitionsByNamesReqHandler`: `names`, `get_col_stats`, `processorCapabilities`, `environmentContext`, `validWriteIdList`;
    - `GetPartitionsByFilterReqHandler`: `filter`, `maxParts`;
  - Вызываем `support.partitionMetadataCache.get(namespace.catalogName(), namespace.backendDbName(), resolved.tableName(), querySignature, impersonation, () -> ...)` вокруг вызова бэкенда (до `externalizeResult`).
  - В `PartitionMetadataCache.deepCopy`: сейчас поддерживаются `TBase<?, ?>` и `List<?>`. Поскольку в изолированных загрузчиках (Hive 4 / HDP мост) ответ может быть объектом из дочернего classloader'а (не `instanceof` родительского `TBase`, либо `TBase` из родительского loader'а), расширяем `PartitionMetadataCache.deepCopy` через `ThriftReflectionCache.deepCopy(value)` для любых объектов с методом `deepCopy()`.

### 3.3. Автопрогрев и фоновая синхронизация `CatalogRouter` (`merge-with-default`)
- В `GetAllDatabasesHandler`:
  - Внутри лямбды загрузчика `support.databaseListCache.get("get_all_databases", backend.name(), null, impersonation, (effectiveImpersonation, background) -> ...)`:
    когда `background == true` (вызов из `DatabaseCacheRefresher`) и `effectiveImpersonation == null` (shared cache) и `support.router.hasMergeWithDefaultCatalogs()`:
    после успешной загрузки списка баз `loaded` для `backend.name()` сразу обновляем `CatalogRouter`:
    - для `defaultCatalog`: `support.router.updateDatabases(backend.name(), loaded)`;
    - для `isMergeWithDefaultCatalog(backend.name())`: фильтруем через `filterExposedDatabases(support, backend.name(), loaded)` и вызываем `support.router.updateDatabases(backend.name(), exposed)`.
- В `RoutingHandler` (или `DatabaseCacheRefresher`):
  - Добавляем метод `warmUpMergedRoutingTableBestEffort()`: если `router.hasMergeWithDefaultCatalogs()`, при старте `RoutingPipelineFactory.assemble` (или в фоновом потоке сразу после старта) пытается один раз загрузить `get_all_databases` через `GetAllDatabasesHandler` без имперсонации, перехватывая и логируя любые ошибки на `WARN` (чтобы недоступный `LENIENT` каталог `hdp` не блокировал старт).

## 4. Боевая конфигурация `hms-proxy.properties` для 300-км линка до `hdp`

Полный перечень рекомендуемых настроек для добавления в боевой `hms-proxy.properties`:
- `catalog.hdp.startup-mode=LENIENT`, `catalog.titanium.startup-mode=LENIENT`
- `catalog.hdp.required-for-readiness=false`, `catalog.titanium.required-for-readiness=false`, `management.readyz.require-all-catalogs=false`
- `catalog.hdp.max-concurrent-calls=64`, `catalog.hdp.concurrency-timeout-ms=10000`, `catalog.hdp.shared-session-pool-size=16`
- `catalog.hdp.conf.hive.metastore.connect.retries=3`, `catalog.hdp.conf.hive.metastore.failure.retries=2`, `catalog.hdp.conf.hive.metastore.client.connect.retry.delay=1s`, `catalog.hdp.conf.hive.metastore.client.socket.timeout=120s`
- `routing.hedged-read.enabled=true`, `routing.hedged-read.max-parallelism=3`, `routing.hedged-read.fanout-timeout-ms=15000`, `routing.degraded-routing-policy=SAFE_FANOUT_READS`
- `routing.backend-state-polling.enabled=true`, `routing.backend-state-polling.interval-ms=10000`
- `routing.circuit-breaker.enabled=true`, `routing.circuit-breaker.failure-threshold=5`, `routing.circuit-breaker.open-state-ms=15000`
- `routing.cache.table-metadata.enabled=true`, `routing.cache.table-metadata.ttl-ms=60000`, `routing.cache.table-metadata.max-entries=20000`, `routing.cache.table-metadata.shared-across-users=true`
- `routing.cache.partition-metadata.enabled=true`, `routing.cache.partition-metadata.ttl-ms=60000`, `routing.cache.partition-metadata.max-entries=50000`, `routing.cache.partition-metadata.shared-across-users=true`
- `routing.cache.serve-stale-on-error=true`, `routing.cache.stale-grace-period-ms=1800000`
- `routing.cache.distributed-invalidation.mode=ZOOKEEPER`, `routing.cache.distributed-invalidation.zookeeper.connect-string=janus-m01.snp.vimpelcom.ru:2181,janus-m02.snp.vimpelcom.ru:2181,janus-m03.snp.vimpelcom.ru:2181`, `routing.cache.distributed-invalidation.zookeeper.znode=/hive/hms-proxy-cache-invalidation`
- `security.group-disk-cache.enabled=true`, `security.group-disk-cache.path=/var/cache/hms-proxy/user-groups.json`
