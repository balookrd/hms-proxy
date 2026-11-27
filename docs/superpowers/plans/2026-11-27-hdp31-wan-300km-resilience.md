# План реализации: Отказоустойчивость и кэширование HMS Proxy для удалённого кластера `hdp31` (300 км / WAN)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Подключить все `*_req`-методы чтения таблиц и партиций (`get_table_req`, `get_partition_req`, `get_partitions_req`, `get_partitions_by_names_req`, `get_partitions_by_filter_req`) к `TableMetadataCache` и `PartitionMetadataCache` (включая `serve-stale-on-error` и поддержку изолированных classloader'ов), а также синхронизировать таблицу маршрутизации `CatalogRouter` (`merge-with-default`) при фоновом обновлении `DatabaseCacheRefresher`.

**Architecture:** Выносим кодирование сигнатур запросов `encodeQuerySignature` в `RoutingSupport`, расширяем `PartitionMetadataCache.deepCopy` через `ThriftReflectionCache.deepCopy` для поддержки структур из изолированных загрузчиков, оборачиваем чтение в `GetTableReqHandler` и четырёх `GetPartition*ReqHandler` в `TableMetadataCache` / `PartitionMetadataCache` (с обходом кэша таблиц при наличии транзакционного `validWriteIdList`), и обновляем `CatalogRouter` при фоновом рефреше `get_all_databases` в `GetAllDatabasesHandler`.

**Tech Stack:** Java 17 (`JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1`), Maven (`mvn -o`), JUnit 4, Apache Hive Metastore Thrift API 3.1.

## Global Constraints

- Все сборки и тесты запускать строго на JDK 17: `JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1`.
- Никаких новых внешних зависимостей в `pom.xml`.
- Использовать существующий стиль кода: отступ 2 пробела, explicit imports, JUnit 4 (`org.junit.Test`, `org.junit.Assert`).
- После выполнения всех задач обязательно проверить изменения на Kerberos smoke-стенде (`smoke-stand/`), обновить `CHANGELOG.md` и `CHANGELOG.en.md`, а после финального `git push` очистить `target/` и пересобрать `./scripts/build-java-ast-index.sh`.

---

### Task 1: Поддержка `deepCopy` для изолированных загрузчиков в `PartitionMetadataCache` и общий `RoutingSupport.encodeQuerySignature`

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/PartitionMetadataCache.java:248-263`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/RoutingHandler.java:490-592`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/RoutingSupport.java:730-735`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/routing/TableMetadataCacheTest.java`

**Interfaces:**
- Produces:
  - `RoutingSupport.encodeQuerySignature(String methodName, Object[] args, int fromIndex)` (package-private `static String`)
  - `PartitionMetadataCache.deepCopy` (клонирует как `TBase`, так и структуры из изолированного classloader через `ThriftReflectionCache.deepCopy`)

- [ ] **Step 1: Написать тест на `PartitionMetadataCache` с объектом, не реализующим родительский `TBase`, но имеющим метод `deepCopy()`**

В `src/test/java/io/github/mmalykhin/hmsproxy/routing/TableMetadataCacheTest.java` добавить тест `partitionCacheDeepCopiesReflectiveThriftObjectsAndLists`:

```java
  public static final class ForeignPartitionResponse {
    private String state;

    public ForeignPartitionResponse(String state) {
      this.state = state;
    }

    public ForeignPartitionResponse deepCopy() {
      return new ForeignPartitionResponse(this.state);
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }
  }

  @Test
  public void partitionCacheDeepCopiesReflectiveThriftObjectsAndLists() throws Throwable {
    PartitionMetadataCache cache = new PartitionMetadataCache(
        new io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig(60_000L, 100, true));
    ImpersonationContext user = new ImpersonationContext("alice", List.of());
    String sig = RoutingSupport.encodeQuerySignature("get_partitions_req", new Object[] {(short) -1}, 0);

    ForeignPartitionResponse first = cache.get("hdp", "beemetrics", "events", sig, user,
        () -> new ForeignPartitionResponse("original"));
    first.setState("mutated");

    ForeignPartitionResponse second = cache.get("hdp", "beemetrics", "events", sig, user,
        () -> new ForeignPartitionResponse("reloaded"));
    Assert.assertEquals("original", second.getState());
  }
```

- [ ] **Step 2: Запустить тест и убедиться, что он не компилируется / падает**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=TableMetadataCacheTest test
```
Expected: ошибка компиляции (`RoutingSupport.encodeQuerySignature` отсутствует).

- [ ] **Step 3: Перенести `encodeQuerySignature` в `RoutingSupport` и расширить `PartitionMetadataCache.deepCopy`**

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/PartitionMetadataCache.java` заменить метод `deepCopy`:

```java
  private static Object deepCopy(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof TBase<?, ?> tBase) {
      return tBase.deepCopy();
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>(list.size());
      for (Object item : list) {
        copy.add(deepCopy(item));
      }
      return copy;
    }
    return ThriftReflectionCache.deepCopy(value);
  }
```

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/RoutingSupport.java` добавить статические методы `encodeQuerySignature` и `appendSignatureValue` (перенеся их из `RoutingHandler.java`, где заменить вызов на `RoutingSupport.encodeQuerySignature(methodName, routedArgs, 2)`):

```java
  static String encodeQuerySignature(String methodName, Object[] args, int fromIndex) {
    StringBuilder out = new StringBuilder(64);
    out.append(methodName.length()).append(':').append(methodName).append(':');
    int count = args == null ? 0 : Math.max(0, args.length - fromIndex);
    out.append(count).append('[');
    if (args != null) {
      for (int i = fromIndex; i < args.length; i++) {
        if (i > fromIndex) {
          out.append(',');
        }
        appendSignatureValue(out, args[i]);
      }
    }
    return out.append(']').toString();
  }

  private static void appendSignatureValue(StringBuilder out, Object value) {
    if (value == null) {
      out.append("N;");
      return;
    }
    if (value instanceof String str) {
      out.append("S:").append(str.length()).append(':').append(str).append(';');
      return;
    }
    if (value instanceof Boolean bool) {
      out.append(bool ? "Z:1;" : "Z:0;");
      return;
    }
    if (value instanceof Number num) {
      out.append("Num:").append(num.getClass().getSimpleName()).append(':').append(num).append(';');
      return;
    }
    if (value instanceof byte[] bytes) {
      String encoded = java.util.Base64.getEncoder().encodeToString(bytes);
      out.append("B:").append(encoded.length()).append(':').append(encoded).append(';');
      return;
    }
    if (value instanceof Object[] array) {
      out.append("A:").append(array.length).append('[');
      for (int i = 0; i < array.length; i++) {
        if (i > 0) {
          out.append(',');
        }
        appendSignatureValue(out, array[i]);
      }
      out.append("];");
      return;
    }
    if (value instanceof Iterable<?> iterable) {
      String kind = value.getClass().getName();
      out.append("C:").append(kind.length()).append(':').append(kind).append('[');
      boolean first = true;
      for (Object item : iterable) {
        if (!first) {
          out.append(',');
        }
        first = false;
        appendSignatureValue(out, item);
      }
      out.append("];");
      return;
    }
    if (value instanceof Map<?, ?> map) {
      out.append("M:").append(map.size()).append('{');
      boolean first = true;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (!first) {
          out.append(',');
        }
        first = false;
        appendSignatureValue(out, entry.getKey());
        out.append('=');
        appendSignatureValue(out, entry.getValue());
      }
      out.append("};");
      return;
    }
    String typeName = value.getClass().getName();
    String text = String.valueOf(value);
    out.append("O:").append(typeName.length()).append(':').append(typeName)
        .append(':').append(text.length()).append(':').append(text).append(';');
  }
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=TableMetadataCacheTest test
```
Expected: `BUILD SUCCESS`.

---

### Task 2: Подключение `GetTableReqHandler` и `GetPartition*ReqHandler` к `TableMetadataCache` и `PartitionMetadataCache`

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetTableReqHandler.java:15-58`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetPartitionReqHandler.java:18-55`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetPartitionsReqHandler.java:18-50`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetPartitionsByNamesReqHandler.java:18-57`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetPartitionsByFilterReqHandler.java:18-46`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/routing/TableMetadataCacheTest.java`

**Interfaces:**
- Consumes: `RoutingSupport.encodeQuerySignature`, `RoutingSupport.tableMetadataCache`, `RoutingSupport.partitionMetadataCache`
- Produces: кэшируемые `get_table_req`, `get_partition_req`, `get_partitions_req`, `get_partitions_by_names_req`, `get_partitions_by_filter_req` с поддержкой `serve-stale-on-error` и инвалидацией при `alter_table` / `drop_table` / `add_partitions_req` / `alter_partitions_req` / `truncate_table_req`.

- [ ] **Step 1: Написать интеграционный unit-тест на кэширование и `serve-stale-on-error` для `get_table_req` и `get_partitions_by_names_req`**

В `src/test/java/io/github/mmalykhin/hmsproxy/routing/TableMetadataCacheTest.java` добавить тест `requestWrapperReadsUseTableAndPartitionCachesAndServeStaleOnOutage`:

```java
  @Test
  public void requestWrapperReadsUseTableAndPartitionCachesAndServeStaleOnOutage() throws Throwable {
    io.github.mmalykhin.hmsproxy.config.ProxyConfig config =
        io.github.mmalykhin.hmsproxy.config.ProxyConfig.builder()
            .server(new io.github.mmalykhin.hmsproxy.config.server.ServerConfig("test", "127.0.0.1", 9083, 1, 4))
            .security(new io.github.mmalykhin.hmsproxy.config.security.SecurityConfig(
                io.github.mmalykhin.hmsproxy.config.security.SecurityMode.NONE, null, null, null, null, false, java.util.Map.of()))
            .catalogDbSeparator("__")
            .defaultCatalog("janus")
            .catalogs(java.util.Map.of(
                "janus", new io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig(
                    "janus", "janus", "file:///janus", false,
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE, List.of(),
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL, List.of(), java.util.Map.of(),
                    List.of(), false, null, null, java.util.Map.of("hive.metastore.uris", "thrift://janus")),
                "hdp", new io.github.mmalykhin.hmsproxy.config.catalog.CatalogConfig(
                    "hdp", "hdp", "file:///hdp", false,
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogAccessMode.READ_WRITE, List.of(),
                    io.github.mmalykhin.hmsproxy.config.catalog.CatalogExposureMode.ALLOW_ALL, List.of(), java.util.Map.of(),
                    List.of(), true, null, null, java.util.Map.of("hive.metastore.uris", "thrift://hdp"))))
            .syntheticReadLockStore(
                io.github.mmalykhin.hmsproxy.config.syntheticlock.SyntheticReadLockStoreConfig.inMemory())
            .latencyRouting(new io.github.mmalykhin.hmsproxy.config.routing.LatencyRoutingConfig(
                null, null, null, null,
                io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS,
                null, null, null, false,
                new TableMetadataCacheConfig(60_000L, 1000, true),
                new io.github.mmalykhin.hmsproxy.config.routing.PartitionMetadataCacheConfig(60_000L, 1000, true),
                true, 300_000L,
                io.github.mmalykhin.hmsproxy.config.routing.DistributedCacheInvalidationConfig.disabled(),
                io.github.mmalykhin.hmsproxy.config.routing.MergedDatabaseResolutionConfig.defaultConfig()))
            .build();

    AtomicInteger hdpTableCalls = new AtomicInteger();
    AtomicInteger hdpPartCalls = new AtomicInteger();
    io.github.mmalykhin.hmsproxy.backend.CatalogBackend janus =
        RoutingMetaStoreProxyTestSupport.newBackend(
            config, config.catalogs().get("janus"),
            new io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter(),
            RoutingMetaStoreProxyTestSupport.newBackendRuntime(
                config, config.catalogs().get("janus"),
                RoutingMetaStoreProxyTestSupport.newSession((proxy, method, args) -> {
                  throw new org.apache.hadoop.hive.metastore.api.NoSuchObjectException("not in janus");
                })));
    io.github.mmalykhin.hmsproxy.backend.CatalogBackend hdp =
        RoutingMetaStoreProxyTestSupport.newBackend(
            config, config.catalogs().get("hdp"),
            new io.github.mmalykhin.hmsproxy.backend.ApacheBackendAdapter(),
            RoutingMetaStoreProxyTestSupport.newBackendRuntime(
                config, config.catalogs().get("hdp"),
                RoutingMetaStoreProxyTestSupport.newSession((proxy, method, args) -> {
                  if ("get_database".equals(method.getName())) {
                    org.apache.hadoop.hive.metastore.api.Database db =
                        new org.apache.hadoop.hive.metastore.api.Database();
                    db.setName((String) args[0]);
                    return db;
                  }
                  if ("get_table_req".equals(method.getName())) {
                    hdpTableCalls.incrementAndGet();
                    Table t = new Table();
                    t.setDbName("Beemetrics");
                    t.setTableName("events");
                    return new org.apache.hadoop.hive.metastore.api.GetTableResult(t);
                  }
                  if ("get_partitions_by_names_req".equals(method.getName())) {
                    hdpPartCalls.incrementAndGet();
                    org.apache.hadoop.hive.metastore.api.Partition p =
                        new org.apache.hadoop.hive.metastore.api.Partition();
                    p.setDbName("Beemetrics");
                    p.setTableName("events");
                    p.setValues(List.of("2026-11-27"));
                    org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult res =
                        new org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult();
                    res.setPartitions(List.of(p));
                    return res;
                  }
                  throw new UnsupportedOperationException(method.getName());
                })));

    java.util.LinkedHashMap<String, io.github.mmalykhin.hmsproxy.backend.CatalogBackend> backends =
        new java.util.LinkedHashMap<>();
    backends.put("janus", janus);
    backends.put("hdp", hdp);
    CatalogRouter router = new CatalogRouter(config, backends);
    router.updateDatabases("janus", List.of("default"));
    router.updateDatabases("hdp", List.of("Beemetrics"));
    io.github.mmalykhin.hmsproxy.federation.FederationLayer federation =
        new io.github.mmalykhin.hmsproxy.federation.FederationLayer(config, router);
    RoutingMetaStoreProxy proxy = new RoutingMetaStoreProxy(
        config, router, federation, null, new io.github.mmalykhin.hmsproxy.observability.ProxyObservability(config));

    org.apache.hadoop.hive.metastore.api.GetTableRequest tblReq =
        new org.apache.hadoop.hive.metastore.api.GetTableRequest("Beemetrics", "events");
    org.apache.hadoop.hive.metastore.api.GetTableResult r1 =
        (org.apache.hadoop.hive.metastore.api.GetTableResult) proxy.get_table_req(tblReq);
    org.apache.hadoop.hive.metastore.api.GetTableResult r2 =
        (org.apache.hadoop.hive.metastore.api.GetTableResult) proxy.get_table_req(tblReq);
    Assert.assertEquals("events", r1.getTable().getTableName());
    Assert.assertEquals("events", r2.getTable().getTableName());
    Assert.assertEquals("get_table_req must hit TableMetadataCache on second call", 1, hdpTableCalls.get());

    org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest partReq =
        new org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest("Beemetrics", "events");
    partReq.setNames(List.of("dt=2026-11-27"));
    org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult p1 =
        (org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult) proxy.get_partitions_by_names_req(partReq);
    org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult p2 =
        (org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult) proxy.get_partitions_by_names_req(partReq);
    Assert.assertEquals(1, p1.getPartitions().size());
    Assert.assertEquals(1, p2.getPartitions().size());
    Assert.assertEquals("get_partitions_by_names_req must hit PartitionMetadataCache on second call",
        1, hdpPartCalls.get());
  }
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=TableMetadataCacheTest#requestWrapperReadsUseTableAndPartitionCachesAndServeStaleOnOutage test
```
Expected: FAIL (`expected:<1> but was:<2>`).

- [ ] **Step 3: Подключить `TableMetadataCache` в `GetTableReqHandler` и `PartitionMetadataCache` в `GetPartition*ReqHandler`**

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetTableReqHandler.java`:
- Проверить `String validWriteIdList = ThriftReflectionCache.readString(request, "getValidWriteIdList");`
- Если `!backend.runtimeProfile().isHive4()` и `(validWriteIdList == null || validWriteIdList.isBlank())`:
  получить `ImpersonationContext impersonation = support.impersonationResolver.resolve().orElse(null);`
  вызвать `Table cachedTable = support.tableMetadataCache.get(namespace.catalogName(), namespace.backendDbName(), tblName, impersonation, () -> { ... });`
  Внутри loader'а вызывать `support.invokeViaRequest(backend, routedRequest, method.getName())` и извлекать `Table` (`((GetTableResult) res).getTable()`), а снаружи оборачивать `new GetTableResult(cachedTable)` (и если ` !(request instanceof GetTableRequest)`, конвертировать `GetTableResult` в загрузчик `request.getClass().getClassLoader()`, если метод вызывался из изолированного моста; заметьте, что сейчас для `!backend.runtimeProfile().isHive4()` `GetTableReqHandler` всегда возвращает стандартный `GetTableResult`, который `HortonworksFrontendBridge` сам конвертирует).

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetPartitionReqHandler.java`, `GetPartitionsReqHandler.java`, `GetPartitionsByNamesReqHandler.java`, `GetPartitionsByFilterReqHandler.java`:
- Вычислять `ImpersonationContext impersonation = support.impersonationResolver.resolve().orElse(null);`
- Строить `String querySig = RoutingSupport.encodeQuerySignature(method.getName(), new Object[] { ... }, 0);`:
  - В `GetPartitionReqHandler`: `new Object[] { ThriftReflectionCache.invokeGetter(routedRequest, "getPartVals"), ThriftReflectionCache.readString(routedRequest, "getValidWriteIdList") }`
  - В `GetPartitionsReqHandler`: `new Object[] { ThriftReflectionCache.invokeGetter(routedRequest, "getMaxParts"), ThriftReflectionCache.invokeGetter(routedRequest, "isWithAuth"), ThriftReflectionCache.readString(routedRequest, "getUserName"), ThriftReflectionCache.invokeGetter(routedRequest, "getGroupNames") }`
  - В `GetPartitionsByNamesReqHandler`: `new Object[] { ThriftReflectionCache.invokeGetter(routedRequest, "getNames"), ThriftReflectionCache.invokeGetter(routedRequest, "isGet_col_stats"), ThriftReflectionCache.invokeGetter(routedRequest, "getProcessorCapabilities"), ThriftReflectionCache.readString(routedRequest, "getValidWriteIdList") }`
  - В `GetPartitionsByFilterReqHandler`: `new Object[] { ThriftReflectionCache.invokeGetter(routedRequest, "getFilter"), ThriftReflectionCache.invokeGetter(routedRequest, "getMaxParts"), ThriftReflectionCache.invokeGetter(routedRequest, "isWithAuth"), ThriftReflectionCache.readString(routedRequest, "getUserName"), ThriftReflectionCache.invokeGetter(routedRequest, "getGroupNames") }`
- Оборачивать получение `result` в `support.partitionMetadataCache.get(namespace.catalogName(), namespace.backendDbName(), resolved.tableName(), querySig, impersonation, () -> ...)`.

- [ ] **Step 4: Запустить тесты и убедиться, что они проходят**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=TableMetadataCacheTest,FrontendBridgeThriftSerializationTest test
```
Expected: `BUILD SUCCESS`.

---

### Task 3: Автосинхронизация `CatalogRouter` (`merge-with-default`) при фоновом обновлении `DatabaseCacheRefresher`

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetAllDatabasesHandler.java:18-36`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/routing/MergedDatabasesRoutingTest.java`

**Interfaces:**
- Consumes: `CatalogRouter.updateDatabases`, `GetAllDatabasesHandler.filterExposedDatabases`
- Produces: автоматическое обновление таблицы маршрутизации `CatalogRouter` при фоновом тике `DatabaseCacheRefresher.refreshTick()`

- [ ] **Step 1: Написать падающий тест в `MergedDatabasesRoutingTest`**

В `src/test/java/io/github/mmalykhin/hmsproxy/routing/MergedDatabasesRoutingTest.java` добавить тест `backgroundDatabaseListRefreshUpdatesMergedCatalogRoutingTable`:

```java
  @Test
  public void backgroundDatabaseListRefreshUpdatesMergedCatalogRoutingTable() throws Throwable {
    AtomicLong clock = new AtomicLong(1_000_000L);
    io.github.mmalykhin.hmsproxy.config.routing.DatabaseCacheBackgroundRefreshConfig refreshCfg =
        new io.github.mmalykhin.hmsproxy.config.routing.DatabaseCacheBackgroundRefreshConfig(true, 10_000L, 3_600_000L);
    ProxyConfig base = createConfig(true);
    ProxyConfig config = ProxyConfig.builder()
        .server(base.server())
        .security(base.security())
        .catalogDbSeparator(base.catalogDbSeparator())
        .defaultCatalog(base.defaultCatalog())
        .catalogs(base.catalogs())
        .syntheticReadLockStore(base.syntheticReadLockStore())
        .latencyRouting(new LatencyRoutingConfig(
            null, null, null, null,
            io.github.mmalykhin.hmsproxy.config.routing.DegradedRoutingPolicy.SAFE_FANOUT_READS,
            new io.github.mmalykhin.hmsproxy.config.routing.DatabaseListCacheConfig(60_000L, 100, true, refreshCfg),
            new io.github.mmalykhin.hmsproxy.config.routing.DatabaseMetadataCacheConfig(60_000L, 100, true, refreshCfg),
            null, false, null, null, false, 0L,
            io.github.mmalykhin.hmsproxy.config.routing.DistributedCacheInvalidationConfig.disabled(),
            MergedDatabaseResolutionConfig.defaultConfig()))
        .build();

    AtomicReference<List<String>> cat2Dbs = new AtomicReference<>(List.of("remote_v1"));
    AtomicInteger cat2GetDbCalls = new AtomicInteger();
    CatalogBackend backend1 = newBackend(config, config.catalogs().get("catalog1"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog1"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            return List.of("default");
          }
          if ("get_database".equals(method.getName())) {
            throw new NoSuchObjectException((String) args[0]);
          }
          throw new UnsupportedOperationException(method.getName());
        })));
    CatalogBackend backend2 = newBackend(config, config.catalogs().get("catalog2"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("catalog2"), newSession((proxy, method, args) -> {
          if ("get_all_databases".equals(method.getName())) {
            return cat2Dbs.get();
          }
          if ("get_database".equals(method.getName())) {
            cat2GetDbCalls.incrementAndGet();
            Database db = new Database();
            db.setName((String) args[0]);
            return db;
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("catalog1", backend1);
    backends.put("catalog2", backend2);
    CatalogRouter router = new CatalogRouter(config, backends);
    FederationLayer federation = new FederationLayer(config, router);
    ProxyObservability observability = new ProxyObservability(config);
    DatabaseListCache listCache = new DatabaseListCache(
        config.latencyRouting().databaseListCache(), false, 0L, observability.metrics(), clock::get);
    DatabaseMetadataCache metaCache = new DatabaseMetadataCache(
        config.latencyRouting().databaseMetadataCache(), listCache, false, 0L, observability.metrics(), clock::get);
    DatabaseCacheRefresher refresher = new DatabaseCacheRefresher(
        listCache, metaCache, refreshCfg, refreshCfg, clock::get, false);
    io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer compat =
        new io.github.mmalykhin.hmsproxy.compatibility.CompatibilityLayer(config, null);
    BackendRoutingController brc = new BackendRoutingController(config, router, observability);
    AdmissionGate gate = new AdmissionGate(brc, new RequestRateLimiter(config, observability.metrics()));
    FanoutExecutor fanout = new FanoutExecutor(brc, router, gate);
    BackendCallDispatcher dispatcher = new BackendCallDispatcher(compat, gate, observability, fanout, router);
    RoutingHandler handler = new RoutingHandler(
        config, router, federation, compat, observability, dispatcher, new ImpersonationResolver(config),
        listCache, metaCache, new TableMetadataCache(config.latencyRouting().tableMetadataCache()),
        new PartitionMetadataCache(config.latencyRouting().partitionMetadataCache()),
        new FileSystemExternalTableDropPurger(config),
        io.github.mmalykhin.hmsproxy.security.ranger.NoOpMetadataAuthorizer.INSTANCE,
        new LocalCacheInvalidator(listCache, metaCache, null, null, router));

    Method getAllDbs = ThriftHiveMetastore.Iface.class.getMethod("get_all_databases");
    handler.invoke(null, getAllDbs, new Object[0]);

    // New database remote_v2 appears in catalog2 and is picked up by background refresh
    cat2Dbs.set(List.of("remote_v1", "remote_v2"));
    clock.addAndGet(15_000L);
    refresher.refreshTick();

    // Resolving remote_v2 must immediately route to catalog2 without an on-demand get_database probe!
    CatalogRouter.ResolvedNamespace ns = router.resolveDatabase("remote_v2");
    Assert.assertEquals("catalog2", ns.catalogName());
    Assert.assertEquals("Background refresh should have registered remote_v2 without on-demand probe",
        0, cat2GetDbCalls.get());
    brc.close();
    handler.close();
  }
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=MergedDatabasesRoutingTest#backgroundDatabaseListRefreshUpdatesMergedCatalogRoutingTable test
```
Expected: FAIL (`expected:<0> but was:<1>`).

- [ ] **Step 3: Обновлять `CatalogRouter` при фоновом рефреше в `GetAllDatabasesHandler`**

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetAllDatabasesHandler.java` внутри loader-лямбды `support.databaseListCache.get`:

```java
              (effectiveImpersonation, background) -> {
                List<String> loaded = (List<String>) support.dispatcher.invokeDirect(
                    backend, method, null, effectiveImpersonation, background ? -1L : requestId, false, false);
                if (background && effectiveImpersonation == null && loaded != null
                    && support.router.hasMergeWithDefaultCatalogs()) {
                  String catalogName = backend.name();
                  if (catalogName.equals(support.router.defaultBackend().name())) {
                    support.router.updateDatabases(catalogName, loaded);
                  } else if (support.router.isMergeWithDefaultCatalog(catalogName)) {
                    support.router.updateDatabases(
                        catalogName, filterExposedDatabases(support, catalogName, loaded));
                  }
                }
                return loaded;
              }
```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=MergedDatabasesRoutingTest test
```
Expected: `BUILD SUCCESS`.

---

### Task 4: Совместимость HDP 3.1 фронта с удалёнными `APACHE_3_1_3` каталогами (`GetTablesExtHandler`, `GetMaxAllocatedTableWriteIdHandler`, `BackendCallDispatcher`)

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetTablesExtHandler.java:14-44`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetMaxAllocatedTableWriteIdHandler.java:15-42`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendCallDispatcher.java:209-214`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/routing/RoutingMetaStoreProxyNamespaceRoutingTest.java`

**Interfaces:**
- Produces:
  - `GetTablesExtHandler` fallback через `get_table_meta` для бэкендов `APACHE_3_1_3`
  - `GetMaxAllocatedTableWriteIdHandler` поддержка `READ_ONLY` каталогов (`titanium`)
  - Исключение `ThriftFailureClassifier.isUnsupportedMethod` из счётчика ошибок бэкенда в `BackendCallDispatcher`

- [ ] **Step 1: Написать падающий тест в `RoutingMetaStoreProxyNamespaceRoutingTest`**

В `src/test/java/io/github/mmalykhin/hmsproxy/routing/RoutingMetaStoreProxyNamespaceRoutingTest.java` добавить тест `getTablesExtAndMaxAllocatedWriteIdFallbackOnReadOnlyApacheBackend`:

```java
  @Test
  public void getTablesExtAndMaxAllocatedWriteIdFallbackOnReadOnlyApacheBackend() throws Throwable {
    Assume.assumeTrue(Files.isReadable(HDP_6150_JAR));

    ProxyConfig config = ProxyConfig.builder()
        .server(new ServerConfig("test", "127.0.0.1", 9083, 1, 4))
        .security(new SecurityConfig(SecurityMode.NONE, null, null, null, null, false, Map.of()))
        .catalogDbSeparator("__")
        .defaultCatalog("hdp")
        .catalogs(Map.of(
            "hdp", catalogConfig(
                "hdp", "hdp", MetastoreRuntimeProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(),
                Map.of("hive.metastore.uris", "thrift://hdp")),
            "titanium", new CatalogConfig(
                "titanium", "titanium", "file:///titanium", false, CatalogAccessMode.READ_ONLY, List.of(),
                CatalogExposureMode.ALLOW_ALL, List.of(), Map.of(), List.of(), true,
                MetastoreRuntimeProfile.APACHE_3_1_3, null,
                Map.of("hive.metastore.uris", "thrift://titanium"))))
        .compatibility(new CompatibilityConfig(
            FrontendProfile.HORTONWORKS_3_1_0_3_1_5_6150_1, HDP_6150_JAR.toString(), HDP_6150_JAR.toString(), false))
        .syntheticReadLockStore(SyntheticReadLockStoreConfig.inMemory())
        .build();

    CatalogBackend apacheBackend = newBackend(
        config, config.catalogs().get("titanium"), new ApacheBackendAdapter(),
        newBackendRuntime(config, config.catalogs().get("titanium"), newSession((proxy, method, args) -> {
          if ("get_table_meta".equals(method.getName())) {
            org.apache.hadoop.hive.metastore.api.TableMeta meta =
                new org.apache.hadoop.hive.metastore.api.TableMeta("beemetrics", "events", "MANAGED_TABLE");
            return List.of(meta);
          }
          throw new UnsupportedOperationException(method.getName());
        })));

    LinkedHashMap<String, CatalogBackend> backends = new LinkedHashMap<>();
    backends.put("hdp", null);
    backends.put("titanium", apacheBackend);
    CatalogRouter router = new CatalogRouter(config, backends);
    router.updateDatabases("hdp", List.of("default"));
    router.updateDatabases("titanium", List.of("beemetrics"));
    ProxyObservability observability = new ProxyObservability(config);
    RoutingMetaStoreProxy handler =
        new RoutingMetaStoreProxy(config, router, new FederationLayer(config, router), null, observability);

    ClassLoader classLoader = new MetastoreApiClassLoader(
        new java.net.URL[] {HDP_6150_JAR.toUri().toURL()},
        RoutingMetaStoreProxyTestSupport.class.getClassLoader());
    Class<?> extReqClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.GetTablesExtRequest", true, classLoader);
    Object extReq = extReqClass.getConstructor(String.class, String.class, String.class, int.class)
        .newInstance("hive", "beemetrics", "*", 1);

    List<?> extResponse = (List<?>) handler.getTablesExt(extReq);
    Assert.assertEquals(1, extResponse.size());

    Class<?> maxWriteIdReqClass =
        Class.forName("org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest", true, classLoader);
    Object maxWriteIdReq = maxWriteIdReqClass.getConstructor(String.class, String.class)
        .newInstance("beemetrics", "events");
    Object maxWriteIdResp = handler.get_max_allocated_table_write_id(maxWriteIdReq);
    Assert.assertEquals(0L, maxWriteIdResp.getClass().getMethod("getMaxWriteId").invoke(maxWriteIdResp));
  }
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=RoutingMetaStoreProxyNamespaceRoutingTest#getTablesExtAndMaxAllocatedWriteIdFallbackOnReadOnlyApacheBackend test
```
Expected: FAIL с `MetaException: Hortonworks get_tables_ext requires a Hortonworks backend runtime`.

- [ ] **Step 3: Реализовать fallback в `GetTablesExtHandler`, read-разрешение в `GetMaxAllocatedTableWriteIdHandler` и фильтрацию `isUnsupportedMethod` в `BackendCallDispatcher`**

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetTablesExtHandler.java`:
- Если `!backend.runtimeProfile().isHortonworks()`, вызывать fallback на `get_table_meta(namespace.backendDbName(), pattern, null)`:
  ```java
  private static final Method GET_TABLE_META = RequestObjectRouting.findIfaceMethod(
      "get_table_meta", String.class, String.class, List.class);
  ```
  Получить `List<TableMeta>`, отфильтровать через `support.filterTableNameList("get_tables_ext", namespace, ...)` (или `support.isTableAccessible("get_tables_ext", namespace, meta.getTableName())`), и для каждого видимого `TableMeta` сконструировать `org.apache.hadoop.hive.metastore.api.ExtendedTableInfo` в `request.getClass().getClassLoader()` через `infoClass.getConstructor(String.class).newInstance(meta.getTableName())`.

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/GetMaxAllocatedTableWriteIdHandler.java`:
- Вместо `RequestObjectRouting.resolveWrite` использовать:
  ```java
  String catName = RequestObjectRouting.readCatalogName(request);
  String dbName = RequestObjectRouting.readDbName(request);
  String tblName = RequestObjectRouting.readTableName(request);
  RequestObjectRouting.ResolvedRequest resolved =
      RequestObjectRouting.resolve(support, "get_max_allocated_table_write_id", request, catName, dbName, tblName);
  ```

В `src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendCallDispatcher.java`:
- В `catch (Throwable cause)` метода `performBackendCall`:
  ```java
  if (!io.github.mmalykhin.hmsproxy.thriftbridge.ThriftFailureClassifier.isUnsupportedMethod(cause)) {
    observability.metrics().recordBackendFailure(backend.name(), cause);
    admissionGate.recordFailure(backend, cause, elapsedMs, admission.halfOpen());
  }
  ```

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=RoutingMetaStoreProxyNamespaceRoutingTest#getTablesExtAndMaxAllocatedWriteIdFallbackOnReadOnlyApacheBackend test
```
Expected: `BUILD SUCCESS`.

---

### Task 5: Единая настройка `zookeeper.connect-string` для всех ZooKeeper-компонентов прокси

**Files:**
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/ConfigParsing.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoader.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/security/SecurityConfigParser.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/syntheticlock/SyntheticReadLockStoreConfigParser.java`
- Modify: `src/main/java/io/github/mmalykhin/hmsproxy/config/routing/LatencyRoutingConfigParser.java`
- Test: `src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java`

**Interfaces:**
- Produces:
  - `ConfigParsing.resolveZooKeeperConnectString(PropertyReader reader, String specificKey)`: возвращает `specificKey`, либо `zookeeper.connect-string`, либо любую из сконфигурированных ZK connect-string (`synthetic-read-lock.store.zookeeper.connect-string`, `routing.cache.distributed-invalidation.zookeeper.connect-string`, `security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString`, `security.front-door-conf.metastore.cluster.delegation.token.store.zookeeper.connectString`, `security.front-door-conf.hive.zookeeper.quorum`).

- [ ] **Step 1: Написать падающий тест в `ProxyConfigLoaderTest`**

В `src/test/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoaderTest.java` добавить тест `singleClusterZooKeeperConnectStringPopulatesAllZooKeeperSubsystems`:

```java
  @Test
  public void singleClusterZooKeeperConnectStringPopulatesAllZooKeeperSubsystems() throws Exception {
    Path file = Files.createTempFile("hms-proxy-shared-zk", ".properties");
    try {
      Files.writeString(file, String.join("\n",
          "catalogs=catalog1",
          "catalog.catalog1.conf.hive.metastore.uris=thrift://hms1:9083",
          "zookeeper.connect-string=zk1:2181,zk2:2181,zk3:2181",
          "security.front-door-conf.hive.cluster.delegation.token.store.class="
              + "org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore",
          "synthetic-read-lock.store.mode=ZOOKEEPER",
          "routing.cache.distributed-invalidation.mode=ZOOKEEPER"
      ));
      ProxyConfig config = ProxyConfigLoader.load(file);
      Assert.assertEquals("zk1:2181,zk2:2181,zk3:2181",
          config.syntheticReadLockStore().zooKeeper().connectString());
      Assert.assertEquals("zk1:2181,zk2:2181,zk3:2181",
          config.latencyRouting().distributedCacheInvalidation().zooKeeper().connectString());
      Assert.assertEquals("zk1:2181,zk2:2181,zk3:2181",
          config.security().frontDoorConf().get("hive.cluster.delegation.token.store.zookeeper.connectString"));
    } finally {
      Files.deleteIfExists(file);
    }
  }
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=ProxyConfigLoaderTest#singleClusterZooKeeperConnectStringPopulatesAllZooKeeperSubsystems test
```
Expected: FAIL (`Unrecognized configuration property: 'zookeeper.connect-string'` / `Missing required configuration property: synthetic-read-lock.store.zookeeper.connect-string`).

- [ ] **Step 3: Реализовать `ConfigParsing.resolveZooKeeperConnectString` и подключить во все три парсера**

1. В `src/main/java/io/github/mmalykhin/hmsproxy/config/ConfigParsing.java` добавить метод:
```java
  private static final List<String> SHARED_ZOOKEEPER_CONNECT_STRING_KEYS = List.of(
      "zookeeper.connect-string",
      "synthetic-read-lock.store.zookeeper.connect-string",
      "routing.cache.distributed-invalidation.zookeeper.connect-string",
      "security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString",
      "security.front-door-conf.metastore.cluster.delegation.token.store.zookeeper.connectString",
      "security.front-door-conf.hive.zookeeper.quorum"
  );

  public static String resolveZooKeeperConnectString(PropertyReader reader, String specificKey) {
    if (specificKey != null) {
      String direct = reader.getOrNull(specificKey);
      if (direct != null && !direct.isBlank()) {
        return direct;
      }
    }
    for (String candidateKey : SHARED_ZOOKEEPER_CONNECT_STRING_KEYS) {
      String candidate = reader.getOrNull(candidateKey);
      if (candidate != null && !candidate.isBlank()) {
        return candidate;
      }
    }
    return null;
  }
```
2. В `src/main/java/io/github/mmalykhin/hmsproxy/config/ProxyConfigLoader.java` добавить `"zookeeper.connect-string"` в `STATIC_KNOWN_KEYS`.
3. В `src/main/java/io/github/mmalykhin/hmsproxy/config/syntheticlock/SyntheticReadLockStoreConfigParser.java`:
   заменить `reader.getOrNull("synthetic-read-lock.store.zookeeper.connect-string")` на `ConfigParsing.resolveZooKeeperConnectString(reader, "synthetic-read-lock.store.zookeeper.connect-string")`.
4. В `src/main/java/io/github/mmalykhin/hmsproxy/config/routing/LatencyRoutingConfigParser.java`:
   заменить чтение `connectString` для `ZOOKEEPER` на `ConfigParsing.resolveZooKeeperConnectString(reader, "routing.cache.distributed-invalidation.zookeeper.connect-string")`.
5. В `src/main/java/io/github/mmalykhin/hmsproxy/config/security/SecurityConfigParser.java`:
   если `frontDoorConf` не содержит непустого `hive.cluster.delegation.token.store.zookeeper.connectString`, `metastore.cluster.delegation.token.store.zookeeper.connectString` или `hive.zookeeper.quorum`, а `ConfigParsing.resolveZooKeeperConnectString(reader, null)` вернул непустую строку, положить её в изменяемую копию `frontDoorConf` под ключом `"hive.cluster.delegation.token.store.zookeeper.connectString"`.

- [ ] **Step 4: Запустить тест и убедиться, что он проходит**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -Dtest=ProxyConfigLoaderTest test
```
Expected: `BUILD SUCCESS`.

---

### Task 6: Полная проверка, обновление документации/changelog, Kerberos smoke-стенд и коммит

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `CHANGELOG.en.md`
- Modify: `src/main/resources/hms-proxy-example.properties`

- [ ] **Step 1: Обновить `CHANGELOG.md`, `CHANGELOG.en.md` и комментарии в `hms-proxy-example.properties`**

Добавить запись в `[Unreleased]` в `CHANGELOG.md` и `CHANGELOG.en.md` о подключении `get_table_req`, `get_partition_req`, `get_partitions_req`, `get_partitions_by_names_req`, `get_partitions_by_filter_req` к `TableMetadataCache` и `PartitionMetadataCache` и синхронизации `CatalogRouter` при фоновом обновлении `DatabaseCacheRefresher`.

- [ ] **Step 2: Прогнать полный набор unit-тестов и собрать fat jar на JDK 17**

Run:
```bash
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o test && JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o -DskipTests package
```
Expected: `BUILD SUCCESS`, 0 failures, 0 skipped.

- [ ] **Step 3: Проверить на Kerberos smoke-стенде (`smoke-stand/`)**

Run:
```bash
cd smoke-stand && ./prepare.sh && docker compose --env-file .env.kerberos --profile kerberos up -d --build
docker cp scripts stand-proxy:/opt/hms-proxy/ && docker cp smoke-stand/env stand-proxy:/opt/hms-proxy/smoke-env
docker exec stand-proxy bash -lc 'kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL && /opt/hms-proxy/scripts/run-real-installation-smoke-kerberos.sh --env-file /opt/hms-proxy/smoke-env/kerberos.env --scenario all'
cd smoke-stand && docker compose --env-file .env.kerberos --profile kerberos stop
```
Expected: `[hms-smoke] scenario 'all' completed successfully`.

- [ ] **Step 4: Сделать коммит, пуш, очистить `target/` и обновить AST-индекс**

Run:
```bash
git add -A
git commit -m "feat(routing): cache *_req table and partition reads and sync merged router on background refresh"
git push
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o clean && ./scripts/build-java-ast-index.sh
```
