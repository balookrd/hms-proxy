# AGENTS.md

Руководство для coding agents, работающих в этом репозитории.

## Общение

- Общайся с пользователем на русском языке по умолчанию.
- Переходи на другой язык только по явной просьбе пользователя или когда нужно сохранить уже существующий англоязычный артефакт.
- Код, команды, имена классов, ключи конфигурации, stack traces и пути к файлам оставляй в исходном виде.

## Форма проекта

- Это Maven-проект на Java 17 для `hms-proxy`: catalog-aware proxy для federation и compatibility поверх Hive Metastore.
- Основной код находится в `src/main/java/io/github/mmalykhin/hmsproxy`.
- Тесты находятся в `src/test/java/io/github/mmalykhin/hmsproxy` и используют JUnit 4 (`org.junit.Test`, `Assert`).
- Примеры runtime-конфигурации находятся в `src/main/resources`, особенно `hms-proxy-example.properties`.
- `hive-metastore/` содержит standalone Hive/Hortonworks metastore jar-файлы, которые используются в compatibility/runtime-profile путях, плюс спутник-jar'ы изолированного рантайма `APACHE_4_1_0` (`libthrift-0.16.0.jar`, `libfb303-0.9.3.jar`, `hive-storage-api-4.1.0.jar`) — они обязаны лежать рядом с `hive-standalone-metastore-common-4.1.0.jar`, иначе Hive 4-бэкенд отказывает на старте. Считай все эти jar-файлы намеренными артефактами проекта.
- `capabilities.yaml` управляет генерируемой compatibility-документацией в `README.md` и `README.ru.md`.
- Скрипты smoke-проверок на реальной установке находятся в `scripts/`; Grafana dashboard assets находятся в `monitoring/`.
- `smoke-stand/` - локальный docker-compose стенд для тех же smoke-скриптов: два standalone metastore (Apache и Hortonworks) за прокси, HDFS, HiveServer2 и MIT KDC. Собирается из jar-файлов каталога `hive-metastore/`; тяжёлые входные артефакты не хранятся в git и восстанавливаются через `smoke-stand/prepare.sh`. Iceberg interop-сценарий `smoke-stand/run-iceberg-interop-smoke.sh` гоняет одну таблицу через REST-клиент `smoke-stand/iceberg-rest-writer/` (пишет настоящие данные через REST front door), оба 3.1-HiveServer2 (с `iceberg-hive-runtime` 1.6.1) и HiveServer2 Hive 4 (`smoke-stand/hs2-hive4/`, compose-профиль `hive4fe`, через третий listener `additional-frontends.hive4fe` с профилем `APACHE_4_1_0`). Бэкенд под тестом — это default-каталог (записи гейт пускает только туда), поэтому сценарий повторяется с каждым из трёх метасторов в этой роли: `--prefix hdp` (конфиг по умолчанию), `--prefix apache` (`.env.apache[-kerberos]`, второй HDFS-кластер) и `--prefix hive4` (`.env.hive4[-kerberos]`, метастор Apache Hive 4.1.0 из `smoke-stand/hms-hive4/`). `smoke-stand/run-iceberg-concurrency-smoke.sh` проверяет изоляцию писателей: N конкурентных REST-коммитов в одну таблицу default-каталога, число строк обязано совпасть с числом успешно завершившихся писателей (упавший громко `CommitFailedException` — корректное поведение, потерянная строка — нет). `smoke-stand/run-iceberg-rowlevel-smoke.sh` покрывает row-level DML Iceberg v2, которого нет в interop-сценарии (тот только дописывает строки, поэтому delete-файлов не создаёт): Hive 4 делает `DELETE`/`UPDATE` в v2-таблице, созданной через REST, остальные front door её читают, прогон повторяется для `merge-on-read` и `copy-on-write`. Режим проверяется по фактическому числу delete-файлов (команда `files` REST-writer'а), а чтения — полным сканом `select id, src`, а не `count(*)`: count Hive умеет отдавать из закэшированной Iceberg-статистики, не читая delete-файл. Линия 3.1 (`iceberg-hive-runtime` 1.6.1) merge-on-read **читает** корректно и умеет `INSERT` поверх, но собственный `DELETE`/`UPDATE` у неё отклоняется на компиляции (SemanticException 10297) — граница проходит по записи, а не по чтению. `smoke-stand/run-iceberg-txn-contention-smoke.sh` измеряет, что делает multi-table транзакция под состязанием: конкурирующий писатель продвигает `main` одной из двух таблиц, транзакция приходит с устаревшим snapshot id и отклоняется с `409 CommitFailedException`, не оставляя изменения ни на одной из таблиц; заканчивается прогон позитивным контролем (та же транзакция с актуальным snapshot id обязана быть принята), иначе проверка прошла бы и на некорректном теле. Атомарным маршрут при этом не становится: при сбое самого коммита, а не требования, предыдущие таблицы остаются закоммиченными (`500 CommitStateUnknownException`). SQL-слой стенда гоняется отдельными env-файлами `smoke-stand/env/sql{,-apache}{,-kerberos}.env` изнутри контейнера HiveServer2: в `simple.env`/`kerberos.env` SQL-настройки закомментированы намеренно (те файлы запускают с хоста, где нет beeline). Раскладка парная — каждый front door со своим метастором в роли default-каталога, чужой удалённый: `HMS_SMOKE_SQL_FRONT_DOORS` выбирает проходы, `HMS_SMOKE_TRANSACTIONAL_SQL_FRONT_DOORS` — где пробовать ACID, `HMS_SMOKE_SQL_RUN_TRUNCATE` — где `TRUNCATE`. Кросс-пары (Apache-фронт над Hortonworks-бэкендом и наоборот) вне этой раскладки, их ограничения записаны как C6 и C7. Оба образа метастора собираются на подходящем Hadoop через каталоги `hms/override-{hdp,apache}`, которые идут на classpath **впереди** Maven-набора (в противоположность `acid-lib`): без этого метастор работал на hadoop-hdfs 2.2.0 и `TRUNCATE` падал с `NoSuchMethodError` на `HdfsAdmin.getEncryptionZoneForPath`. Guava из вендорского набора исключена сознательно (HDP несёт 11.0.2, метастору нужна 19), а для Apache-метастора берётся явный список jar-ов, а не копирование `hs2/lib`: там лежит `hive-exec`, uber-jar, который затенил бы проверяемый jar метастора. DDL линии 3.1 в сценариях обязан быть `CREATE EXTERNAL TABLE`: managed Iceberg-таблица под `DbTxnManager` берёт на `INSERT` EXCLUSIVE-лок на саму себя и упирается в лок Iceberg, а наружу это выходит как `return code 2 from MapRedTask`. Детали и известные ограничения - в `smoke-stand/README.md` и секциях H и I `smoke-stand/TEST-MATRIX.md`.

## Сборка и тесты

Когда зависимости уже доступны локально, предпочитай документированные offline-команды Maven:

```bash
mvn -o test
mvn -o package
```

CI использует:

```bash
mvn -B test
mvn -B -DskipTests package
```

Полезные формы targeted test runs:

```bash
mvn -o -Dtest=CatalogRouterTest test
mvn -o -Dtest=RoutingMetaStoreProxyCompatibilityTest test
```

При изменении `capabilities.yaml` или генерируемых compatibility-таблиц обнови README matrix командой:

```bash
mvn -o -q -Dtest=CapabilityMatrixDocSyncTest -Dcapabilities.updateReadme=true test
```

Для real-installation smoke checks сначала собери fat jar:

```bash
mvn -DskipTests package
```

Затем используй scenario runners после создания и настройки подходящего env-файла:

```bash
scripts/run-real-installation-smoke-simple.sh --scenario all
scripts/run-real-installation-smoke-kerberos.sh --scenario all
```

Без реального кластера те же проверки можно прогнать на локальном стенде:

```bash
cd smoke-stand && ./prepare.sh && docker compose up -d --build
scripts/run-real-installation-smoke-simple.sh --env-file smoke-stand/env/simple.env --scenario all
```

Тесты прогоняй только на Java 17: на новых JVM часть тестов молча самоисключается, потому что Hadoop UGI использует удалённый в JDK 24+ Subject API.

## Архитектурные заметки

- `app` запускает proxy и management HTTP listener.
- `config` парсит configuration models для server, security, catalog, routing, rate limits, compatibility, synthetic locks и management endpoints.
- `backend` отвечает за isolated metastore runtime loading и поведение backend adapter/session.
- `frontend` связывает различия Apache/Hortonworks front-door processors.
- `routing` - центральный слой request resolution. Он отвечает за namespace translation, special-case HMS RPCs, compatibility fallbacks, fanout/degraded reads, synthetic locks, rate limiting, write guards и external table/location rewriting.
- `federation` отвечает за client-visible/internal namespace exposure и view compatibility behavior.
- `security` отвечает за Thrift server auth, Kerberos/front-door request context и local delegation tokens.
- `observability` отвечает за metrics, audit logging, runtime state и health probes.
- `tools` содержит entry points для direct HMS smoke CLI.
- Знание о служебном плейсхолдере Hive (`_dummy_database`/`_dummy_table`, константы `SemanticAnalyzer.DUMMY_DATABASE`/`DUMMY_TABLE`, которые Hive шлёт в `LockRequest` для `INSERT ... VALUES`) живёт только в `routing/HivePlaceholderNamespace`. Плейсхолдер не выбирает каталог, не считается вторым namespace lock-запроса и не переписывается интернализацией в реальную backend-базу. Не добавляй локальных сравнений с `_dummy_database` в других классах.
- Для RPC, у которых Hive IDL не объявляет исключений (`add_write_notification_log`, `open_txns`, `show_locks` и другие: у их `<method>_result` есть только поле `success`), libthrift 0.9.3 подменяет любое серверное исключение на `TApplicationException("Internal error processing <method>")`. Текст отказа таких методов виден только в логе - не рассчитывай, что его получит клиент, и не проверяй его в smoke-скриптах. Настоящий HMS теряет свои тексты ошибок так же.
- «Это Iceberg-таблица?» решается только в `routing/IcebergTablePointerGuard` и только по записи метастора (`metadata_location` в прочитанном `Table`), а не по присланному клиентом объекту: `alter_table_with_environment_context`, которым HiveServer2 открывает `INSERT`, несёт снапшот времени компиляции без единого Iceberg-ключа, поэтому проверка по запросу — no-op для той самой формы, которая теряет данные. Не заводи локальных проверок Iceberg-ности в других классах. Починка — слияние параметров (запись метастора как база, параметры запроса сверху, оба указателя принудительно как в записи), а не восстановление фиксированного списка ключей: набор ключей зависит от версии Iceberg. Цену чтения ограничивает отрицательный TTL-кэш «это не Iceberg-таблица» (`routing.iceberg-pointer-guard.*`); Iceberg-таблицы не кэшируются никогда. Читать запись обязательно через `support.invokeDirect` с `Method` — у Hive 4 в IDL нет позиционного `get_table`, и апгрейд до `get_table_req` умеет только `Hive4BackendAdapter`; вызов по имени (`invokeByReflection`) падает на Hive 4-бэкенде с `NoSuchMethodException` — том самом, где работает CAS. Чтение и запись делает атомарными табличный лок Iceberg (`routing/IcebergCommitLock`, форма скопирована с `MetastoreLock.createLock`: один компонент EXCLUSIVE/TABLE с backend-именем БД, без `txnid`), который guard держит через перечитывание, merge и `alter_table` бэкенда. Порядок менять нельзя: лок берётся **только после** того, как чтение без лока показало, что нужна починка — честный коммит Iceberg шлёт свой `alter_table` изнутри этого же лока, поэтому запрос лока до решения — самоблокировка на каждом коммите. `INSERT` самого Hive лока на целевую таблицу не берёт (в его `LockRequest` только плейсхолдер `_dummy_database`), так что ветка починки ни за чем не стоит в очереди. Не полученный лок никогда не отменяет alter: merge применяется без защиты и считается отдельным `outcome`. На non-default каталогах лок не берётся сознательно — их писателей обслуживает synthetic shim, и за объект этого лока никто не борется.
- Раз запись метастора guard'ом всё равно прочитана, там же живёт и вторая, независимая от указателя защита: сохранение **Hive-дескриптора** Iceberg-таблицы (`routing.iceberg-pointer-guard.hive-engine-descriptor`, дефолт `true`, outcome `hive_descriptor_kept`). `HiveTableOperations` на каждом коммите перестраивает `StorageDescriptor` и выбирает между конкретными `HiveIcebergInputFormat`/`OutputFormat`/`SerDe` плюс `storage_handler` и абстрактными `FileInputFormat`/`FileOutputFormat`/`LazySimpleSerDe` с удалением `storage_handler` — по свойству таблицы `engine.hive.enabled`, а при его отсутствии по `iceberg.engine.hive.enabled` в Hadoop-конфигурации **того процесса, который коммитит**. Таблица, созданная через `STORED BY ICEBERG` в Hive 4, этого свойства не задаёт, поэтому её ломает первый же `INSERT` от 3.1-движка, чей `hive-site.xml` флага не содержит, — и запрос при этом совершенно законный forward commit, а не устаревший alter. Прокси — единственное место, куда дотягиваются оба писателя, поэтому починка живёт здесь, а не в конфигурации движков. Guard только **сохраняет** то, что уже есть в записи: таблице без `storage_handler` он его никогда не выдаёт, и из дескриптора трогает ровно три поля формата, не колонки и не location.
- Классификация backend Thrift-ошибок живёт только в `thriftbridge/ThriftFailureClassifier`: «метода нет» - это `TApplicationException` с типом `UNKNOWN_METHOD` (или отсутствие метода в загруженном runtime), transport failure и protocol desync - отдельные категории. Не пиши локальные `instanceof TApplicationException` для решений про fallback, downgrade или переоткрытие соединения.
- Изолированный BACKEND-рантайм `APACHE_4_1_0` живёт на СВОЁМ libthrift 0.16 (child-first `org.apache.thrift.` и `com.facebook.fb303.` + спутник-jar'ы из `hive-metastore/`, собирается только через `MetastoreApiClassLoader.forBackendRuntime`) - клиент Hive 4 сгенерирован против 0.16, а fat jar несёт 0.9.3 для линии 3.1. Всё, что пересекает границу загрузчиков, конвертирует `thriftbridge/ThriftValueConverter`: структуры - сериализацией (binary protocol стабилен между версиями), инфраструктурные исключения thrift - маппингом на parent-классы, иначе `ThriftFailureClassifier` их не узнает. FRONT-door-мост Hive 4 (`Hive4FrontendBridge`) намеренно остаётся на parent-thrift 0.9.3: его generated processor обязан реализовывать parent `TProcessor` серверного стека. Инварианты закреплены `Hive4IsolatedRuntimeTest`.
- `restcatalog` - Iceberg REST front door поверх routing-слоя: per-catalog сервисы работают через in-process прокси `IMetaStoreClient`; для не-default каталогов включён name translation, чтобы клиент видел внутренние имена баз этого каталога. Write-запросы к таблицам, view (create/update/drop/rename), namespace DDL (create/update/drop) и multi-table transaction commit поддержаны, но только когда namespace резолвится в `routing.default-catalog` - только его таблицы подкреплены реальным HMS-локом, остальные каталоги обслуживает synthetic lock shim без проверки конфликтов. `WriteRouteGate` проверяет резолвленный каталог, а не prefix запроса, поэтому federated-имя под default-prefix отказывается так же, как прямой запрос к non-default prefix; `GET /v1/config` объявляет всю эту асимметрию в `endpoints` - default-каталог видит все тринадцать write-маршрутов, что WriteRouteGate проверяет, остальные каталоги видят только чтение. Не добавляй локальных проверок "разрешён ли write" в других классах - весь gate живёт в `WriteRouteGate`. `DELETE ...?purgeRequested=true` обслуживается как настоящий purge: прокси сам удаляет data- и metadata-файлы таблицы, обходя её манифесты (это единственный путь REST-фронта, который читает Avro), синхронно до ответа `204`. Внутри default-каталога удаление удерживает `WriteRouteGate`, а границу «что именно можно удалить» задаёт только `IcebergPurgePolicy` (`rest-catalog.purge.mode`, дефолт `ALLOW` - нынешнее поведение без ограничений). Не заводи вторую проверку purge в других классах. В `ALLOWLIST` границ две, и обе обязательны: pre-flight по location и `metadata.json` отвечает `403` до дропа, а `PrefixGuardedFileIO` сверяет каждый удаляемый путь и пропускает чужой - в REST-протоколе манифесты пишет клиент, поэтому коммит может указать снапшот на файлы в чужом дереве, и эти пути становятся известны только при обходе манифестов, уже после pre-flight. Отказ бросается как `ForbiddenException` (вендоренный `RESTCatalogAdapter` маппит её в `403`) до дропа, поэтому после отказа целы и таблица, и файлы. Сопоставление префиксов общее с Thrift-путём - `util/PathPrefixAllowlist`. Каждый `IcebergRestService` получает per-catalog Hadoop `Configuration` через `IcebergRestServices.open(..., hadoopConfForCatalog)`, которая в проде резолвится в `router.requireBackend(catalog).hiveConf()` - тот же объект, что уже несёт `fs.defaultFS`/Kerberos-настройки catalog.<name>.conf.* для Thrift-пути. Не заводи для REST-пути отдельную голую `new Configuration()`: под Kerberos это провалит запись в HDFS ("Failed to specify server's Kerberos principal name"), потому что namenode-принципал у каждого каталога свой и известен только через его собственный conf.

- Iceberg на каждом коммите пишет один из двух `StorageDescriptor`: с включённым Hive-движком — `storage_handler` и конкретные `HiveIcebergInputFormat`/`OutputFormat`/`SerDe`, с выключенным — абстрактные `FileInputFormat`/`FileOutputFormat`/`LazySimpleSerDe` без `storage_handler`. Выбор идёт от свойства таблицы `engine.hive.enabled`, а при его отсутствии — от `iceberg.engine.hive.enabled` в конфигурации коммитящего процесса. Таблица, созданная через `STORED BY ICEBERG` в Hive 4, свойства не несёт, поэтому одна запись делала её нечитаемой для клиентов 3.1. Закрыто по одной настройке на каждый путь записи, обе включены по умолчанию: `rest-catalog.hive-engine-descriptor` (собственные REST-коммиты прокси; выставляется на **копии** per-catalog `Configuration`, никогда не на общей с Thrift-путём `hiveConf` бэкенда, и только если значение ещё не задано, чтобы явный `catalog.<name>.conf.*` уважался) и `routing.iceberg-pointer-guard.hive-engine-descriptor` (коммиты по Thrift от движка, который прокси настроить не может, — его Iceberg живёт в чужой JVM). Guard дескриптор **только сохраняет и никогда не навязывает**: запись без `storage_handler` — таблица, которой нечего терять, её alter'ы проходят нетронутыми; из записи берутся лишь три поля формата, колонки и location остаются клиентскими. Испорченная раньше таблица чинится сама на следующем коммите.

## Парсинг конфигурации

- Конфигурация валидируется строго: неверное или противоречивое значение даёт ошибку старта, а не тихий fallback на default.
- Boolean-ключи читай через `PropertyReader.getBoolean` (принимает только `true`/`false`); enum-ключи - через `ConfigParsing.parseEnum` (регистронезависимо, сообщение со списком допустимых констант). Не используй `Boolean.parseBoolean` и голый `Enum.valueOf` для новых ключей.
- Длительности в HiveConf-ключах парсь через `TimeoutValueParser`: набор суффиксов повторяет `HiveConf.unitFor`, значение без суффикса - секунды, нераспознанное - WARN и default (без исключения, парсер работает и в рантайм-путях).
- Комбинации ключей, которые отменяют друг друга (whitelist без нужного `access-mode`, `IN_MEMORY` вместе с zk-настройками, конфликт `host:port` между листенерами), должны падать на старте. Конфликты биндингов проверяй через `ConfigParsing.bindingsConflict`, он учитывает wildcard-хосты.

## Стиль кода

- Следуй существующему Java-стилю: отступ 2 пробела, explicit imports, небольшие сфокусированные классы и понятные имена методов.
- Держи изменения узко сфокусированными. Пути routing, security, compatibility и class-loading чувствительны для production.
- Предпочитай deterministic routing и явные safe failures вместо догадок, когда catalog ownership или namespace context неоднозначны.
- Сохраняй совместимость с Java 17. Не добавляй требования к более новым версиям языка или runtime.
- Не добавляй новые зависимости без реальной необходимости; Hive/Hadoop dependency convergence хрупкий. `hadoop-hdfs` и `hadoop-common` обязаны быть одной версии: Maven их не сравнивает (разные artifact ID), а рассинхрон ломает `DFSOutputStream` (`NoSuchMethodError: FSOutputSummer.<init>`) при любой записи в HDFS изнутри JVM прокси - `pom.xml` явно исключает транзитивный `hadoop-hdfs` из `orc-core` и держит `hadoop-hdfs:2.6.0` напрямую, чтобы совпасть с `hadoop-common`. Проверяй `mvn -o dependency:tree | grep -iE "hadoop-hdfs|hadoop-common"` при трогании этих зависимостей.
- `org.apache.avro:avro` пиньётся в `dependencyManagement` на `1.12.0` - версию, против которой собран `iceberg-core`. `hadoop-mapreduce-client-core` тянет `avro:1.7.4` на той же глубине дерева и выигрывает по порядку объявления, а без post-1.8 API (`LogicalTypes`, `Conversion`) любое чтение манифестов Iceberg (`DELETE ...?purgeRequested=true`) падает с `NoClassDefFoundError`. Avro API на этом classpath больше никто не вызывает: только hadoop-common (`io.serializer.avro`, `AvroFSInput`, аннотации `@Stringable`/`@Nullable` на `Path`/`Text`/`DelegationKey`) и generated jobhistory-классы hadoop-mapreduce, оба вне рантайм-путей прокси; jar-ы в `hive-metastore/` на avro не ссылаются вовсе. Проверяй `mvn -o dependency:tree | grep -i avro` при трогании этих зависимостей.
- Логирование: slf4j-api и binding `slf4j-reload4j` держи на одной версии `${slf4j.version}`. `org.apache.log4j` должен приходить только из reload4j, поэтому `log4j:log4j` исключён во всех зависимостях, которые его тянут (обе Hive-зависимости, `hadoop-mapreduce-client-core`, `curator-test`). Добавляя зависимость, проверь `mvn -o dependency:tree | grep log4j`: второй провайдер этого пакета даст в fat jar классы, выбранные shade-плагином произвольно.
- `hadoop-hdfs` тянет `xerces:xercesImpl`/`xml-apis` (нужны только offline-вьюеру image/edits, не рантайм-пути `DFSClient`, который использует этот прокси) - оба исключены в блоке `hadoop-hdfs` в `pom.xml`. `xercesImpl` несёт `META-INF/services/javax.xml.parsers.*`, что в shaded fat jar делает 2007-летний парсер (с известными CVE) JVM-wide JAXP-провайдером для каждого `DocumentBuilderFactory`/`SAXParserFactory` в процессе, включая Hadoop `Configuration` и парсинг hive-site - и тихо вытесняет парсер JDK. Добавляя зависимость, проверь `mvn -o dependency:tree | grep -iE "xerces|xml-apis"`.
- Комментарии оставляй короткими и только там, где они объясняют неочевидное compatibility, security, routing или class-loading поведение.
- Не форматируй несвязанные файлы и не меняй generated docs, если твоя задача не требует их обновления.

## Тестирование

- Добавляй или обновляй сфокусированные unit tests рядом с затронутым package.
- Для routing behavior сначала смотри `src/test/java/io/github/mmalykhin/hmsproxy/routing`.
- Для config parsing используй существующие parser tests в `src/test/java/io/github/mmalykhin/hmsproxy/config`.
- Для compatibility/runtime loading проверь тесты в `compatibility`, `backend` и `frontend`, прежде чем добавлять новые fixtures.
- In-memory фейки метастора (`RecordingThriftIface` и любые новые) обязаны отдавать на read-путях копии структур, а не хранимые объекты: за настоящим метастором всегда Thrift-провод, и каждый ответ - свежая десериализация. Iceberg `HiveTableOperations` мутирует прочитанный `Table` (ставит новый `metadata_location`) ещё до `alter_table`, поэтому общий объект применяет запись даже к отвергнутому коммиту - а commit-status-проверка Iceberg, которая идёт следом за отказом, читает эту собственную незакоммиченную запись и объявляет коммит успешным. Отказ метастора превращается в `204` с незакоммиченными метаданными, которого на реальном метасторе нет.
- Меняя frontend bridge, добавляй проверку в `FrontendBridgeThriftSerializationTest`: прямой вызов handler proxy не прогоняет ответ через generated write scheme, поэтому неверная форма ответа и смешение классов двух загрузчиков видны только через реальный Thrift round-trip.
- Если изменение влияет на public compatibility behavior, при необходимости обнови `capabilities.yaml`, generated README tables и smoke documentation.
- Если изменение влияет на Kerberos, impersonation, synthetic locks, ACID/txn или Hortonworks-only methods, в финальном сообщении отдельно укажи, какие real-cluster smoke checks не запускались.
- Тесты, которые трогают Hadoop `UserGroupInformation` или `FileSystem`, работают только на Java 17: на JDK 24+ UGI падает с `UnsupportedOperationException: getSubject is not supported`, и такие тесты помечены `Assume`. Прогоняй их на Java 17, иначе они молча скипаются.

## Доказательность проверок

Правила выведены из разбора 2026-07-31…08-04, где каждая из них уже стоила ложного «зелено».

- **Пропуск никогда не заменяет проверку.** `run-iceberg-interop-smoke.sh` восемь строк комментария объяснял, почему при `--origin hive4` не надо гонять участников `hdp` и `apache`, — и потому не мог заметить, что записанная причина неверна, а настоящий дефект хуже. Если сценарий что-то исключает, он это ограничение постулирует, а не проверяет. То же и с флагом, выключенным по умолчанию: ACID-блок SQL-смоука не выполнялся ни разу, хотя строка C2 матрицы стояла ✅.
- **Проверка обязана уметь падать, и это надо увидеть.** Прежде чем засчитывать зелёный прогон, сломай проверяемое и убедись, что он краснеет. Вакуумные формы, пойманные на практике: `select count(*)` вместо выборки значения (печатает число при любом содержимом); `TRUNCATE`, признанный успешным по отсутствию ошибки, тогда как Hortonworks-клиент шлёт пустой список партиций и не усекает ничего; ассерт на строку, которой в выводе не бывает вовсе; `hdfs dfs -ls -R ... 2>/dev/null | grep -c` из контейнера без Kerberos-тикета - отказ аутентификации выходит пустым листингом, и проверка purge печатала «ничего не осталось», ни разу не прочитав HDFS. Общее правило: команда, у которой stderr уведён в `/dev/null`, а статус не проверен, не проверяет ничего - её отказ неотличим от пустого результата.
- **Код возврата конвейера — не результат сценария.** `... | grep ... | tail` возвращает статус `tail`, и падение smoke-прогона так выглядит успехом. Тот же дефект у `git apply -3 patch | head && echo ok`. Смотри последнюю строку самого сценария (`smoke passed` / `error:`) либо проверяй `$?` отдельной командой.
- **Сводка в логе прокси — не доказательство содержимого RPC.** `WriteTraceUtil` печатает выборочные поля: `writeId` в ней нет, и по её отсутствию нельзя заключить, что клиент его не прислал. Различай измеренное и выведенное; если механизм не проверен, так и пиши в матрице, а не выдавай гипотезу за вывод.

## Операционные предосторожности

- Runnable artifact - `target/hms-proxy-<version>-fat.jar`; версия вычисляется через Maven и может включать git-derived metadata.
- Java 17 вместе со старыми Hadoop Kerberos libraries может требовать документированные JVM flags `--add-opens` и `--add-exports`.
- Process-wide Kerberos state (`UserGroupInformation.setConfiguration`, `UserGroupInformation.loginUserFromKeytab`) меняется только через `security/ProcessKerberosConfiguration`: front door ставит полную конфигурацию при старте, остальные пути лишь дополняют её один раз и никогда не перезаписывают. Инвариант закреплён тестом `ProcessWideUgiStateTest`.
- Kerberos health probe (`/readyz`) читает уже существующие login'ы и не делает kinit: endpoint не аутентифицирован и опрашивается каждые несколько секунд. Истёкший TGT отдаётся как `STALE` и не роняет readiness - Hadoop не обновляет TGT keytab-логина сам, а SASL acceptor работает по service keys из keytab.
- `logs/`, `target/`, IDE metadata и локальные smoke env files - локальные артефакты. Не включай их в обычные code changes без явной просьбы.
- Игнорируй директории и файлы `.claude/`, `.idea/` и `*.iml`: не читай их, не анализируй и не упоминай в ответах. Это персональные/IDE-артефакты, не относящиеся к проекту.
- Smoke scripts могут требовать реальные HMS/HS2/Kerberos credentials; не запускай их бездумно против production-like окружений.
- Жизненный цикл Thrift-listener'ов: `MetastoreThriftServer` владеет только своим сокетом. Общий `FrontDoorSecurity` закрывает тот, кто его открыл (`HmsProxyApplication`), а не `stop()` отдельного listener'а. `stop()` обязан оставаться идемпотентным и безопасным в гонке с `serve()` — libthrift 0.9.3 сбрасывает свой флаг `stopped_` уже внутри `serve()`. Shutdown hook останавливает primary listener и ждёт упорядоченную остановку в main-потоке: JVM делает halt сразу после возврата последнего hook.

## Поддержка AGENTS.md

- Если задача меняет что-то, что описано в этом файле или должно в нём появиться (новые модули, команды сборки/тестов, конвенции, операционные правила, чувствительные пути, требования к зависимостям), обнови `AGENTS.md` в том же изменении.
- Не дублируй сюда детали, которые легко вывести из кода или git history; добавляй только то, что важно знать агенту заранее и что не очевидно из самого репозитория.
- Держи формулировки короткими и согласованными с остальным файлом; не переписывай существующие разделы без необходимости.
