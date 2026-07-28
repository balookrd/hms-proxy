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
- `hive-metastore/` содержит standalone Hive/Hortonworks metastore jar-файлы, которые используются в compatibility/runtime-profile путях. Считай эти jar-файлы намеренными артефактами проекта.
- `capabilities.yaml` управляет генерируемой compatibility-документацией в `README.md` и `README.ru.md`.
- Скрипты smoke-проверок на реальной установке находятся в `scripts/`; Grafana dashboard assets находятся в `monitoring/`.
- `smoke-stand/` - локальный docker-compose стенд для тех же smoke-скриптов: два standalone metastore (Apache и Hortonworks) за прокси, HDFS, HiveServer2 и MIT KDC. Собирается из jar-файлов каталога `hive-metastore/`; тяжёлые входные артефакты не хранятся в git и восстанавливаются через `smoke-stand/prepare.sh`. Детали и известные ограничения - в `smoke-stand/README.md`.

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
- Классификация backend Thrift-ошибок живёт только в `thriftbridge/ThriftFailureClassifier`: «метода нет» - это `TApplicationException` с типом `UNKNOWN_METHOD` (или отсутствие метода в загруженном runtime), transport failure и protocol desync - отдельные категории. Не пиши локальные `instanceof TApplicationException` для решений про fallback, downgrade или переоткрытие соединения.
- `restcatalog` - Iceberg REST front door поверх routing-слоя: per-catalog сервисы работают через in-process прокси `IMetaStoreClient`; для не-default каталогов включён name translation, чтобы клиент видел внутренние имена баз этого каталога. Write-запросы к таблицам (create/commit/drop/rename/register) поддержаны, но только когда namespace резолвится в `routing.default-catalog` - только его таблицы подкреплены реальным HMS-локом, остальные каталоги обслуживает synthetic lock shim без проверки конфликтов. `WriteRouteGate` проверяет резолвленный каталог, а не prefix запроса, поэтому federated-имя под default-prefix отказывается так же, как прямой запрос к non-default prefix; `GET /v1/config` объявляет эту асимметрию в `endpoints`. Не добавляй локальных проверок "разрешён ли write" в других классах - весь gate живёт в `WriteRouteGate`.

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
- Логирование: slf4j-api и binding `slf4j-reload4j` держи на одной версии `${slf4j.version}`. `org.apache.log4j` должен приходить только из reload4j, поэтому `log4j:log4j` исключён во всех зависимостях, которые его тянут (обе Hive-зависимости, `hadoop-mapreduce-client-core`, `curator-test`). Добавляя зависимость, проверь `mvn -o dependency:tree | grep log4j`: второй провайдер этого пакета даст в fat jar классы, выбранные shade-плагином произвольно.
- Комментарии оставляй короткими и только там, где они объясняют неочевидное compatibility, security, routing или class-loading поведение.
- Не форматируй несвязанные файлы и не меняй generated docs, если твоя задача не требует их обновления.

## Тестирование

- Добавляй или обновляй сфокусированные unit tests рядом с затронутым package.
- Для routing behavior сначала смотри `src/test/java/io/github/mmalykhin/hmsproxy/routing`.
- Для config parsing используй существующие parser tests в `src/test/java/io/github/mmalykhin/hmsproxy/config`.
- Для compatibility/runtime loading проверь тесты в `compatibility`, `backend` и `frontend`, прежде чем добавлять новые fixtures.
- Меняя frontend bridge, добавляй проверку в `FrontendBridgeThriftSerializationTest`: прямой вызов handler proxy не прогоняет ответ через generated write scheme, поэтому неверная форма ответа и смешение классов двух загрузчиков видны только через реальный Thrift round-trip.
- Если изменение влияет на public compatibility behavior, при необходимости обнови `capabilities.yaml`, generated README tables и smoke documentation.
- Если изменение влияет на Kerberos, impersonation, synthetic locks, ACID/txn или Hortonworks-only methods, в финальном сообщении отдельно укажи, какие real-cluster smoke checks не запускались.
- Тесты, которые трогают Hadoop `UserGroupInformation` или `FileSystem`, работают только на Java 17: на JDK 24+ UGI падает с `UnsupportedOperationException: getSubject is not supported`, и такие тесты помечены `Assume`. Прогоняй их на Java 17, иначе они молча скипаются.

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
