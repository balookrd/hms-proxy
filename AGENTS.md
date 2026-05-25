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

## Стиль кода

- Следуй существующему Java-стилю: отступ 2 пробела, explicit imports, небольшие сфокусированные классы и понятные имена методов.
- Держи изменения узко сфокусированными. Пути routing, security, compatibility и class-loading чувствительны для production.
- Предпочитай deterministic routing и явные safe failures вместо догадок, когда catalog ownership или namespace context неоднозначны.
- Сохраняй совместимость с Java 17. Не добавляй требования к более новым версиям языка или runtime.
- Не добавляй новые зависимости без реальной необходимости; Hive/Hadoop dependency convergence хрупкий.
- Комментарии оставляй короткими и только там, где они объясняют неочевидное compatibility, security, routing или class-loading поведение.
- Не форматируй несвязанные файлы и не меняй generated docs, если твоя задача не требует их обновления.

## Тестирование

- Добавляй или обновляй сфокусированные unit tests рядом с затронутым package.
- Для routing behavior сначала смотри `src/test/java/io/github/mmalykhin/hmsproxy/routing`.
- Для config parsing используй существующие parser tests в `src/test/java/io/github/mmalykhin/hmsproxy/config`.
- Для compatibility/runtime loading проверь тесты в `compatibility`, `backend` и `frontend`, прежде чем добавлять новые fixtures.
- Если изменение влияет на public compatibility behavior, при необходимости обнови `capabilities.yaml`, generated README tables и smoke documentation.
- Если изменение влияет на Kerberos, impersonation, synthetic locks, ACID/txn или Hortonworks-only methods, в финальном сообщении отдельно укажи, какие real-cluster smoke checks не запускались.

## Операционные предосторожности

- Runnable artifact - `target/hms-proxy-<version>-fat.jar`; версия вычисляется через Maven и может включать git-derived metadata.
- Java 17 вместе со старыми Hadoop Kerberos libraries может требовать документированные JVM flags `--add-opens` и `--add-exports`.
- `logs/`, `target/`, IDE metadata и локальные smoke env files - локальные артефакты. Не включай их в обычные code changes без явной просьбы.
- Игнорируй директории и файлы `.claude/`, `.idea/` и `*.iml`: не читай их, не анализируй и не упоминай в ответах. Это персональные/IDE-артефакты, не относящиеся к проекту.
- Smoke scripts могут требовать реальные HMS/HS2/Kerberos credentials; не запускай их бездумно против production-like окружений.

## Поддержка AGENTS.md

- Если задача меняет что-то, что описано в этом файле или должно в нём появиться (новые модули, команды сборки/тестов, конвенции, операционные правила, чувствительные пути, требования к зависимостям), обнови `AGENTS.md` в том же изменении.
- Не дублируй сюда детали, которые легко вывести из кода или git history; добавляй только то, что важно знать агенту заранее и что не очевидно из самого репозитория.
- Держи формулировки короткими и согласованными с остальным файлом; не переписывай существующие разделы без необходимости.
