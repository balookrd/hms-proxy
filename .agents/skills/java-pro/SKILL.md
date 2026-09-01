---
name: java-pro
description: Expert Java 17 development guidelines, concurrency, lock safety, memory efficiency, clean architecture, performance optimization, and JUnit 4 testing tailored for hms-proxy.
---

# Java Pro Guidelines for hms-proxy

Руководство и стандарты экспертной разработки на Java 17 в проекте `hms-proxy`.

---

## 1. Языковые стандарты и Java 17

- **Целевая платформа:** Java 17 (LTS).
- **Идиомы и конструкции:**
  - Используй `record` для неизменяемых структур данных (DTO, конфигураций, результатов вычислений).
  - Используй `switch` expressions и pattern matching (`switch (x) { case A a -> ... }`).
  - Используй pattern matching для `instanceof`: `if (obj instanceof Table table) { ... }`.
  - Используй фабричные методы неизменяемых коллекций: `List.of()`, `Set.of()`, `Map.of()`, `Map.ofEntries()`.
  - Избегай использования устаревших или удаляемых JDK API (например, `Subject.getSubject()`, который ломается на JDK 24+).

---

## 2. Многопоточность и безопасность блокировок

- **Потокобезопасность:**
  - Применяй потокобезопасные структуры данных: `ConcurrentHashMap`, `CopyOnWriteArrayList`, `AtomicReference`, `AtomicInteger`, `AtomicBoolean`.
  - Используй `ConcurrentHashMap.computeIfAbsent` / `putIfAbsent` для атомарных операций кэширования.
- **Жизненный цикл блокировок:**
  - Любая блокировка (локальная `ReentrantLock`, `ReadWriteLock` или распределённая через ZooKeeper / HMS) обязана освобождаться строго в блоке `finally`.
  - Защита от утечек блокировок при прерывании: никогда не оставляй зависший `lock` при возникновении `MetaException` или сетевых сбоев.
- **Защита от бесконечных циклов (Tight Loop Prevention):**
  - При реализации повторов (retry) или обработке фоновых потоков никогда не допускай перезапуска цикла без задержки (backoff / sleep) в секции `catch`.
- **Прерывания и тайм-ауты:**
  - Всегда корректно обрабатывай `InterruptedException`: восстанавливай статус потока через `Thread.currentThread().interrupt()`.

---

## 3. Архитектура и изоляция слоёв в hms-proxy

- **Соблюдение границ подсистем:**
  - `routing/`: Маршрутизация запросов, трансляция неймспейсов, специальные обработчики (`SpecialCaseHandler`).
  - `backend/`: Изолированные загрузчики классов (`MetastoreApiClassLoader`), адаптеры (`BackendAdapter`), сессии (`BackendInvocationSession`).
  - `thriftbridge/`: Классификация ошибок Thrift (`ThriftFailureClassifier`), конвертация структур между версиями Thrift (0.9.3 и 0.16).
  - `federation/`: Видимость внешних/внутренних неймспейсов, фильтрация таблиц и баз данных.
  - `security/`: Аутентификация, контекст Kerberos/UGI (`ClientRequestContext`), токены делегирования.
  - `observability/`: Метрики Prometheus, аудит-логи (`RequestContext.currentObservation()`), health-чеки.
- **Паттерн `SpecialCaseHandler`:**
  - Для добавления или изменения поведения конкретного HMS RPC создавай отдельный класс, реализующий `SpecialCaseHandler`, и регистрируй его в диспетчере `RoutingHandler.buildSpecialCaseHandlers`.

---

## 4. Производительность и управление памятью

- **Single-Flight Coalescing:**
  - Для высоконагруженных операций чтения (например, чтение метаданных баз данных) используй дедупликацию одновременных запросов (`SingleFlightCache`), чтобы предотвратить истощение пула соединений бэкенда.
- **Ограниченные кэши с TTL:**
  - Все кэши обязаны иметь верхнюю границу размера (максимальное число элементов) и TTL, чтобы исключить утечки памяти при динамических неймспейсах.
- **Минимизация аллокаций в горячих путях:**
  - Избегай создания избыточных временных коллекций и лишней глубокой сериализации в цикле обработки Thrift-запросов.
  - Делай защитное копирование (defensive copying) только тогда, когда объект передаётся в вызывающий код или может быть мутирован.

---

## 5. Тестирование и верификация

- **JUnit 4:**
  - Используй аннотации `@Test`, `@Before`, `@After` и ассерты `org.junit.Assert.*` (`assertEquals`, `assertTrue`, `assertThrows`).
- **Изолированное тестирование прокси:**
  - Для тестов маршрутизации и безопасности используй готовые вспомогательные методы из `RoutingMetaStoreProxyTestSupport`.
  - Мокируй бэкенды через `newSession((proxy, method, args) -> ...)` без тяжелых сторонних фреймворков.
- **Тестовое окружение:**
  - Всегда запускай тесты на JDK 17:
    ```bash
    JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 mvn -o test
    ```
  - Покрывай как позитивные сценарии, так и degraded modes, fallback при `UNKNOWN_METHOD`, таймауты и `READ_ONLY` ограничения.

---

## 6. Документация и синхронизация артефактов

- **Всегда синхронно обновляй документацию:**
  - При изменении runtime-поведения, добавлении новых настроек, параметров конфигурации или фич обязательно обновляй:
    - `README.md` и `README.ru.md` (секции возможностей и примеры конфигурации).
    - `src/main/resources/hms-proxy-example.properties` (комментарии и дефолты).
    - `capabilities.yaml` (матрица совместимости; при изменении синхронизируй командой `mvn -o -q -Dtest=CapabilityMatrixDocSyncTest -Dcapabilities.updateReadme=true test`).
