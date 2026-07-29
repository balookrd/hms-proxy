# Smoke-стенд

Локальный docker-compose стенд, чтобы гонять наборы `scripts/run-real-installation-smoke-*.sh`
против настоящих Hive-метасторов, а не против продового кластера.

За одним прокси стоят два standalone-метастора:

| Сервис | Backend | Роль | Порт на хосте |
| --- | --- | --- | --- |
| `hms-hdp` | Hortonworks `3.1.0.3.1.0.0-78` | default catalog — владеет ACID/txn-состоянием | 19084 |
| `hms-apache` | Apache `3.1.3` | non-default catalog — synthetic lock shim, purge на стороне прокси | 19083 |
| `proxy` | тестируемый fat jar | front door | 19085 thrift (Apache), 19086 thrift (Hortonworks), 19090 management |
| `hs2` | HiveServer2 3.1.3, смотрит в прокси | SQL-слой, Apache front door | 10000, 10002 |
| `hs2-hdp` | вендорский HDP HiveServer2, смотрит в Hortonworks front door | SQL-слой, `--profile hdp` | 10010, 10012 |
| `namenode` / `datanode` | Apache Hadoop `3.1.3` | первый HDFS-кластер — хранилище каталога `hdp` | 19870 UI, 18020 |
| `namenode-b` / `datanode-b` | Apache Hadoop `3.1.3` | второй HDFS-кластер — хранилище каталога `apache` | 19871 UI, 18021 |
| `kdc` | MIT Kerberos, realm `SMOKE.LOCAL` | только с `--profile kerberos` | 18848/udp |

Базы отдаются как `<catalog>__<db>`: `hdp__default`, `apache__default`. Каталоги живут на **разных
HDFS-кластерах** (см. ниже), у каждого свой `/warehouse` и корень `/external`, разрешённый для
purge внешних таблиц.

Что здесь действительно прогонялось, а что нет, записано в
[TEST-MATRIX.ru.md](TEST-MATRIX.ru.md).

## Как запустить

```bash
# один раз на сборку прокси: разложить jar-ы и classpath метастора
./prepare.sh

# слой 1 — без Kerberos
docker compose up -d --build

# собственный раннер репозитория, против стенда
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  ../scripts/run-real-installation-smoke-simple.sh --env-file env/simple.env --scenario all
```

```bash
# слой 2 — Kerberos, включая HiveServer2. Всегда передавайте --env-file: compose пересоздаёт
# зависимости, до которых дотягивается через depends_on, и вызов без этих переменных перезапустит
# прокси без SASL, из-за чего керберизованный HiveServer2 зависнет в handshake.
docker compose --env-file .env.kerberos --profile kerberos up -d --build
```

Свежему HDFS один раз нужны каталоги (на обоих кластерах):

```bash
for n in stand-namenode stand-namenode-b; do
  docker exec $n bash -c \
    'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /warehouse/hive4 /external && hdfs dfs -chmod -R 1777 /warehouse /external'
done
```

Сценарию notification нужна таблица на HDP-бэкенде — `add_write_notification_log` резолвит таблицу
перед записью лога:

```bash
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e 'create table if not exists smoke_txn_tbl (id int) stored as orc;'"
```

Kerberos-смоук обязан запускаться **внутри** compose-сети: KDC и сервисные принципалы резолвятся
по именам контейнеров:

```bash
docker exec stand-proxy java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp /opt/hms-proxy/hms-proxy.jar io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn \
  --uri thrift://proxy:9083 --auth kerberos \
  --server-principal hive/proxy@SMOKE.LOCAL --client-principal smoke-user@SMOKE.LOCAL \
  --keytab /keytabs/smoke-user.keytab --conf hive.metastore.execute.setugi=false \
  --db hdp__default --table smoke_txn_tbl
```

## SQL-слой

HiveServer2 настроен на `hive.metastore.uris=thrift://proxy:9083`, поэтому каждый statement из
Beeline идёт настоящим клиентским путём — включая `create_table_with_environment_context`, тот
самый RPC, который обязан покрывать guard транзакционного DDL. Beeline запускается внутри
контейнера: там лежат клиентские библиотеки Hive и работают внутрисетевые DNS-имена.

```bash
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true --outputformat=tsv2 \
  -e 'show databases; use apache__default; show tables;'"
```

Запросы выполняются как локальный MapReduce (`mapreduce.framework.name=local`), YARN и Tez не
нужны.

Чтобы проверить guard, раскомментируйте `guard.transactional-ddl.mode=REJECT_TRANSACTIONAL` в
`proxy/hms-proxy.properties`, перезапустите прокси и создайте транзакционную таблицу: прокси должен
отклонить её по имени метода.

## Purge внешних таблиц

Прокси удаляет данные внешних таблиц сам только для каталогов на runtime-профиле `APACHE_3_1_3` —
Hortonworks-бэкенд делает это самостоятельно, поэтому `enabledFor` его пропускает. Проверять нужно
на каталоге `apache`, под разрешённым корнем `/external`.

Таблице при этом обязательно нужно свойство `'external.table.purge'='true'` — обычное правило Hive
для внешних таблиц. Без него `DROP TABLE ... PURGE` данные не трогает, и прокси прав, что тоже их
не трогает. Создайте внешнюю таблицу на такой location, удалите её — и в логе прокси появится:

```
FileSystemExternalTableDropPurger: purged external table data for catalog 'apache'
  at location 'hdfs://namenode-b:8020/external/purge_me'
```

Удаление идёт в пуле `hms-proxy-drop-purge-*`, вне request-потока.

## Два HDFS-кластера

Каталоги лежат на **разных файловых системах** — именно это делает межкластерное поведение прокси
проверяемым:

| Каталог | Метастор | Файловая система | Порты хоста |
| --- | --- | --- | --- |
| `hdp` (default) | `hms-hdp` | `hdfs://namenode:8020` | 19870 / 18020 |
| `apache` | `hms-apache` | `hdfs://namenode-b:8020` | 19871 / 18021 |

Оба кластера работают на **Apache** Hadoop 3.1.3, как бы ни назывался каталог поверх них: имена
`hdp` и `apache` описывают runtime метастора, который федерирует прокси, а не хранилище. Вендорский
дистрибутив HDP попадает в стенд только как *клиент* — сервис `hs2-hdp` — и никогда как файловая
система.

Оба кластера намеренно живут в одном Kerberos-realm: клиент с единственным TGT дотягивается до
любого из них, и поэтому один запрос может читать сразу с двух. Cross-realm trust проверял бы KDC,
а не прокси.

Всю работу делают две настройки:

- `federation.external-table-location-rewrite.mode=REWRITE_IF_SOURCE_DEFAULT_FS` вместе с
  `catalog.<name>.conf.fs.defaultFS`. Иначе `CREATE EXTERNAL TABLE ... LOCATION '/external/x'` в
  каталоге `apache` записался бы относительно `fs.defaultFS` *клиента* — путь, который кластер
  самого каталога обслужить не может. Прокси переписывает его в `hdfs://namenode-b:8020/external/x`
  и делает то же самое с location, явно указывающей на другой кластер.
- `mapreduce.job.hdfs-servers` на обоих HiveServer2, со списком обоих namenode. Керберизованная
  MapReduce-задача собирает delegation tokens только для тех файловых систем, о которых ей сказали;
  пропущенная роняет задачу с `Can't get Master Kerberos principal for use as renewer`, а наружу
  это выходит голым `return code 2`.

Purge удаляет данные на кластере своего каталога, в границах
`catalog.<name>.conf.hms.proxy.external-table-drop-purge.allowed-prefixes`.

## Чем стенд не является

- **Не настоящая инсталляция.** Метасторы поднимаются из jar-ов, лежащих в `hive-metastore/`, без
  остального дистрибутива HDP: ни Ranger, ни Atlas, ни HA. Стенд проверяет поведение протокола и
  маршрутизации, а не интеграцию стека.
- **Нет Ranger/Atlas/HA**, как выше — при этом сам Kerberos-профиль полный: клиент, HiveServer2,
  прокси, оба метастора и HDFS (keytab-ы namenode и datanode, SASL data transfer, SPNEGO)
  аутентифицируются, и ни один сервис не откатывается на simple auth.
- **Нет YARN/Tez.** Запросы идут локальным MapReduce — этого хватает для DDL, чтения и небольших
  записей, но о распределённом исполнении не говорит ничего. Hortonworks-овый HiveServer2 вообще
  стартует только потому, что `hive.in.test` проводит его мимо вендорской проверки
  «mr execution engine is not supported!» — см. раздел ниже.

## Hortonworks front door

`add_write_notification_log` существует только в Hortonworks-версии Thrift-интерфейса, а Thrift не
умеет договариваться о версии, поэтому прокси поднимает под него второй listener
(`additional-frontends.hdp`, порт 9084 в контейнере, 19086 на хосте). Основной listener сохраняет
форму Apache 3.1.3, на которой говорит HiveServer2 3.1.3. Env-файлы смоука направляют туда сценарий
notification через `HMS_SMOKE_NOTIFICATION_URI`; все остальные сценарии остаются на основном front
door.

Негативная половина сценария — тот же вызов против `apache__default` — отклоняется прокси, но
клиент не видит причины: Hive IDL не объявляет исключений для этого метода, поэтому libthrift 0.9.3
подменяет отказ на `Internal error processing add_write_notification_log`. Причина есть в логе
прокси:

```bash
docker logs stand-proxy 2>&1 | grep 'requires a Hortonworks backend runtime'
```

## Iceberg REST catalog front door

Plain-профиль включает и Iceberg REST listener прокси (`rest-catalog.*` в
`proxy/hms-proxy.properties`, host-порт 19183). Он обслуживает все write-роуты —
table, view и namespace DDL, а также multi-table transaction commit, — но только для
default-каталога (`hdp`): его таблицы подкреплены реальным HMS-локом, а любой другой
каталог обслуживает synthetic lock shim и отказывает write с `403`. `--scenario rest`
(или REST-шаг `--scenario all`) гоняет его curl'ом с хоста: discovery конфигурации,
листинги namespace и таблиц, load таблицы, полные write round trip'ы и негативные
формы — неизвестный prefix, неизвестная таблица и write-роут на non-default каталоге;
все должны падать чисто.

Проверке load-table нужна настоящая Iceberg-таблица. Стенд регистрирует минимальную
вручную — написанный руками `metadata.json` в HDFS плюс Hive-оболочка таблицы, которая
на него указывает:

```bash
# 1. Положить минимальный файл table metadata Iceberg на кластер каталога hdp
docker cp <metadata.json> stand-namenode:/tmp/00000-smoke.metadata.json
docker exec stand-namenode bash -c \
  'hdfs dfs -mkdir -p /warehouse/hdp/smoke_iceberg_tbl/metadata &&
   hdfs dfs -put -f /tmp/00000-smoke.metadata.json /warehouse/hdp/smoke_iceberg_tbl/metadata/'

# 2. Зарегистрировать таблицу в каталоге hdp с двумя свойствами, по которым её узнаёт HiveCatalog
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e \"create external table if not exists hdp__default.smoke_iceberg_tbl (id int, ds string)
      stored as parquet
      location 'hdfs://namenode:8020/warehouse/hdp/smoke_iceberg_tbl'
      tblproperties (
        'table_type'='ICEBERG',
        'metadata_location'='hdfs://namenode:8020/warehouse/hdp/smoke_iceberg_tbl/metadata/00000-smoke.metadata.json');\""
```

Файл метаданных прокси читает из HDFS сам (HadoopFileIO с голой `Configuration`), поэтому
прошедший load доказывает всю цепочку: REST-роут → HiveCatalog → собственный routing-слой
прокси → HMS → HDFS. Обычные Hive-таблицы той же базы (`smoke_read_hdp`, `smoke_txn_tbl`)
через REST видны быть не должны — smoke проверяет и это.

### Вторая таблица, на втором каталоге

`HMS_SMOKE_REST_SECOND_PREFIX` (см. `smoke-stand/env/simple.env`) направляет REST-smoke ещё и
на каталог `apache` — это доказывает, что warehouse discovery и чистое представление работают
и для non-default prefix. Ему нужна вторая Iceberg-таблица, зарегистрированная так же, но на
собственном кластере каталога `apache` (`namenode-b`):

```bash
# 1. Положить минимальный файл table metadata Iceberg на кластер каталога apache
docker cp <metadata.json> stand-namenode-b:/tmp/00000-smoke.metadata.json
docker exec stand-namenode-b bash -c \
  'hdfs dfs -mkdir -p /warehouse/apache/smoke_iceberg_tbl_ap/metadata &&
   hdfs dfs -put -f /tmp/00000-smoke.metadata.json /warehouse/apache/smoke_iceberg_tbl_ap/metadata/'

# 2. Зарегистрировать таблицу в каталоге apache
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e \"create external table if not exists apache__default.smoke_iceberg_tbl_ap (id int, ds string)
      stored as parquet
      location 'hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap'
      tblproperties (
        'table_type'='ICEBERG',
        'metadata_location'='hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap/metadata/00000-smoke.metadata.json');\""
```

`metadata.json` — копия файла первой таблицы, с `location`, указывающим на путь
`smoke_iceberg_tbl_ap` выше, и новым `table-uuid`.

Kerberos-профиль тоже гоняет REST listener, на том же порту (19183), что и plain-профиль.
Он отвечает на SPNEGO: KDC выдаёт принципал `HTTP/proxy@SMOKE.LOCAL` в тот же keytab, которым
пользуется Thrift front door, а `hms-proxy-kerberos.properties` указывает `rest-catalog.kerberos.*`
на него. Сам handshake по-прежнему покрыт end-to-end тестом `SpnegoIntegrationTest` на
hadoop-minikdc; дополнительно стенд проверяет его curl'ом с `--negotiate` *изнутри*
`stand-proxy` (KDC и hostname `proxy` резолвятся только внутри сети):

```bash
docker exec stand-proxy kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
docker exec stand-proxy curl -sS --negotiate -u : http://proxy:9183/v1/config
```

Полный набор REST-проверок гоняет сам smoke-скрипт: в `env/kerberos.env` есть REST-блок с
`HMS_SMOKE_REST_CURL_OPTS=--negotiate -u :`, поэтому после kinit выше (и `docker cp` каталога
`scripts/` с env-файлом внутрь `stand-proxy`) runner `--scenario all` / `--scenario rest`
прогоняет все REST-проверки под SPNEGO, включая проверку 401-вызова для запроса без
`--negotiate`.

Какие именно проверки прогонялись на этом профиле — раздел G в `TEST-MATRIX.ru.md`.

## Hortonworks HiveServer2 (`--profile hdp`)

Настоящий HDP HiveServer2, подключённый к Hortonworks front door: этот listener наконец проверяется
тем клиентом, ради которого он существует, а не только smoke CLI. Ему нужен вендорский дистрибутив
— Cloudera закрыла HDP-репозитории, так что ничего из этого не выкачивается из Maven:

```bash
# Нужны ровно две вещи из установки HDP 3.1.0.0-78: hive/ и hadoop/mapreduce.tar.gz.
# Tarball — самодостаточный Hadoop (common, hdfs, mapreduce, yarn, bin, lib/native), именно его
# настоящий HDP-кластер раскладывает по узлам; в голом каталоге hadoop/ нет MapReduce-клиента, и
# HiveServer2 не выполнил бы из него ни одного INSERT. Это только *клиентская* часть HDP —
# собственные HDFS-кластеры стенда работают на Apache Hadoop и к ней отношения не имеют.
HDP_DIST_DIR=~/hdp/3.1.0.0-78 ./prepare.sh
docker compose --profile hdp up -d --build
docker exec stand-hs2-hdp beeline -u jdbc:hive2://localhost:10000/default -n hive -e 'show databases;'
```

Без `HDP_DIST_DIR` стенд по-прежнему собирается — пропускается только этот сервис.

Что он добавляет по сравнению с соседним Apache HiveServer2:

- `add_write_notification_log`, отправленный **самим Hive** после ACID-записи, а не синтезированный
  smoke CLI — с настоящими delta-путями и контрольными суммами.
- Транзакционные таблицы. Standalone-метасторы не могли их создавать (`The table must be stored
  using an ACID compliant format`): `TransactionalValidationListener` нужен `OrcOutputFormat` из
  `hive-exec`, а тому, в свою очередь, — `org.apache.hadoop.mapred.InputFormat` из
  `hadoop-mapreduce-client-core`. Теперь `prepare.sh` раскладывает оба рядом с каждым метастором
  (Apache-jar-ы для Apache, вендорские для Hortonworks), а entrypoint дописывает их **после**
  тестируемого jar, чтобы собственная копия metastore-классов внутри `hive-exec` никогда его не
  перекрыла.

Две вещи этот профиль воспроизводит неточно:

- **Движок исполнения.** Hortonworks собирает без MapReduce, и проверка срабатывает в двух местах с
  разными сообщениями. `HiveConf.initialize()` вызывает `validateExecutionEngine`, поэтому `mr`,
  записанный в `hive-site.xml`, вообще не даёт серверу стартовать (`mr execution engine is not
  supported!`); в конфиге остаётся вендорский Tez, а клиенты переключаются посессионно через
  `set hive.execution.engine=mr;` — SQL-смоук делает это через `HMS_SMOKE_SQL_HDP_SESSION_INIT`.
  Этот `set` тоже валидируется (`hive execution engine mr is not supported.`), и право его пройти —
  единственное, что здесь покупает `hive.in.test=true`. Сам Tez не вариант: ему нужен
  ResourceManager и tez-tarball в HDFS, а в дистрибутиве нет ни того, ни другого. Путь *метаданных*
  — все RPC, которые прокси реально обслуживает, — от этого не страдает; отличается только
  исполнение запросов.
- **Эмуляция.** Нативные библиотеки дистрибутива собраны только под x86_64, поэтому на Apple
  Silicon сервис целиком идёт под `linux/amd64` и стартует заметно медленно (закладывайте пару
  минут).

## Hive 4-бэкенд и Iceberg interop-сценарий (`--profile hive4`)

Профиль `hive4` подменяет default-каталог на standalone-метастор Apache Hive 4.1.0
(`hms-hive4` — тонкая обёртка над официальным образом `apache/hive:4.1.0` в `hms-hive4/`;
обёртка пишет hive-site/core-site сама, потому что в образе нет `find` и его собственный
conf-symlink-механизм молча не работает). Прокси достигает его через изолированный клиентский
рантайм `APACHE_4_1_0` — клиентский jar Hive 4 плюс его спутник-jar'ы (`libthrift-0.16.0`,
`libfb303-0.9.3`, `hive-storage-api-4.1.0`) из `hive-metastore/`, загружаемые child-first.
Каталог `apache` остаётся non-default.

Профиль добавляет и **третий front door**: `additional-frontends.hive4fe` на 9085 (хост 19088)
объявляет диалект `APACHE_4_1_0`, и к нему подключается `hs2-hive4` — HiveServer2 Hive 4.1.0 из
того же официального образа (Tez local mode, Iceberg встроен). У Thrift нет согласования версий,
поэтому Hive 4-клиент не может пользоваться никаким другим listener; два HiveServer2 3.1-диалектов
сохраняют свои front door поверх того же Hive 4-бэкенда.

```bash
./prepare.sh
docker compose --env-file .env.hive4 --profile hive4 --profile hdp up -d --build
# Kerberos-вариант:
docker compose --env-file .env.hive4-kerberos --profile hive4 --profile hdp --profile kerberos up -d --build
```

Свежему HDFS нужен `/warehouse/hive4` (см. инициализацию каталогов выше) — REST-путь create его
не создаёт.

Поверх этого профиля `run-iceberg-interop-smoke.sh` проводит одну Iceberg-таблицу через все
движки и диалекты: REST writer (`iceberg-rest-writer/`, собирается `prepare.sh`, работает внутри
`stand-proxy`, потому что запись data-файлов требует датанод) создаёт таблицу и коммитит
настоящие Parquet-строки через REST; вендорский HDP HiveServer2 читает и дописывает через
Hortonworks front door; Apache HiveServer2 дописывает и читает через Apache front door (оба
несут `iceberg-hive-runtime` 1.6.1 — последний релиз Iceberg с Hive 3-рантаймом); HiveServer2
Hive 4 читает всё это и дописывает через Hive 4 front door; затем REST видит все SQL-коммиты и
удаляет таблицу:

```bash
smoke-stand/run-iceberg-interop-smoke.sh              # plain
smoke-stand/run-iceberg-interop-smoke.sh --kerberos   # SPNEGO + SASL сквозняком
```

Под Kerberos writer аутентифицирует REST одноразовыми SPNEGO-токенами на каждый запрос
(кастомный Iceberg `AuthManager` внутри writer-jar) и логинится в HDFS из keytab smoke-user.
HiveServer2 Hive 4 каждые несколько секунд пишет в лог отказ `scheduled_query_poll` — это
Hive 4-only фича без соответствия в Apache 3.1.3, отклоняется чисто как `UNKNOWN_METHOD`; шум по
design. Известный пробел: `DELETE ... ?purgeRequested=true` отвечает 500 (серверному purge нужен
Avro-класс, которого нет в fat jar), поэтому сценарий использует обычный `DELETE` и удаляет
data-файлы явно. Если сразу после пересборки стенда HiveServer2 отвечает «File does not exist»
на существующие файлы — перезапусти HS2-контейнеры: их JVM кэшируют устаревший DNS-резолв
namenode. Что именно прогонялось — раздел H в `TEST-MATRIX.ru.md`.

## MapReduce под Kerberos

Чтобы керберизованный `INSERT` заработал, нужны две вещи, и `LocalJobRunner` прячет обе за
`return code 2`:

- **Renewer для delegation token.** MapReduce собирает HDFS-токены перед стартом задачи и называет
  для них renewer; без настройки падает с `Can't get Master Kerberos principal for use as renewer`.
  ResourceManager-а здесь нет, поэтому `yarn.resourcemanager.principal` указывает на сам
  HiveServer2. Настройка обязана лежать в `core-site.xml` — `hive-site.xml` до `Configuration`
  задачи не доходит.
- **Нативные библиотеки Hadoop.** Защищённый shuffle идёт через `SecureIOUtils`, который без них
  отказывается работать (`Secure IO is not possible without native code extensions`). Classpath,
  собранный Maven, несёт только Java-классы, поэтому образ копирует `lib/native` из подходящего
  образа дистрибутива Hadoop.

Эти библиотеки собраны только под x86_64. На arm64-хосте (Apple Silicon) они не грузятся, поэтому
сервис HiveServer2 идёт в эмуляции: `HS2_PLATFORM=linux/amd64` в `.env.kerberos`. Эмуляция заметно
медленнее и нужна только для Kerberos-профиля — в plain-профиле защищённого shuffle нет, и он
работает нативно.

## Заметки, на которые ушло время

- Derby обязана сама создать каталог своей базы, поэтому том монтируется на уровень выше
  (`/opt/hms/db`), а не на сам `metastore_db`.
- Сеть compose названа явно: имя по умолчанию содержит подчёркивание, а `HiveMetaStoreClient`
  отвергает URI метастора, в hostname которого оно есть.
- В вендоренных standalone-jar-ах нет schema-скриптов `.sql`, поэтому ACID-таблицы создаются
  программно через `InitSchema` (`TxnDbUtil.prepDb`); остальное DataNucleus создаёт сам при первом
  обращении.
- `HiveMetaStoreClient` в Hortonworks-jar-ах собирает `URI[]` через `Arrays.asList(...).toArray()`,
  который на JDK 9+ возвращает `Object[]`, и `resolveUris` падает с `ClassCastException`. Эта ветка
  выполняется только при выборе URI по умолчанию (`RANDOM`), поэтому smoke CLI фиксирует
  `metastore.thrift.uri.selection=SEQUENTIAL`.
- Метастору нужно переопределить `metastore.expression.proxy` и `metastore.task.threads.always`:
  по умолчанию там названы классы из полного дистрибутива Hive, а не из standalone-jar. Заглушка
  `DefaultPartitionExpressionProxy` при этом бросает `UnsupportedOperationException` на обычном
  `WHERE p='...'` по партиционированной таблице, поэтому вместе с `hive-exec` возвращается
  настоящий `PartitionExpressionForMetastore`.
- Hadoop читает `hadoop.security.authentication` из `core-site.xml` на classpath, а не из
  `-D`-свойств — без файла сервер пытается использовать OS-пользователя как принципал.
- Метастор работает через `TUGIBasedProcessor`, который отвергает второй `set_ugi` в одном
  соединении, поэтому backend-клиенты прокси ставят `hive.metastore.execute.setugi=false`.
- HiveServer2 на старте опрашивает notification log и не стартует, пока его scratch-каталог не
  станет world-writable; поэтому бэкенды ставят `metastore.event.db.notification.api.auth=false`.
- Образы Hadoop от `bde2020` принимают переменные вида `CORE_CONF_<key>` / `HDFS_CONF_<key>` (точки
  как подчёркивания, дефисы как три подчёркивания), а не форму `<FILE>.XML_<key>` из образа
  `apache/hadoop`. Если их перепутать, `fs.defaultFS` останется незаданным, и datanode будет искать
  namenode на собственном hostname.
- HDFS зафиксирован на 3.1.3 под клиентов Hive 3.1.3. `hive-service` дополнительно тянет Hadoop
  2.7.1 рядом с jar-ами 3.1.0, и ужиться на одном classpath они не могут: `DFSClient` из 2.7.1
  требует `SpanReceiverHost`, который Hadoop 3 удалил, и HiveServer2 умирает, не открыв порт.
- Короткое Kerberos-имя каждого сервисного принципала обязано существовать в образе как
  OS-пользователь, иначе поиск групп в Hadoop падает с `no such user`, и HiveServer2 не
  достартовывает.
- Защищённому Hadoop нужен `yarn.resourcemanager.principal` как renewer delegation token даже при
  локальном MapReduce и без YARN; без него каждый statement падает с `Can't get Master Kerberos
  principal for use as renewer` *после* уже успешного SASL-handshake.
- Защищённый DataNode может обойтись без привилегированных портов, только когда включена SASL data
  transfer protection *и* веб-политика — `HTTPS_ONLY`; в остальных случаях он падает с
  `Cannot start secure DataNode due to incorrect config`. `HTTPS_ONLY`, в свою очередь, требует
  keystore — отсюда `hdfs/keystore.jks` и `hdfs/truststore.jks` в дереве. В них самоподписанный
  сертификат для одноразового локального стенда, а пароль лежит открыто в `hdfs/ssl-server.xml` —
  здесь они ничего не защищают, так что переиспользовать их нигде нельзя. Сертификат истекает в
  2036 году; чтобы перевыпустить (обратите внимание на `-storetype JKS`: `keytool` начиная с Java 9
  по умолчанию пишет PKCS12, и Hadoop тогда сообщает `Invalid keystore format`):

  ```bash
  keytool -genkeypair -alias hdfs-stand -keyalg RSA -keysize 2048 -validity 3650 \
    -dname "CN=hdfs-stand, OU=smoke, O=stand, L=local, ST=local, C=US" \
    -keystore hdfs/keystore.jks -storetype JKS -storepass smokepass -keypass smokepass
  keytool -exportcert -alias hdfs-stand -keystore hdfs/keystore.jks -storepass smokepass \
    | keytool -importcert -alias hdfs-stand -keystore hdfs/truststore.jks -storetype JKS \
      -storepass smokepass -noprompt
  ```

## Kerberos и HiveServer2

С `--env-file .env.kerberos` аутентифицирована вся цепочка: клиент держит TGT для
`smoke-user@SMOKE.LOCAL`, HiveServer2 работает как `hive/hs2@SMOKE.LOCAL`, прокси — как
`hive/proxy@SMOKE.LOCAL`, каждый метастор — как `hive/hms-*@SMOKE.LOCAL`. Beeline подключается с
сервисным принципалом в URL:

```bash
docker exec stand-hs2 kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://hs2:10000/default;principal=hive/hs2@SMOKE.LOCAL' --silent=true --outputformat=tsv2 \
  -e 'show databases;'"
```

## Что стенд уже поймал

- `INSERT ... VALUES` ломался во всех каталогах: Hive отправляет `LockRequest`, первый компонент
  которого — плейсхолдер `_dummy_database`/`_dummy_table`, а проверка на несколько namespace
  считала его вторым каталогом. Юнит-тесты такой формы не видели.
- Как только плейсхолдер перестал блокировать запрос, под ним обнаружился второй отказ: `INSERT` в
  non-default каталог открывал транзакцию против TxnHandler-а default-каталога, но лок отправлял в
  бэкенд самого каталога, и тот отвечал `NoSuchTxnException`. Теперь write-локи для non-default
  каталогов обслуживает shim.
- Запрос, джойнящий два каталога, падал целиком с `Error in acquiring locks`: Hive берёт лок на все
  таблицы выражения одним запросом, а любой запрос, назвавший больше одного namespace, отклонялся.
  Та же проверка отвергала и join двух баз *одного* каталога. Теперь lock-запросы расщепляются по
  каталогам. Видно это только настоящему SQL-клиенту — прямой smoke CLI шлёт по одному namespace на
  запрос и такой формы никогда не порождал.
- Readiness-проба больше не мешает SASL: 15 обращений к `/readyz` подряд, а следом Kerberos-смоук
  проходят, тогда как старая проба переписывала процессную конфигурацию UGI.
- Guard транзакционного DDL срабатывает на `create_table_with_environment_context` — том RPC,
  который Beeline действительно отправляет и который guard раньше не покрывал.
- Purge внешних таблиц удаляет настоящие данные HDFS для каталогов `APACHE_3_1_3`, вне
  request-потока.
- Hortonworks front door теперь отвечает настоящему HDP HiveServer2: федерация, DDL, ACID-запись и
  кросс-каталожный join проходят, а `add_write_notification_log` приходит от самого Hive. Пока не
  появился вендорский дистрибутив, этот listener проверялся только smoke CLI.
- После разнесения каталогов по двум HDFS-кластерам переписывание location внешних таблиц перестало
  быть фичей «только для юнит-тестов»: и неквалифицированный `LOCATION`, и location, названный на
  другом кластере, оба попадают на файловую систему каталога-владельца, а одна MapReduce-задача
  читает сразу с обоих.
- Межкластерному purge нужен принципал namenode *своего* каталога в конфиге прокси. Purger сам
  открывает эту файловую систему, поэтому при настроенном принципале только первого кластера
  удаление падало с `Failed to specify server's Kerberos principal name` — **после** уже успешного
  drop, оставляя данные осиротевшими. Отсюда `catalog.<name>.conf.dfs.namenode.kerberos.principal`
  на каждый каталог в Kerberos-профиле.

## Проверено сквозняком

Kerberos, от клиентского тикета до бэкенда:

```
smoke-user@SMOKE.LOCAL --SASL--> hive/hs2 --SASL--> hive/proxy --SASL--> hive/hms-{hdp,apache}
```

`show databases` возвращает `default` и `apache__default`, DDL через прокси проходит, а в
audit-логе записано `"authenticatedUser":"hive"` — собственный принципал HiveServer2, потому что
стенд работает с `hive.server2.enable.doAs=false`. Включите doAs (плюс `hadoop.proxyuser.hive.*`),
чтобы проверять impersonation конечного пользователя.
