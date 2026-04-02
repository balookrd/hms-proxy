# HMS Proxy

English documentation: [README.md](README.md), [SMOKE.md](SMOKE.md)

Java proxy для Hive Metastore, который поднимает один внешний HMS Thrift endpoint и маршрутизирует
запросы в несколько backend metastore по каталогу.

## Что поддерживается

- один production-facing HMS Thrift endpoint для HiveServer2 и прямых HMS API клиентов
- Apache Hive Metastore `3.1.3` на фронте с compatibility downgrade для старых backend RPC
- Hortonworks Hive Metastore `3.1.0.3.1.0.0-78` на backend
- опциональная смена front-door identity, чтобы proxy представлялся как Hortonworks
  `3.1.0.3.1.0.0-78`, а не Apache Hive Metastore `3.1.3`
- опциональный Hortonworks frontend bridge через HDP `standalone-metastore` jar для
  HDP-only thrift request-wrapper методов
- routing по явному `catName` в новых HMS запросах
- routing по legacy database name в формате `catalog<separator>db` для старых клиентов
- статический набор каталогов в конфиге
- опциональный Kerberos/SASL на фронте
- опциональная impersonation аутентифицированного Kerberos пользователя на backend HMS

## Важные ограничения

- каталоги задаются только через конфиг proxy, а не через `create_catalog` / `drop_catalog`
- legacy database references без префикса каталога идут в `routing.default-catalog`
- session-level compatibility calls и другие global read-only операции без catalog context тоже
  идут в `routing.default-catalog`
- ACID/txn/lock lifecycle RPC без catalog/database context тоже идут в `routing.default-catalog`
- global write operations без явного catalog context в multi-catalog режиме в общем случае
  запрещаются
- если backend HMS возвращает `TApplicationException` для части read-only service API
  (notifications, privilege refresh/introspection, token/key listings кроме delegation-token issuance,
  txn/lock/compaction status), proxy отдаёт empty compatibility response вместо ошибки
- если backend HMS не поддерживает более новые Apache `3.1.3` request-wrapper RPC вроде
  `get_table_req` или `get_table_objects_by_name_req`, proxy автоматически переключается
  на legacy метод, совместимый с Hortonworks `3.1.0.x`
- на практике это означает, что ACID write lifecycle полноценно поддерживается только для
  `default-catalog`, если из payload нельзя извлечь namespace
- request-based ACID методы, где в payload есть `dbName` или `fullTableName`, продолжают
  маршрутизироваться по этому payload

## Сборка

```bash
mvn -o test
mvn -o package
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
```

Версия сборки теперь вычисляется из git на каждом коммите в формате `0.1.<git-distance>-<short-sha>`.

## Запуск

```bash
java -jar "target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar" /etc/hms-proxy/hms-proxy.properties
```

`mvn package` создаёт обычный jar и runnable fat jar с classifier `fat`.
Имя fat jar меняется на каждом новом коммите.

Для Java 17+ с Hadoop 2.x Kerberos библиотеками запускай так:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -jar "target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar" /etc/hms-proxy/hms-proxy.properties
```

## Модель маршрутизации

- catalog-aware клиенты могут отправлять `catName=dbCatalog, dbName=sales`
- legacy клиенты могут использовать database names вроде `catalog1.sales`
- `get_all_databases()` возвращает префиксированные имена вроде `catalog1.sales`
- table objects на выходе переписываются обратно во внешний namespace
- если запрос несёт не-proxy `catName`, например стандартный Hive `hive`, proxy для
  совместимости пытается маршрутизировать по `dbName` / `default-catalog`
- по умолчанию externalized HMS objects используют proxy catalog ids в `catName`/`catalogName`
- для старых HiveServer2 сценариев можно включить
  `compatibility.preserve-backend-catalog-name=true`, чтобы во внешних объектах сохранялся
  backend catalog name, например `hive`, а `dbName` при этом оставался proxy namespace

Разделитель каталога и базы настраивается:

```properties
routing.catalog-db-separator=__
```

Тогда legacy names будут выглядеть как `catalog1__sales`, а не `catalog1.sales`.

## Guard для transactional DDL

Proxy можно настроить так, чтобы он защищал создание и изменение таблиц, если во входящем
metadata таблица помечена как transactional:

- `transactional=true`
- любое непустое значение `transactional_properties`

Правило применяется к `create_table`, `alter_table` и `alter_table_with_environment_context`.

Режим reject:

```properties
guard.transactional-ddl.mode=reject
```

Режим rewrite:

```properties
guard.transactional-ddl.mode=rewrite
```

В режиме rewrite proxy переписывает входящую таблицу в `EXTERNAL_TABLE`, добавляет
`external.table.purge=true` и удаляет `transactional` и `transactional_properties`.

Также можно ограничить его конкретными IP-адресами или CIDR-масками:

```properties
guard.transactional-ddl.mode=reject
guard.transactional-ddl.client-addresses=10.10.0.15,10.20.0.0/16,2001:db8::/64
```

Если `guard.transactional-ddl.client-addresses` задан, проверка применяется только к совпавшим
клиентам. Если не задан, проверка действует для всех клиентов.

## Frontend profile и runtime jars

Можно выбрать, какую версию HMS proxy объявляет наружу:

```properties
compatibility.frontend-profile=APACHE_3_1_3
```

или для Hortonworks клиентов:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
```

Для полноценного Hortonworks frontend нужно указать HDP `standalone-metastore` jar:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

Для isolated Hortonworks backend runtime можно указать backend jar:

```properties
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

Для mixed deployment runtime можно закрепить явно по каталогу:

```properties
catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_0_78
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar

catalog.apache.runtime-profile=APACHE_3_1_3
```

С этим jar proxy поднимает Hortonworks thrift `Processor` в isolated classloader и автоматически
бриджит общие RPC в внутренний Apache `3.1.3` handler. Поддержанные HDP-only методы:

- `truncate_table_req` -> `truncate_table`
- `alter_table_req` -> `alter_table` / `alter_table_with_environment_context`
- `alter_partitions_req` -> `alter_partitions` / `alter_partitions_with_environment_context`
- `rename_partition_req` -> `rename_partition`
- `update_table_column_statistics_req` -> `set_aggr_stats_for`
- `update_partition_column_statistics_req` -> `set_aggr_stats_for`
- `add_write_notification_log` -> прямой Hortonworks passthrough только в Hortonworks backend

## ACID / txn / lock policy

- request-based ACID методы с routable namespace в payload, например
  `get_valid_write_ids`, `allocate_table_write_ids`, `compact`, `compact2`,
  `add_dynamic_partitions`, `fire_listener_event`, `repl_tbl_writeid_state`,
  маршрутизируются по payload
- id-only lifecycle методы, например `open_txns`, `commit_txn`, `abort_txn`,
  `abort_txns`, `check_lock`, `unlock`, `heartbeat`, `heartbeat_txn_range`,
  привязаны к `routing.default-catalog`
- это осознанная модель: proxy не пытается быть distributed ACID coordinator между
  несколькими backend metastore

## Логирование

Для пакета proxy по умолчанию включён подробный debug tracing через bundled `log4j.properties`.
Каждый клиентский вызов получает `requestId`, а в логах есть:

- входящий HMS method и аргументы
- выбранный backend catalog
- proxied thrift method и переписанные аргументы
- backend response или backend error
- итоговый client response или client-visible error

Если логов слишком много, переопредели уровень через свой `log4j.properties`.

## HiveServer2

Укажи в HiveServer2 `hive.metastore.uris` на proxy вместо одного backend HMS.
Для multi-catalog deployment лучше использовать Hive/клиентов, которые сохраняют catalog fields.

Для Beeline/HS2 обычно удобнее separator `__`, чем `.`:

```properties
routing.catalog-db-separator=__
```

Если metadata writes через proxy ведут себя не так, как напрямую против backend HMS, можно
попробовать:

```properties
compatibility.preserve-backend-catalog-name=true
```

Тогда `catName`/`catalogName` будет сохраняться с backend стороны, обычно это `hive`, а routing
по-прежнему будет идти по externalized `dbName`.

## Безопасность

### Без Kerberos

```properties
server.port=9083
security.mode=NONE

catalogs=warehouse
catalog.warehouse.conf.hive.metastore.uris=thrift://hms-backend:9083
routing.default-catalog=warehouse
```

### С Kerberos

Безопасность делится на две независимые части: front door (клиенты -> proxy) и backend
connections (proxy -> HMS).

**Front door**:

```properties
security.mode=KERBEROS
security.server-principal=hive/_HOST@REALM.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab
security.client-principal=hive/_HOST@REALM.COM
security.client-keytab=/etc/security/keytabs/hms-proxy-client.keytab
```

`security.server-principal` и `security.keytab` обязательны при `security.mode=KERBEROS`.
`_HOST` разворачивается в каноническое имя хоста proxy перед Kerberos login. Если DNS-имя
хоста не совпадает с principal в keytab/KDC, используй явный FQDN.

Когда Kerberos включён на фронте, delegation-token методы
(`get_delegation_token`, `renew_delegation_token`, `cancel_delegation_token`)
обслуживаются локально самим proxy.

Если HiveServer2 подключается как `hive/_HOST@REALM.COM` и запрашивает delegation token от имени
пользователя, proxy тоже должен видеть `hadoop.proxyuser.hive.*` правила на своём фронте:

```properties
security.front-door-conf.hadoop.proxyuser.hive.hosts=hs2-1.example.com,hs2-2.example.com
security.front-door-conf.hadoop.proxyuser.hive.groups=*
```

Для persistent storage delegation tokens можно включить `ZooKeeperTokenStore` через обычный
`HiveConf` или напрямую в `hms-proxy.properties`:

```properties
security.front-door-conf.hive.cluster.delegation.token.store.class=org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString=zk1:2181,zk2:2181,zk3:2181
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.znode=/hms-proxy-delegation-tokens
```

Если при этом включён `security.mode=KERBEROS`, proxy автоматически прокинет
`hive.metastore.kerberos.principal` и `hive.metastore.kerberos.keytab.file` в front-door `HiveConf`
из `security.server-principal` и `security.keytab`, чтобы встроенный `ZooKeeperTokenStore`
аутентифицировался в ZooKeeper по SASL/Kerberos. Если для ZooKeeper нужны отдельные credentials,
задай эти `hive.metastore.kerberos.*` явно через `security.front-door-conf.*`.

### Kerberos impersonation

Если хочешь, чтобы backend HMS вызовы выполнялись от имени аутентифицированного пользователя, а не
от service principal proxy:

```properties
security.impersonation-enabled=true
```

Или только для конкретных backend:

```properties
security.impersonation-enabled=false

catalog.catalog1.impersonation-enabled=true
catalog.catalog2.impersonation-enabled=false
```

Это требует:

- `security.mode=KERBEROS` на фронте
- proxy-user impersonation rules на backend HMS для `security.client-principal`

Если backend HMS настроен на Kerberos/SASL:

```properties
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```

Когда для любого backend включён `hive.metastore.sasl.enabled=true`, proxy открывает outbound HMS
соединения под `security.client-principal` и `security.client-keytab`.

## Пример mixed config: Hortonworks front + hdp backend + apache backend + Kerberos

```properties
server.name=hms-proxy
server.bind-host=0.0.0.0
server.port=9083

routing.default-catalog=hdp
routing.catalog-db-separator=__

compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar

security.mode=KERBEROS
security.server-principal=hive/_HOST@EXAMPLE.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab
security.client-principal=hive/_HOST@EXAMPLE.COM
security.client-keytab=/etc/security/keytabs/hms-proxy-client.keytab
security.impersonation-enabled=true

catalogs=hdp,apache

catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_0_78
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
catalog.hdp.impersonation-enabled=true
catalog.hdp.conf.hive.metastore.uris=thrift://hdp-hms.example.com:9083
catalog.hdp.conf.hive.metastore.sasl.enabled=true
catalog.hdp.conf.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM

catalog.apache.runtime-profile=APACHE_3_1_3
catalog.apache.impersonation-enabled=true
catalog.apache.conf.hive.metastore.uris=thrift://apache-hms.example.com:9083
catalog.apache.conf.hive.metastore.sasl.enabled=true
catalog.apache.conf.hive.metastore.kerberos.principal=hive/_HOST@EXAMPLE.COM
```

## Ручной HMS smoke client

Для сценариев из [SMOKE.ru.md](SMOKE.ru.md) в репозитории теперь есть runnable-клиент прямых HMS
RPC:

- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn`
- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification`

Сначала собери jar:

```bash
mvn -DskipTests package
```

Для Java 17+ в Kerberos-окружении с Hadoop 2.x используй те же JVM-флаги, что и для proxy:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn \
  --uri thrift://proxy-host:9083 \
  --auth kerberos \
  --server-principal hive/proxy-host.example.com@REALM.COM \
  --client-principal alice@REALM.COM \
  --keytab /etc/security/keytabs/alice.keytab \
  --krb5-conf /etc/krb5.conf \
  --db hdp__default \
  --table smoke_txn_tbl
```

Этот режим последовательно вызывает:

- `open_txns`
- `allocate_table_write_ids`
- `lock`
- `check_lock`
- `get_valid_write_ids`
- `commit_txn`

Режим `notification` нужен для Hortonworks-only RPC `add_write_notification_log`, поэтому ему
дополнительно нужен HDP standalone metastore jar:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification \
  --uri thrift://proxy-host:9083 \
  --auth kerberos \
  --server-principal hive/proxy-host.example.com@REALM.COM \
  --client-principal alice@REALM.COM \
  --keytab /etc/security/keytabs/alice.keytab \
  --krb5-conf /etc/krb5.conf \
  --db hdp__default \
  --table smoke_txn_tbl \
  --txn-id 1001 \
  --write-id 2001 \
  --files-added hdfs:///warehouse/tablespace/managed/hive/smoke_txn_tbl/delta_1001_1001_0000/bucket_00000 \
  --hdp-standalone-metastore-jar /opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

Что важно:

- `--server-principal` должен указывать на front-door principal самого proxy, а не backend HMS
- `--client-principal` и `--keytab` это Kerberos credentials клиента, которым запускается smoke
- дополнительные HiveConf overrides можно передавать через повторяющийся `--conf key=value`
- `notification` должен проходить для Hortonworks-routed каталога и падать с
  `requires a Hortonworks backend runtime` для Apache-routed каталога
