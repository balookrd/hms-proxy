# Smoke stand

A local docker-compose stand for running the `scripts/run-real-installation-smoke-*.sh` suites
against real Hive metastores instead of a production cluster.

Two standalone metastores sit behind one proxy:

| Service | Backend | Role | Host port |
| --- | --- | --- | --- |
| `hms-hdp` | Hortonworks `3.1.0.3.1.0.0-78` | default catalog — owns ACID/txn state | 19084 |
| `hms-apache` | Apache `3.1.3` | non-default catalog — synthetic lock shim, proxy-side purge | 19083 |
| `proxy` | the fat jar under test | front door | 19085 thrift (Apache), 19086 thrift (Hortonworks), 19090 management |
| `hs2` | HiveServer2 3.1.3, points at the proxy | SQL layer, Apache front door | 10000, 10002 |
| `hs2-hdp` | vendor HDP HiveServer2, points at the Hortonworks front door | SQL layer, `--profile hdp` | 10010, 10012 |
| `namenode` / `datanode` | Apache Hadoop `3.1.3` | first HDFS cluster — storage for the `hdp` catalog | 19870 UI, 18020 |
| `namenode-b` / `datanode-b` | Apache Hadoop `3.1.3` | second HDFS cluster — storage for the `apache` catalog | 19871 UI, 18021 |
| `kdc` | MIT Kerberos, realm `SMOKE.LOCAL` | only with `--profile kerberos` | 18848/udp |

Databases are exposed as `<catalog>__<db>`: `hdp__default`, `apache__default`. The two catalogs
live on different HDFS clusters (see below), each with its own `/warehouse` and an `/external`
root allowlisted for external-table purge.

What has actually been run here — and what has not — is recorded in
[TEST-MATRIX.en.md](TEST-MATRIX.en.md).

## Starting the stand

```bash
# once per proxy build: stage jars and the metastore classpath
./prepare.sh

# layer 1 — no Kerberos
docker compose up -d --build

# the repo's own runner, against the stand
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.20.1 \
  ../scripts/run-real-installation-smoke-simple.sh --env-file env/simple.env --scenario all
```

```bash
# layer 2 — Kerberos, including HiveServer2. Always pass --env-file: compose recreates
# dependencies reached through depends_on, and a call without these variables restarts the
# proxy without SASL, which hangs the Kerberos HiveServer2 in its handshake.
docker compose --env-file .env.kerberos --profile kerberos up -d --build
```

A fresh HDFS needs its directories once (on both clusters), and smoke scenarios need baseline tables. The `init-stand.sh` script creates them; scenario runners call it themselves:

```bash
cd smoke-stand && ./init-stand.sh
```

On the Kerberos profile (`KERBEROS_ENABLED=true` on `stand-proxy`) the script runs `kinit` itself
and connects to HiveServer2 with the `hive/hs2@SMOKE.LOCAL` principal, and the Iceberg REST writer
through SPNEGO. The `smoke_txn_tbl` and `smoke_iceberg_tbl` tables are checked after creation, and
if they cannot be prepared the script prints the output and exits with an error: the failure used to
go to `/dev/null`, and on a fresh Kerberos stand the notification smoke then failed with `Internal
error processing add_write_notification_log`.

If `docker compose up` recreated the namenode containers, restart their HDFS clients
(`stand-hms-*`, `stand-proxy`, `stand-hs2*`): the namenode address is cached in their DFS client,
and recreated containers may swap IPs, after which HiveServer2 answers
`File does not exist: /opt/hs2/scratch`.

Manually, these steps look like:

```bash
for n in stand-namenode stand-namenode-b; do
  docker exec $n bash -c \
    'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /warehouse/hive4 /external && hdfs dfs -chmod -R 1777 /warehouse /external'
done
```

The notification scenario needs its table to exist on the HDP backend — `add_write_notification_log`
resolves the table before writing the log entry:

```bash
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e 'create table if not exists smoke_txn_tbl (id int) stored as orc;'"
```

The Kerberos smoke has to run inside the compose network, because the KDC and the service
principals resolve by container name:

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

## SQL layer

HiveServer2 is configured with `hive.metastore.uris=thrift://proxy:9083`, so every Beeline
statement travels the real client path — including `create_table_with_environment_context`, the
RPC the transactional-DDL guard has to cover. Beeline runs inside the container, because that is
where the Hive client libraries and the in-network DNS names are:

```bash
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true --outputformat=tsv2 \
  -e 'show databases; use apache__default; show tables;'"
```

Queries run as local MapReduce (`mapreduce.framework.name=local`), so no YARN or Tez is needed.

To exercise the guard:

1. Uncomment `guard.transactional-ddl.mode=REJECT_TRANSACTIONAL` in
   `proxy/hms-proxy.properties`.
2. Restart the proxy.
3. Create a transactional table. The proxy must reject it by method name.

### Running the SQL smoke (`env/sql*.env`)

`--scenario sql` runs from inside `stand-hs2-hdp`, whose beeline reaches both HiveServer2. The env
file picks the layout - `sql.env`/`sql-kerberos.env` with the Hortonworks metastore as the default
catalog, `sql-apache.env`/`sql-apache-kerberos.env` with the Apache one. The order:

```bash
smoke-stand/init-stand.sh   # from the host: the container has no docker, so the runner cannot call it
docker exec stand-hs2-hdp kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL   # Kerberos only
docker cp scripts/run-real-installation-smoke.sh stand-hs2-hdp:/opt/hs2-hdp/run-smoke.sh
docker cp smoke-stand/env/sql-kerberos.env stand-hs2-hdp:/opt/hs2-hdp/sql-kerberos.env
docker cp smoke-stand/proxy/hms-proxy-fat.jar stand-hs2-hdp:/opt/hs2-hdp/hms-proxy-fat.jar
docker exec stand-hs2-hdp /opt/hs2-hdp/run-smoke.sh --env-file /opt/hs2-hdp/sql-kerberos.env --scenario sql
```

- `init-stand.sh` creates the read tables the scenario reads right after `use`:
  `hdp__default.smoke_read_hdp` and `apache__default.smoke_read_ap` (names from
  `HMS_SMOKE_HDP_READ_TABLE`/`HMS_SMOKE_APACHE_READ_TABLE`). It creates them through the proxy,
  each through the front door of its own metastore, so the step is the same for both layouts.
  Without them the run fails with `Table not found smoke_read_hdp`.
- The fat jar is needed for `--scenario sql` too: the runner checks `HMS_SMOKE_FAT_JAR` at startup
  (`error: fat jar not found`).
- After HDFS is restarted, restart both HiveServer2 (`docker restart stand-hs2 stand-hs2-hdp`):
  HiveServer2 creates its scratch directory only at startup, and without the restart every new
  session fails with `FileNotFoundException: /opt/hs2/scratch`.

## External-table purge

The proxy deletes external-table data itself only for catalogs on the `APACHE_3_1_3` runtime
profile — a Hortonworks backend does that on its own, so `enabledFor` skips it. Check it against
the `apache` catalog, under the allowlisted `/external` root:

```bash
docker exec stand-namenode-b bash -c \
  'hdfs dfs -mkdir -p hdfs://namenode-b:8020/external/purge_me && echo "7,delta" | hdfs dfs -put -f - hdfs://namenode-b:8020/external/purge_me/data.csv'
```

Then create an external table on that location with `'external.table.purge'='true'` in
`apache__default`, drop it, and the proxy logs:

```
FileSystemExternalTableDropPurger: purged external table data for catalog 'apache'
  at location 'hdfs://namenode-b:8020/external/purge_me'
```

The deletion runs on the `hms-proxy-drop-purge-*` pool, off the request thread.

## Two HDFS clusters

The catalogs sit on different filesystems, which is what makes the proxy's cross-filesystem
behaviour testable at all:

| Catalog | Metastore | Filesystem | Host ports |
| --- | --- | --- | --- |
| `hdp` (default) | `hms-hdp` | `hdfs://namenode:8020` | 19870 / 18020 |
| `apache` | `hms-apache` | `hdfs://namenode-b:8020` | 19871 / 18021 |

Both clusters run Apache Hadoop 3.1.3, whatever the catalog on top of them is called: the names
`hdp` and `apache` describe the metastore runtime the proxy federates, not the storage. The vendor
HDP distribution enters the stand only as a *client* — the `hs2-hdp` service — and never as a
filesystem.

Both clusters share one Kerberos realm on purpose: a client holding a single TGT can then reach
either one, which is what allows a single query to read across them. Cross-realm trust would test
the KDC rather than the proxy.

Two settings do all the work:

- `federation.external-table-location-rewrite.mode=REWRITE_IF_SOURCE_DEFAULT_FS` together with
  `catalog.<name>.conf.fs.defaultFS`. A `CREATE EXTERNAL TABLE ... LOCATION '/external/x'` in the
  `apache` catalog would otherwise be recorded against the *client's* `fs.defaultFS` — a path the
  catalog's own cluster cannot serve. The proxy rewrites it to `hdfs://namenode-b:8020/external/x`,
  and does the same for a location that explicitly names the other cluster.
- `mapreduce.job.hdfs-servers` on both HiveServer2 instances, listing both namenodes. A kerberized
  MapReduce job collects delegation tokens only for the filesystems it is told about; a missing one
  fails the job with `Can't get Master Kerberos principal for use as renewer`, which surfaces as a
  bare `return code 2`.

Purge deletes data on the catalog's own cluster, bounded by
`catalog.<name>.conf.hms.proxy.external-table-drop-purge.allowed-prefixes`. Note that it also needs
the table property `external.table.purge=true` — the ordinary Hive rule for external tables. Without
it `DROP TABLE ... PURGE` leaves the data in place, and the proxy is right not to touch it.

## Stand limitations

- Not a real installation. The metastores run from the jars vendored in `hive-metastore/`,
  without the rest of an HDP distribution: no Ranger, no Atlas, no HA. It validates protocol and
  routing behaviour, not stack integration.
- No Ranger/Atlas/HA, as above. The Kerberos profile itself is complete: the client,
  HiveServer2, the proxy, both metastores and HDFS (namenode and datanode keytabs, SASL data
  transfer, SPNEGO) all authenticate, and no service falls back to simple auth.
- No YARN/Tez. Queries run as local MapReduce, which is enough for DDL, reads and small
  writes, but says nothing about distributed execution. The Hortonworks HiveServer2 only starts at
  all because `hive.in.test` lets it past the vendor's "mr execution engine is not supported!"
  check — see its section below.

## Hortonworks front door

`add_write_notification_log` exists only in the Hortonworks Thrift interface, and Thrift has no
version negotiation, so the proxy exposes a second listener for it (`additional-frontends.hdp`,
container port 9084, host port 19086). The primary listener keeps the Apache 3.1.3 shape that
HiveServer2 3.1.3 speaks. The smoke env files point the notification scenario there with
`HMS_SMOKE_NOTIFICATION_URI`; every other scenario stays on the primary front door.

The negative half of the scenario — the same call against `apache__default` — is refused by the
proxy, but the client cannot see why: the Hive IDL declares no exceptions for this method, so
libthrift 0.9.3 replaces the failure with `Internal error processing add_write_notification_log`.
The reason is in the proxy log:

```bash
docker logs stand-proxy 2>&1 | grep 'requires a Hortonworks backend runtime'
```

## Iceberg REST catalog front door

The plain profile also enables the proxy's Iceberg REST listener (`rest-catalog.*` in
`proxy/hms-proxy.properties`, host port 19183). Every write route — table, view and namespace
DDL, plus multi-table transaction commit — is served, but only for the default catalog (`hdp`):
its tables are backed by a real HMS lock, while every other catalog is served by the synthetic
lock shim and refuses writes with `403`. `--scenario rest` (or the REST step of `--scenario all`)
drives it with curl from the host: config discovery, namespace and table listings, a table load,
the full write round trips, and the negative shapes — unknown prefix, unknown table, and a write
route on a non-default catalog, all of which must fail cleanly.

The load-table check needs a real Iceberg table. The stand registers a minimal one by hand —
a hand-written `metadata.json` in HDFS plus a Hive table shell that points at it:

```bash
# 1. Put a minimal Iceberg table metadata file onto the hdp catalog's cluster
docker cp <metadata.json> stand-namenode:/tmp/00000-smoke.metadata.json
docker exec stand-namenode bash -c \
  'hdfs dfs -mkdir -p /warehouse/hdp/smoke_iceberg_tbl/metadata &&
   hdfs dfs -put -f /tmp/00000-smoke.metadata.json /warehouse/hdp/smoke_iceberg_tbl/metadata/'

# 2. Register the table in the hdp catalog with the two properties HiveCatalog keys on
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e \"create external table if not exists hdp__default.smoke_iceberg_tbl (id int, ds string)
      stored as parquet
      location 'hdfs://namenode:8020/warehouse/hdp/smoke_iceberg_tbl'
      tblproperties (
        'table_type'='ICEBERG',
        'metadata_location'='hdfs://namenode:8020/warehouse/hdp/smoke_iceberg_tbl/metadata/00000-smoke.metadata.json');\""
```

The proxy reads the metadata file from HDFS itself (HadoopFileIO with a bare `Configuration`),
so a passing load proves the whole chain: REST route → HiveCatalog → the proxy's own routing
layer → HMS → HDFS. Plain Hive tables of the same database (`smoke_read_hdp`,
`smoke_txn_tbl`) must stay invisible through REST — the smoke asserts that too.

### A second table, on the second catalog

`HMS_SMOKE_REST_SECOND_PREFIX` (see `smoke-stand/env/simple.env`) points the REST smoke at the
`apache` catalog too, which proves warehouse discovery and the clean view also work for a
non-default prefix. It needs a second Iceberg table, registered the same way but on the
`apache` catalog's own cluster (`namenode-b`):

```bash
# 1. Put a minimal Iceberg table metadata file onto the apache catalog's cluster
docker cp <metadata.json> stand-namenode-b:/tmp/00000-smoke.metadata.json
docker exec stand-namenode-b bash -c \
  'hdfs dfs -mkdir -p /warehouse/apache/smoke_iceberg_tbl_ap/metadata &&
   hdfs dfs -put -f /tmp/00000-smoke.metadata.json /warehouse/apache/smoke_iceberg_tbl_ap/metadata/'

# 2. Register the table in the apache catalog
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e \"create external table if not exists apache__default.smoke_iceberg_tbl_ap (id int, ds string)
      stored as parquet
      location 'hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap'
      tblproperties (
        'table_type'='ICEBERG',
        'metadata_location'='hdfs://namenode-b:8020/warehouse/apache/smoke_iceberg_tbl_ap/metadata/00000-smoke.metadata.json');\""
```

The `metadata.json` is a copy of the first table's, with `location` pointed at the
`smoke_iceberg_tbl_ap` path above and a fresh `table-uuid`.

The Kerberos profile runs the REST listener too, on the same port (19183) as the plain profile.
It answers SPNEGO: the KDC issues an `HTTP/proxy@SMOKE.LOCAL` principal into the same keytab the
Thrift front door uses, and `hms-proxy-kerberos.properties` points `rest-catalog.kerberos.*` at
it. The handshake itself is also covered end-to-end by `SpnegoIntegrationTest` on hadoop-minikdc;
the stand additionally exercises it with `curl --negotiate` run *inside* `stand-proxy` (the KDC
and the `proxy` hostname only resolve in-network):

```bash
docker exec stand-proxy kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
docker exec stand-proxy curl -sS --negotiate -u : http://proxy:9183/v1/config
```

The full REST check set runs through the smoke script itself: `env/kerberos.env` carries the REST
block with `HMS_SMOKE_REST_CURL_OPTS=--negotiate -u :`, so after the kinit above (and a
`docker cp` of `scripts/` and the env file into `stand-proxy`) the `--scenario all` / `--scenario
rest` runner drives every REST check under SPNEGO, including the 401-challenge assertion for a
request without `--negotiate`.

See `TEST-MATRIX.en.md` section G for exactly which checks have been run against this profile.

## Hortonworks HiveServer2 (`--profile hdp`)

A real HDP HiveServer2 that connects to the Hortonworks front door, so that listener is driven by
the client it exists for instead of by the smoke CLI alone. It needs the vendor distribution —
Cloudera closed the HDP repositories, so nothing here can be fetched from Maven:

```bash
# Needs exactly two things from an HDP 3.1.0.0-78 install: hive/ and hadoop/mapreduce.tar.gz.
# The tarball is a self-contained Hadoop (common, hdfs, mapreduce, yarn, bin, lib/native) and is
# what a real HDP cluster ships to its nodes; the bare hadoop/ directory has no MapReduce client,
# so HiveServer2 could not run a single INSERT from it. Note this is the HDP *client* side only -
# the stand's own HDFS clusters are Apache Hadoop and are untouched by it.
HDP_DIST_DIR=~/hdp/3.1.0.0-78 ./prepare.sh
docker compose --profile hdp up -d --build
docker exec stand-hs2-hdp beeline -u jdbc:hive2://localhost:10000/default -n hive -e 'show databases;'
```

Without `HDP_DIST_DIR` the stand still builds; only this service is skipped.

What it adds over the Apache HiveServer2 next door:

- `add_write_notification_log` sent by Hive itself after an ACID write, not synthesized by the
  smoke CLI — with real delta paths and checksums.
- Transactional tables. The standalone metastores could not create them (`The table must be stored
  using an ACID compliant format`), because `TransactionalValidationListener` needs `OrcOutputFormat`
  from `hive-exec` and that class in turn needs `org.apache.hadoop.mapred.InputFormat` from
  `hadoop-mapreduce-client-core`. `prepare.sh` now stages both next to each metastore — Apache jars
  for the Apache one, vendor jars for the Hortonworks one — and the entrypoint appends them after
  the jar under test, so `hive-exec`'s own copy of the metastore classes can never shadow it.

Two things this profile does not reproduce faithfully:

- The execution engine. Hortonworks builds without MapReduce, and the check fires in two places
  with two different messages. `HiveConf.initialize()` runs `validateExecutionEngine`, so naming
  `mr` in `hive-site.xml` stops the server from starting (`mr execution engine is not supported!`);
  the config therefore keeps the vendor default of Tez, and clients switch per session with
  `set hive.execution.engine=mr;` — which the SQL smoke does through
  `HMS_SMOKE_SQL_HDP_SESSION_INIT`. That `set` is validated too (`hive execution engine mr is not
  supported.`), and passing it is the single thing `hive.in.test=true` buys here. Tez itself is not
  an option: it needs a ResourceManager and the Tez tarball in HDFS, and this distribution ships
  neither. The *metadata* path — every RPC the proxy actually serves — is unaffected; only query
  execution differs from a real HDP cluster.
- Emulation. The distribution's native libraries are x86_64 only, so on Apple Silicon the whole
  service runs under `linux/amd64` and is noticeably slow to start (allow a couple of minutes).

## Hive 4 backend and the Iceberg interop scenario (`--profile hive4`)

The `hive4` profile swaps the default catalog for an Apache Hive 4.1.0 standalone metastore
(`hms-hive4`, a thin wrapper over the official `apache/hive:4.1.0` image in `hms-hive4/`; the
wrapper writes hive-site/core-site itself because the image has no `find` and its own
conf-symlink mechanism silently does nothing). The proxy reaches it through the isolated
`APACHE_4_1_0` client runtime — the Hive 4 client jar plus its companion jars
(`libthrift-0.16.0`, `libfb303-0.9.3`, `hive-storage-api-4.1.0`) from `hive-metastore/`, loaded
child-first. The `apache` catalog stays as the non-default one.

The profile also adds the third front door: `additional-frontends.hive4fe` on 9085 (host
19088) advertises the `APACHE_4_1_0` dialect, and `hs2-hive4` — a Hive 4.1.0 HiveServer2 from
the same official image, Tez local mode, Iceberg built in — connects to it. Thrift has no
version negotiation, so a Hive 4 client can use no other listener; the two 3.1-dialect
HiveServer2 instances keep their own front doors on top of the same Hive 4 backend.

```bash
./prepare.sh
docker compose --env-file .env.hive4 --profile hive4 --profile hdp up -d --build
# Kerberos variant:
docker compose --env-file .env.hive4-kerberos --profile hive4 --profile hdp --profile kerberos up -d --build
```

A fresh HDFS needs `/warehouse/hive4` (see the directory init above) — the REST create path
does not mkdir it.

On top of this profile, `run-iceberg-interop-smoke.sh` drives one Iceberg table through every
engine and every dialect: the REST writer (`iceberg-rest-writer/`, built by `prepare.sh`, runs
inside `stand-proxy` because data-file writes need the datanodes) creates the table and commits
real Parquet rows through REST; the vendor HDP HiveServer2 reads and appends through the
Hortonworks front door; the Apache HiveServer2 appends and reads through the Apache one (both
carry `iceberg-hive-runtime` 1.6.1 — the last Iceberg release with a Hive 3 runtime); the Hive 4
HiveServer2 reads all of it and appends through the Hive 4 front door; REST then sees every SQL
commit and drops the table:

```bash
smoke-stand/run-iceberg-interop-smoke.sh --prefix hive4              # plain
smoke-stand/run-iceberg-interop-smoke.sh --prefix hive4 --kerberos   # SPNEGO + SASL end to end
```

The scenario is not tied to the Hive 4 backend: `--prefix` names whichever catalog the running
proxy config makes default, because REST writes are gated to it. All three backends the stand
carries can take that role, and each is a different runtime profile on the write path:

| Backend under test | Runtime profile | Compose call | Scenario |
| --- | --- | --- | --- |
| Hortonworks `3.1.0` (`hms-hdp`) | `HORTONWORKS_3_1_0_3_1_0_78` | `docker compose --profile hdp --profile hive4fe up -d --build` | `--prefix hdp` |
| Apache `3.1.3` (`hms-apache`) | `APACHE_3_1_3` | `docker compose --env-file .env.apache --profile hdp --profile hive4fe up -d --build` | `--prefix apache` |
| Apache Hive `4.1.0` (`hms-hive4`) | `APACHE_4_1_0` | `docker compose --env-file .env.hive4 --profile hive4 --profile hive4fe --profile hdp up -d --build` | `--prefix hive4` |

Append `--profile kerberos` and the matching `-kerberos` env file for the SASL variant. The
`apache` catalog lives on the second HDFS cluster, so with `--prefix apache` the table and its
files land on `namenode-b` — the scenario cleans up there on its own.

`--origin` picks which of the four front doors creates the table and writes first; the other
three then modify what it made, each reading the running total before its own append:

```bash
smoke-stand/run-iceberg-interop-smoke.sh --prefix hive4 --origin hdp      # SQL creates, REST modifies
smoke-stand/run-iceberg-interop-smoke.sh --prefix hive4 --origin rest     # the default
```

With `--origin hive4` the two 3.1-line engines sit the round out, and the run says so: a table
created by `STORED BY ICEBERG` carries no concrete `inputFormat` in its StorageDescriptor
(Hive 4 resolves it through the storage handler at plan time), and Hive 3.1 cannot plan against
that. Tables created by REST or by the 3.1 storage handler carry the concrete
`HiveIcebergInputFormat` and are readable by every engine, Hive 4 included — see
`TEST-MATRIX.en.md` H9-H12.

Under Kerberos the writer authenticates REST with per-request SPNEGO tokens (a custom Iceberg
`AuthManager` inside the writer jar) and logs into HDFS from the smoke-user keytab. The Hive 4
HiveServer2 logs `scheduled_query_poll` refusals every few seconds — a Hive 4-only feature with
no Apache 3.1.3 mapping, refused cleanly as `UNKNOWN_METHOD`; noise by design. The scenario ends with a real
`DELETE ... ?purgeRequested=true` and asserts no data, manifest or metadata file survives it -
that manifest walk is the one REST path that reads Avro, so a broken Avro dependency shows up
here and nowhere else. That assertion reads HDFS from inside the namenode container, which under
Kerberos has no ticket of its own, so the scenario logs it in from the node keytab first; without
that the call fails with `Client cannot authenticate via:[TOKEN, KERBEROS]` and an unreadable HDFS
would read as an empty one (it did, until 2026-08-04 - see the revalidation log).
If HiveServer2 answers "File does not exist" for files that exist right after a
stand rebuild, restart the HS2 containers — their JVMs cache a stale DNS resolution of the
namenodes. See `TEST-MATRIX.en.md` section H for what has been run.

## Row-level DML (`run-iceberg-rowlevel-smoke.sh`)

The interop scenario only ever appends, so it never produces an Iceberg delete file. This one
does. Hive 4 is the only engine on the stand with native row-level DML over Iceberg, so it plays
the writer: it deletes and updates rows in a format-version 2 table the REST front door created,
and the other three front doors then read what it left behind.

```bash
smoke-stand/run-iceberg-rowlevel-smoke.sh --prefix hive4              # both delete modes
smoke-stand/run-iceberg-rowlevel-smoke.sh --prefix hive4 --kerberos
smoke-stand/run-iceberg-rowlevel-smoke.sh --prefix hive4 --mode merge-on-read
```

`--mode` picks one of `write.delete.mode`/`write.update.mode`; without it both run. The mode is
not taken on trust — the REST writer's `files` command reports the planned scan's data- and
delete-file counts, and the run asserts merge-on-read leaves a delete file behind while
copy-on-write leaves none. Every read assertion is a full `select id, src` row scan rather than
`select count(*)`, because Hive can answer a count from the Iceberg summary it holds as table
stats, which a reader unable to apply delete files would still get right.

The result is the opposite of the `inputFormat` asymmetry above: both 3.1 engines read
merge-on-read correctly — `iceberg-hive-runtime` 1.6.1 applies position deletes — and can even
`INSERT` on top of a row-level-modified table. What they cannot do is produce row-level changes:
`DELETE` and `UPDATE` are refused at compile time with `SemanticException [Error 10297] ... that
is not transactional`. See `TEST-MATRIX.en.md` H13-H20.

## Writer isolation (`run-iceberg-concurrency-smoke.sh`)

The Iceberg REST front door allows writes into the default catalog only, because only there does
a commit take a real Hive lock; every other catalog is served by the synthetic shim, which grants
locks without checking conflicts. This scenario tests the half of that argument a stand can test:

```bash
smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hive4 --writers 8
```

It creates a table with one baseline row, fires N concurrent appends at it through the REST front
door, counts the writers that exited 0 and requires the table to hold exactly that many rows plus
the baseline. A writer that fails loudly (`CommitFailedException`) is correct behaviour under
contention and does not fail the run — a writer that reports success while its rows are gone
does. At eight writers the stand usually produces both outcomes at once — seven commits and one
refusal, with every committed row present — but not always: whether any writer runs out of
retries varies run to run, and a run where all eight commit is equally correct. What is invariant,
and what the scenario actually asserts, is that the row count equals the writers that reported
success.

`--sql-writers N` adds Hive `INSERT`s to the same table, so the contention crosses front doors:

```bash
smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hive4 --writers 6 --sql-writers 2 --sql-engine hdp
```

A beeline `INSERT` spends tens of seconds in MapReduce before committing while a REST append
commits immediately, so firing both at once would run them in sequence and prove nothing.
The scenario instead keeps issuing REST appends in rounds while any SQL writer is alive, and then
asserts that the two sides' commit windows actually intersect — it reads the `alter_table`
timestamps back out of the proxy log, where the thread name tells the paths apart. See
`TEST-MATRIX.en.md` section I.

## Multi-table transactions under contention (`run-iceberg-txn-contention-smoke.sh`)

`POST /v1/{prefix}/transactions/commit` carries several tables in one request, and a client can
reasonably read that as all-or-nothing. This scenario measures what actually happens when a
competing writer moves one of those tables out from under the transaction:

```bash
smoke-stand/run-iceberg-txn-contention-smoke.sh --prefix hive4 --kerberos
```

The contention is real rather than injected: a second writer appends to one of the two tables
through the same front door, advancing that table's `main` ref, and the transaction then arrives
carrying the snapshot id read before that append — exactly what a losing racer would send, so no
timing games are needed. The transaction is refused with `409 CommitFailedException: Requirement
failed: branch main has changed`, neither table is left carrying the update, and the competing
writer's rows survive.

The run then does the same transaction with the current snapshot id and requires it to be
accepted and applied to both tables. That positive control is the point: without it the
refusal above would look equally convincing if the body were malformed, the prefix wrong or the
table not writable at all.

Requirement contention being all-or-nothing does not make the route atomic in general. When a
commit fails rather than a requirement, Iceberg's own adapter validates every requirement up
front and then commits the tables one by one with no rollback, so a failure partway leaves the
earlier tables committed and the request answers `500 CommitStateUnknownException`. Both halves
are recorded as I5 and I6 in `TEST-MATRIX.en.md`.

## Apache Ranger and shared metadata cache (`run-ranger-shared-cache-smoke.sh`)

Validates embedded Apache Ranger policy evaluation combined with global shared metadata caching (`shared-across-users=true`) and Kerberos user authentication:

```bash
cd smoke-stand && ./prepare.sh
# Runs in fully kerberized mode by default:
./run-ranger-shared-cache-smoke.sh
# Or without Kerberos (simple auth):
./run-ranger-shared-cache-smoke.sh --no-kerberos
```

The scenario exercises multi-user authorization under Kerberos (with user keytabs issued by the stand's KDC) and global caching against the running proxy:
- Creates isolated test namespaces `sales` (authorized tables `orders`, `customers`, and forbidden table `secret_orders`) and `finance` (tables `reports`, `expenses`).
- Verifies that `alice` (via Kerberos SASL or `set_ugi`) sees only authorized `sales` database and its permitted tables in `get_all_databases`, `get_all_tables`, `get_tables` with pattern, and `get_table_meta`.
- Verifies table-level granularity: `alice` receives a `NoSuchObjectException` refusal when directly requesting `get_table` on `secret_orders` within the permitted `sales` database, and table listings omit it.
- Verifies that `bob` queries `get_all_databases` from the shared cache (without repeated backend HMS queries), receiving only `finance`.
- Asserts that `bob` is rejected when accessing `sales` and `alice` is rejected when accessing `finance`.
- Verifies Ranger group policies: users `charlie` (with group `sales`) and `david` (with group `finance`) are authorized according to group rules.
- Asserts that unauthorized user `eve` cannot access any private namespaces, while `admin` sees all namespaces and cleans up.
- Validates Prometheus metrics (`hms_proxy_ranger_evaluations_total` with `result="allowed"` and `result="denied"`, `hms_proxy_ranger_filtered_objects_total`, `hms_proxy_ranger_plugin_info`).

## Auth mode of `run-database-cache-smoke.sh`, `run-unprefixed-db-and-reload-smoke.sh`, `run-schema-pattern-smoke.sh`, `run-news-txn-smoke.sh`, `run-spark-path-based-smoke.sh`, `run-partition-and-rename-smoke.sh`, `run-merged-databases-smoke.sh`, `run-cross-dc-resilience-smoke.sh`

These eight scenarios take no mode flag: they read `KERBEROS_ENABLED` of the running container
(`stand-proxy`; `stand-hs2-hdp` for `run-news-txn-smoke.sh`) and switch to SASL on the Kerberos
profile by themselves. Stand for them:

```bash
cd smoke-stand
docker compose --env-file .env.kerberos --profile kerberos --profile hdp up -d --build
./init-stand.sh
```

`--profile hdp` is needed only by `run-news-txn-smoke.sh` (container `stand-hs2-hdp`).

What changes on the Kerberos profile:

| Scenario | Client | Authentication |
| --- | --- | --- |
| `run-database-cache-smoke.sh`, `run-unprefixed-db-and-reload-smoke.sh`, `run-schema-pattern-smoke.sh`, `run-spark-path-based-smoke.sh`, `run-partition-and-rename-smoke.sh`, `run-merged-databases-smoke.sh`, `run-cross-dc-resilience-smoke.sh` | `HmsMetastoreSmokeCli metadata` inside `stand-proxy` | `--auth kerberos --client-principal <user>@SMOKE.LOCAL --keytab /keytabs/<user>.keytab --server-principal hive/proxy@SMOKE.LOCAL`; every call is made as `admin`. Before the first call the scenario runs `kinit` with `/keytabs/admin.keytab` into a separate cache `/tmp/krb5cc_<scenario>` and fails if the KDC or the keytab is unavailable |
| `run-news-txn-smoke.sh` | beeline inside `stand-hs2-hdp` | `kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL` in the container, JDBC URL `jdbc:hive2://hs2-hdp:10000/default;principal=hive/hs2-hdp@SMOKE.LOCAL` (overridden by `BEELINE_JDBC_URL`) |

Per-scenario details:

- `run-database-cache-smoke.sh` recreates the proxy with `hms-proxy-db-cache-kerberos.properties`
  on the Kerberos profile (that is `hms-proxy-kerberos.properties` plus the cache block) and with
  `hms-proxy-db-cache.properties` on the plain one. Afterwards the proxy goes back to the config it
  was started with before the scenario (the container's `PROXY_CONFIG`). Only `proxy` is recreated
  (`--no-deps`): a recreated `kdc` would mint new keys into every keytab under the running
  services. The scenario waits up to 90 s for the proxy's `/readyz`.
- `run-unprefixed-db-and-reload-smoke.sh` appends lines to the config the proxy runs with
  (`proxy/hms-proxy-kerberos.properties` on the Kerberos profile) and restores it from `.bak` at the
  end. Its checks take `hdp` as the default catalog, so on a config with another
  `routing.default-catalog` (`.env.apache-kerberos`) the scenario fails at once. Before the checks
  it logs the namenode containers in with their keytabs; otherwise `hdfs dfsadmin -safemode wait`
  is refused authentication and waits for nothing.
- `run-merged-databases-smoke.sh` likewise edits the config the proxy runs with and logs the
  namenode containers in before `hdfs dfsadmin -safemode wait`; a `dfsadmin` refusal fails the
  scenario (it used to be silenced with `|| true`).
- `run-cross-dc-resilience-smoke.sh` recreates the proxy with
  `hms-proxy-cross-dc-kerberos.properties` on the Kerberos profile (that is
  `hms-proxy-cross-dc.properties` plus the Kerberos block of `hms-proxy-kerberos.properties`) and
  with `hms-proxy-cross-dc.properties` on simple, and puts back the config the proxy ran with
  afterwards. Only `proxy` is recreated (`--no-deps`).

The "object is gone" checks of these scenarios pass `--expect-error NoSuchObjectException` to the
CLI. The CLI compares the expected string with every exception of the cause chain in the form
`ClassName: message` and fails when the op succeeded or failed with a different exception. A
failure to connect or log in is never the expected error, so an unreachable proxy fails such a
check. These checks used to read `if run_cli ... 2>/dev/null; then fail; fi` and counted any
failure as "the object is gone".

## Database cache background auto-refresh (`run-database-cache-smoke.sh`)

Validates background periodic auto-refresh of database list (`DatabaseListCache`) and metadata (`DatabaseMetadataCache`) caches while active client traffic is observed:

```bash
cd smoke-stand && ./prepare.sh
./run-database-cache-smoke.sh
```

The scenario exercises the refresher lifecycle (`DatabaseCacheRefresher`) against a running proxy with `hms-proxy-db-cache.properties` (`hms-proxy-db-cache-kerberos.properties` on the Kerberos profile):
- Creates isolated test database `db_cache_smoke_test`.
- Executes warmup `get_all_databases` and `get_database` calls, priming the caches and establishing access timestamps.
- Verifies background polling by asserting that `hms_proxy_cache_refreshes_total` increments for both `database_list` and `database_metadata` within the active window.
- Pauses requests past the activity window (`activity-window-ms=10000`) and asserts idle sleep: refresh metrics cease incrementing.
- Sends fresh client requests and verifies awakening: background refreshes resume.

## Unprefixed databases and live configuration reload (`run-unprefixed-db-and-reload-smoke.sh`)

Validates hard mapping of remote databases into the root federated namespace (`catalog.<name>.unprefixed-databases`), shadowing of identically named databases in the default catalog, and live configuration reload (Safe Reload) via `SIGHUP`:

```bash
cd smoke-stand && ./prepare.sh
./run-unprefixed-db-and-reload-smoke.sh
```

The scenario exercises:
- Creating test databases with identical names in default catalog (`hdp`) and remote catalog (`apache`).
- Dynamically applying `catalog.apache.unprefixed-databases` via `SIGHUP` without restarting the service (`docker kill --signal=HUP stand-proxy`).
- Verifying `get_all_databases`: the database appears without prefix (`smoke_unprefixed_db`), and the prefixed entry `apache__smoke_unprefixed_db` is hidden.
- Verifying shadowing: requests targeting `smoke_unprefixed_db` route to the remote catalog `apache`, while tables from the default catalog's database are shadowed.
- Creating and dropping a table via DDL inside the unprefixed database.
- Verifying Safe Reload: injecting an invalid configuration property and sending `SIGHUP` does not crash the service (the proxy responds on `/readyz` and continues serving requests on the previous valid configuration).
- Reverting the configuration and confirming un-shadowing after `SIGHUP`.

## Soft remote catalog merging and live configuration reload (`run-merged-databases-smoke.sh`)

Validates dynamic soft merging of remote catalog schemas with the default catalog (`catalog.<name>.merge-with-default=true`) without static schema lists and without shadowing:

```bash
cd smoke-stand && ./prepare.sh
./run-merged-databases-smoke.sh
```

The scenario exercises:
- Creating a unique database in the remote catalog (`apache__smoke_merged_unique_db`) and colliding databases (`smoke_collision_db`) in both catalogs.
- Dynamically enabling `catalog.apache.merge-with-default=true` via `SIGHUP` without restarting the service.
- `get_all_databases`: the unique remote database is exposed to clients without a prefix (`smoke_merged_unique_db`), while canonical prefixed access is retained via `get_database`.
- No shadowing: the colliding default catalog database remains unprefixed, while the identically named remote database remains accessible via its explicit prefix (`apache__smoke_collision_db`).
- Routing of table calls (`get_all_tables`) for both unique and colliding schemas.
- Database creation without explicit prefix (`create_database`): the newly created database is routed to the default catalog (`hdp`), not the remote one.
- Safe Reload: an attempt to apply an invalid configuration is rejected while the proxy continues serving traffic on the last valid configuration.
- Restoring initial configuration and returning the unique database to prefixed mode after a subsequent `SIGHUP`.

## Converted schema patterns for DBeaver / Hue (`run-schema-pattern-smoke.sh`)

Validates resolution of converted schema patterns passed by JDBC GUI clients (DBeaver, Hue, DataGrip) via HiveServer2:

```bash
cd smoke-stand && ./prepare.sh
./run-schema-pattern-smoke.sh
```

The scenario exercises:
- `get_databases` with patterns `hdp..*` and `apache..*`, verifying routing and externalization (`hdp__default`, `apache__default`).
- `get_table_meta` with patterns `hdp..*` and `apache..default`, asserting that table metadata is fetched from the targeted remote backend.
- Negative checks (queries against non-existent catalogs like `nonexistent..*` return no matches).

## Partitioned transactional table insert (`run-news-txn-smoke.sh`)

Exercises end-to-end `INSERT INTO ... PARTITION (...) SELECT ... FROM ...` writes into partitioned ACID ORC tables from a staging table:

```bash
cd smoke-stand
./run-news-txn-smoke.sh
```

The scenario:
- Creates a database and staging table `news_crawler_stg`, populated with sample records.
- Creates a partitioned transactional ORC table `news` (`PARTITIONED BY (source string) CLUSTERED BY (id) INTO 2 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')`).
- Executes `INSERT INTO ... PARTITION (source = 'crawler') SELECT ... FROM news_crawler_stg` evaluating SQL expressions (`CAST`, `COALESCE`, `parse_url`, `NULL`).
- Asserts that the inserted row is read back through the target partition and cleans up test metadata. The query results cache is off (`hive.query.results.cache.enabled=false`): the table is recreated with the same write id, and with the cache on the verification query returned the previous run's rows.

## Table/partition rename and partition exchange (`run-partition-and-rename-smoke.sh`)

Validates mutation routing, federation, and path normalization across catalogs:

```bash
cd smoke-stand
./run-partition-and-rename-smoke.sh
```

The scenario validates:
- Intra-catalog table renames and rejection of cross-catalog table renames (`MetaException: Cannot rename/move table across different catalogs`). That the old name is gone is checked by expecting `NoSuchObjectException` from `get_table`.
- Table renaming inside a federated (non-default) catalog (`apache__default`).
- Intra-catalog partition exchange (`EXCHANGE PARTITION`) and rejection of cross-catalog partition exchange (`MetaException: Cannot exchange partitions across different catalogs`).
- Partition renaming inside a federated catalog (`apache__default`) with correct namespace translation.
- External partition location rewriting and normalization (`ExternalTableLocationRewriter`) during partition additions (`add_partition`) and modifications (`alter_partition`).

Before the checks the scenario waits for both namenodes to leave safe mode (`hdfs dfsadmin -safemode wait`, after a `kinit` with the namenode keytab under Kerberos): right after the stand is recreated the first `create_table` otherwise fails with `SafeModeException`.

## Cross-DC and WAN resilience (`run-cross-dc-resilience-smoke.sh`)

Exercises the proxy's failover mechanisms for linked remote datacenters and degraded WAN links:

```bash
cd smoke-stand && ./prepare.sh
./run-cross-dc-resilience-smoke.sh
```

The scenario tests on a live proxy configured with `hms-proxy-cross-dc.properties` (`hms-proxy-cross-dc-kerberos.properties` on the Kerberos profile):
- Table and partition metadata caches (`TableMetadataCache`, `PartitionMetadataCache`): validates cache misses and hits on consecutive `get_table` and `get_partition` calls via Prometheus `hms_proxy_cache_requests_total` metrics.
- Automatic DDL invalidation: confirms cache invalidation for tables and partitions upon `alter_table` mutations.
- Serve-Stale-on-Error: verifies serving expired metadata (`stale_hit`) during simulated remote catalog outages (`docker stop stand-hms-apache`) without throwing client errors.
- Granular `/readyz`: validates HTTP `200 OK` readiness when the secondary remote catalog is down with `required-for-readiness=false` (prevents local load balancers from taking proxy out of rotation).
- A hung rather than stopped remote catalog (step 4b): `docker pause stand-hms-apache` for 20 seconds; `/readyz` must answer 200 the whole time, and at least one sample must show `apache` as `connected=false`; then the pause is lifted. `docker stop` cannot test this: a stopped backend fails fast (`Connection refused`, later `UnknownHostException`), so the probe never hangs. The cross-DC configs declare `catalogs=apache,hdp` on purpose: probes run in declared order, and only this way the hung `apache` probe is awaited before the `hdp` one. Measured on the Kerberos stand: a build without the `isDone()` check in `BackendProbeRunner` fails the step in 3 of 3 runs, the current code passes 3 of 3.
- Catalog Shadow / Replica Fallback: fallback of read-only RPCs (`get_table`) to a local shadow replica (`hdp`) during outages with `hms_proxy_catalog_fallback_total` increment.
- Split-Brain protection: `create_table` against the failed `apache` catalog does not go to the replica. The scenario checks three things: the call fails with a `MetaException` whose text names the `apache` catalog (`Backend catalog 'apache' failed in method ...`, a failed connect included, which keeps `Could not connect to meta store ... hms-apache` inside; `Unable to open backend metastore client for catalog apache ...` when an impersonation session cannot be opened under Kerberos); the `hms_proxy_catalog_fallback_total` counter for `create_table` does not grow; the table does not exist on the `hdp` replica (`NoSuchObjectException`). The failure alone proves nothing: without the protection the write would fail too, on the stopped metastore.
- Lenient Startup: verifies proxy startup and readiness when the secondary remote metastore is offline. The proxy is recreated with `--no-deps` and `WAIT_FOR_APACHE=false` (passed into the container through `docker-compose.yml`), and the scenario checks that `stand-hms-apache` is still stopped after the restart. Compose used to start it again through `depends_on`, and `WAIT_FOR_APACHE` never reached the container, so the step tested a start against a running metastore.
- Recovery: brings the secondary metastore back online and verifies that normal routing resumes.

## Spark SQL placeholders and path-based tables (`run-spark-path-based-smoke.sh`)

Checks the proxy's answers to Spark SQL service names: file-datasource format pseudo-databases, path-based tables such as `csv."hdfs://..."`, and the `global_temp` system database. Calls go through `HmsMetastoreSmokeCli metadata` as user `admin`: inside the `stand-proxy` container against `thrift://localhost:9083` when it is running, otherwise from the host against `${PROXY_HOST}:${PROXY_PORT}` (default `127.0.0.1:19085`) using the fat jar.

```bash
cd smoke-stand
./run-spark-path-based-smoke.sh
```

Checks in order (the path in them is `hdfs://titanium/warehouse/tablespace/external/hive/amdocs_arena_lz.db/csvlogs/tasks/`; it does not point at the stand's HDFS):

1. `get_database` for `csv`, `parquet`, `orc`, `json`, `delta`, `iceberg` returns `NoSuchObjectException`, not `Access denied`.
2. `get_database` for `csv."<path>"` returns `NoSuchObjectException`.
3. `get_table` with database `csv` and the path as the table name returns `NoSuchObjectException`.
4. The same with the path in double quotes.
5. `get_table` with database `default` and table `csv."<path>"` returns `NoSuchObjectException`.
6. `get_tables` for database `csv` returns an empty list.
7. `get_table_meta` with pattern `csv` returns an empty list.
8. `get_database("global_temp")` returns `NoSuchObjectException`; `get_tables` for `global_temp` returns an empty list.

The scenario does not check that the backend metastore is never called: it looks only at the reply to the client.

## MapReduce under Kerberos

Two things are needed before a kerberized `INSERT` can run, and `LocalJobRunner` hides both behind
`return code 2`:

- A delegation-token renewer. MapReduce collects HDFS tokens before starting a job and names a
  renewer for them; with none configured it fails with `Can't get Master Kerberos principal for use
  as renewer`. There is no ResourceManager here, so `yarn.resourcemanager.principal` points at
  HiveServer2 itself. It must be in `core-site.xml` — `hive-site.xml` does not reach the job's
  `Configuration`.
- Hadoop's native libraries. A secure shuffle goes through `SecureIOUtils`, which refuses to
  run without them (`Secure IO is not possible without native code extensions`). The Maven-resolved
  classpath carries Java classes only, so the image copies `lib/native` from the matching Hadoop
  distribution image.

Those libraries are built for x86_64 only. On an arm64 host (Apple Silicon) they cannot load, so
the HiveServer2 service runs emulated: `HS2_PLATFORM=linux/amd64` in `.env.kerberos`. Emulation
makes it noticeably slower, and it is only needed for the Kerberos profile — the plain profile has
no secure shuffle and runs natively.

## Known issues

- Derby must create its own database directory, so the volume mounts one level above it
  (`/opt/hms/db`), never on `metastore_db` itself.
- The compose network is named explicitly: the default name contains an underscore, and
  `HiveMetaStoreClient` rejects a metastore URI whose hostname has one.
- The vendored standalone jars carry no schema `.sql`, so ACID tables are created programmatically
  by `InitSchema` (`TxnDbUtil.prepDb`); DataNucleus auto-creates the rest on first use.
- `HiveMetaStoreClient` in the Hortonworks jars builds its `URI[]` through
  `Arrays.asList(...).toArray()`, which returns `Object[]` on JDK 9+ and throws a
  `ClassCastException` inside `resolveUris`. That branch runs only for the default
  `RANDOM` URI selection, so the smoke CLI pins `metastore.thrift.uri.selection=SEQUENTIAL`.
- The metastore needs `metastore.expression.proxy` and `metastore.task.threads.always` overridden:
  their defaults name classes that live in a full Hive distribution, not in the standalone jar. The
  `DefaultPartitionExpressionProxy` stub then throws `UnsupportedOperationException` on a plain
  `WHERE p='...'` against a partitioned table, so the real `PartitionExpressionForMetastore` comes
  back together with `hive-exec`.
- Hadoop reads `hadoop.security.authentication` from `core-site.xml` on the classpath, not from
  `-D` system properties — without the file the server tries to use the OS user as a principal.
- The metastore runs a `TUGIBasedProcessor`, which refuses a second `set_ugi` on one connection,
  so the proxy's backend clients set `hive.metastore.execute.setugi=false`.
- HiveServer2 polls the notification log at startup and refuses to start unless its scratch dir is
  world-writable; the backends therefore set `metastore.event.db.notification.api.auth=false`.
- The `bde2020` Hadoop images take `CORE_CONF_<key>` / `HDFS_CONF_<key>` variables (dots as
  underscores, dashes as three underscores) — not the `<FILE>.XML_<key>` form used by
  `apache/hadoop`. Mixing them leaves `fs.defaultFS` unset and the datanode looks for a namenode
  at its own hostname.
- HDFS is pinned to 3.1.3 to match the Hive 3.1.3 clients. `hive-service` also drags Hadoop 2.7.1
  next to the 3.1.0 jars, and the two cannot share a classpath: `DFSClient` from 2.7.1 wants
  `SpanReceiverHost`, which Hadoop 3 removed, and HiveServer2 dies before opening a port.
- The Kerberos short name of every service principal must exist as an OS user in the image,
  otherwise Hadoop's group lookup fails with `no such user` and HiveServer2 never finishes
  starting.
- Secure Hadoop wants `yarn.resourcemanager.principal` as the delegation-token renewer even with
  local MapReduce and no YARN; without it every statement fails with `Can't get Master Kerberos
  principal for use as renewer` *after* the SASL handshake has already succeeded.
- A secure DataNode may only skip privileged ports when SASL data transfer protection is on *and*
  the web policy is `HTTPS_ONLY`; anything else aborts with `Cannot start secure DataNode due to
  incorrect config`. `HTTPS_ONLY` in turn needs a keystore, hence `hdfs/keystore.jks` and
  `hdfs/truststore.jks` in the tree. They hold a self-signed certificate for a throwaway local
  stand, and their password sits in plain sight in `hdfs/ssl-server.xml` — nothing here guards
  anything, so do not reuse them anywhere. The certificate expires in 2036; to reissue it (note
  `-storetype JKS`: Java 9+ `keytool` writes PKCS12 by default, and Hadoop then reports
  `Invalid keystore format`):

  ```bash
  keytool -genkeypair -alias hdfs-stand -keyalg RSA -keysize 2048 -validity 3650 \
    -dname "CN=hdfs-stand, OU=smoke, O=stand, L=local, ST=local, C=US" \
    -keystore hdfs/keystore.jks -storetype JKS -storepass smokepass -keypass smokepass
  keytool -exportcert -alias hdfs-stand -keystore hdfs/keystore.jks -storepass smokepass \
    | keytool -importcert -alias hdfs-stand -keystore hdfs/truststore.jks -storetype JKS \
      -storepass smokepass -noprompt
  ```

## Kerberos and HiveServer2

With `--env-file .env.kerberos` the whole chain is authenticated: the client holds a TGT for
`smoke-user@SMOKE.LOCAL`, HiveServer2 runs as `hive/hs2@SMOKE.LOCAL`, the proxy as
`hive/proxy@SMOKE.LOCAL`, and each metastore as `hive/hms-*@SMOKE.LOCAL`. Beeline then connects
with the service principal in the URL:

```bash
docker exec stand-hs2 kinit -kt /keytabs/smoke-user.keytab smoke-user@SMOKE.LOCAL
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://hs2:10000/default;principal=hive/hs2@SMOKE.LOCAL' --silent=true --outputformat=tsv2 \
  -e 'show databases;'"
```

## Defects found on the stand

- `INSERT ... VALUES` broke in every catalog: Hive sends a `LockRequest` whose first component is
  the `_dummy_database`/`_dummy_table` placeholder, and the multi-namespace check counted it as a
  second catalog. Unit tests never saw this shape.
- Once the placeholder stopped blocking the request, a second failure surfaced underneath it: an
  `INSERT` into a non-default catalog opened its transaction against the default catalog's
  TxnHandler but sent the lock to the catalog's own backend, which answered `NoSuchTxnException`.
  Write locks for non-default catalogs are now served by the shim.
- A query joining two catalogs failed outright with `Error in acquiring locks`: Hive locks every
  table of a statement in one request, and any request naming more than one namespace was rejected.
  The same check also refused a join across two databases of a *single* catalog. Lock requests are
  now split by catalog. Only a real SQL client shows this — the direct smoke CLI issues one
  namespace per lock request and never produced the shape.
- The readiness probe no longer disturbs SASL: 15 `/readyz` scrapes followed by a Kerberos smoke
  run pass, where the old probe would have rewritten the process-wide UGI configuration.
- The transactional-DDL guard fires on `create_table_with_environment_context`, the RPC Beeline
  actually sends — the method the guard did not cover before.
- External-table purge deletes real HDFS data for `APACHE_3_1_3` catalogs, off the request thread.
- The Hortonworks front door now answers a real HDP HiveServer2: federation, DDL, ACID writes and a
  cross-catalog join all pass, and `add_write_notification_log` arrives from Hive itself. Until the
  vendor distribution was available, that listener had only ever been exercised by the smoke CLI.
- With the catalogs split across two HDFS clusters, external-table location rewriting stopped being
  a unit-test-only feature: an unqualified `LOCATION` and one naming the other cluster both land on
  the filesystem of the catalog that owns the table, and a single MapReduce job reads from both.
- Purge across clusters needs the *catalog's own* namenode principal in the proxy config. The purger
  opens that filesystem itself, so with only the first cluster's principal configured the delete died
  with `Failed to specify server's Kerberos principal name` — after the drop had already
  succeeded, leaving the data orphaned. Hence `catalog.<name>.conf.dfs.namenode.kerberos.principal`
  per catalog in the Kerberos profile.

## End-to-end Kerberos check

Kerberos, from a client ticket down to the backend:

```
smoke-user@SMOKE.LOCAL --SASL--> hive/hs2 --SASL--> hive/proxy --SASL--> hive/hms-{hdp,apache}
```

`show databases` returns `default` and `apache__default`, DDL through the proxy succeeds, and the
audit log records `"authenticatedUser":"hive"` — HiveServer2's own principal, because the stand
runs with `hive.server2.enable.doAs=false`. Turn doAs on (plus `hadoop.proxyuser.hive.*`) to
exercise end-user impersonation instead.
