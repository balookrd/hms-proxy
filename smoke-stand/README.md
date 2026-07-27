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
live on **different HDFS clusters** (see below), each with its own `/warehouse` and an `/external`
root allowlisted for external-table purge.

What has actually been run here — and what has not — is recorded in
[TEST-MATRIX.md](TEST-MATRIX.md).

## Run it

```bash
# once per proxy build: stage jars and the metastore classpath
./prepare.sh

# layer 1 — no Kerberos
docker compose up -d --build

# the repo's own runner, against the stand
JAVA_HOME=/Users/mvmalykh/Library/Java/JavaVirtualMachines/liberica-17.0.19 \
  ../scripts/run-real-installation-smoke-simple.sh --env-file env/simple.env --scenario all
```

```bash
# layer 2 — Kerberos, including HiveServer2. Always pass --env-file: compose recreates
# dependencies reached through depends_on, and a call without these variables restarts the
# proxy without SASL, which hangs the Kerberos HiveServer2 in its handshake.
docker compose --env-file .env.kerberos --profile kerberos up -d --build
```

A fresh HDFS needs its directories once:

```bash
docker exec stand-namenode bash -c \
  'hdfs dfs -mkdir -p /warehouse/apache /warehouse/hdp /external && hdfs dfs -chmod -R 1777 /warehouse /external'
```

The notification scenario needs its table to exist on the HDP backend — `add_write_notification_log`
resolves the table before writing the log entry:

```bash
docker exec stand-hs2 bash -c "java -cp '/opt/hs2/conf:/opt/hs2/lib/*' org.apache.hive.beeline.BeeLine \
  -u 'jdbc:hive2://localhost:10000/default' -n hive --silent=true \
  -e 'create table if not exists smoke_txn_tbl (id int) stored as orc;'"
```

The Kerberos smoke has to run **inside** the compose network, because the KDC and the service
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

To exercise the guard, uncomment `guard.transactional-ddl.mode=REJECT_TRANSACTIONAL` in
`proxy/hms-proxy.properties`, restart the proxy, and create a transactional table: the proxy must
reject it by name.

## External-table purge

The proxy deletes external-table data itself only for catalogs on the `APACHE_3_1_3` runtime
profile — a Hortonworks backend does that on its own, so `enabledFor` skips it. Check it against
the `apache` catalog, under the allowlisted `/external` root:

```bash
docker exec stand-namenode bash -c \
  'hdfs dfs -mkdir -p /external/purge_me && echo "7,delta" | hdfs dfs -put -f - /external/purge_me/data.csv'
```

Then create an external table on that location with `'external.table.purge'='true'` in
`apache__default`, drop it, and the proxy logs:

```
FileSystemExternalTableDropPurger: purged external table data for catalog 'apache'
  at location 'hdfs://namenode:8020/external/purge_me'
```

The deletion runs on the `hms-proxy-drop-purge-*` pool, off the request thread.

## Two HDFS clusters

The catalogs sit on **different filesystems**, which is what makes the proxy's cross-filesystem
behaviour testable at all:

| Catalog | Metastore | Filesystem | Host ports |
| --- | --- | --- | --- |
| `hdp` (default) | `hms-hdp` | `hdfs://namenode:8020` | 19870 / 18020 |
| `apache` | `hms-apache` | `hdfs://namenode-b:8020` | 19871 / 18021 |

Both clusters run **Apache** Hadoop 3.1.3, whatever the catalog on top of them is called: the names
`hdp` and `apache` describe the metastore runtime the proxy federates, not the storage. The vendor
HDP distribution enters the stand only as a *client* — the `hs2-hdp` service — and never as a
filesystem.

Both clusters share one Kerberos realm on purpose: a client holding a single TGT can then reach
either one, which is what allows a single query to read across them. Cross-realm trust would test
the KDC rather than the proxy.

Two settings carry the weight:

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

## What the stand is not

- **Not a real installation.** The metastores run from the jars vendored in `hive-metastore/`,
  without the rest of an HDP distribution: no Ranger, no Atlas, no HA. It validates protocol and
  routing behaviour, not stack integration.
- **No Ranger/Atlas/HA**, as above — the Kerberos profile itself is complete: the client,
  HiveServer2, the proxy, both metastores and HDFS (namenode and datanode keytabs, SASL data
  transfer, SPNEGO) all authenticate, and no service falls back to simple auth.
- **No YARN/Tez.** Queries run as local MapReduce, which is enough for DDL, reads and small
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
`proxy/hms-proxy.properties`, host port 19183). It is read-only and serves the default
catalog (`hdp`) only. `--scenario rest` (or the REST step of `--scenario all`) drives it
with curl from the host: config discovery, namespace and table listings, a table load, and
the negative shapes — unknown prefix, unknown table, and a write route, all of which must
fail cleanly.

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

The Kerberos profile leaves the REST listener off: SPNEGO needs a GSS-enabled curl inside
the network, and the handshake itself is already covered end-to-end by
`SpnegoIntegrationTest` on hadoop-minikdc.

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

- `add_write_notification_log` sent by **Hive itself** after an ACID write, not synthesized by the
  smoke CLI — with real delta paths and checksums.
- Transactional tables. The standalone metastores could not create them (`The table must be stored
  using an ACID compliant format`), because `TransactionalValidationListener` needs `OrcOutputFormat`
  from `hive-exec` and that class in turn needs `org.apache.hadoop.mapred.InputFormat` from
  `hadoop-mapreduce-client-core`. `prepare.sh` now stages both next to each metastore — Apache jars
  for the Apache one, vendor jars for the Hortonworks one — and the entrypoint appends them **after**
  the jar under test, so `hive-exec`'s own copy of the metastore classes can never shadow it.

Two things this profile does not reproduce faithfully:

- **The execution engine.** Hortonworks builds without MapReduce, and the check fires in two places
  with two different messages. `HiveConf.initialize()` runs `validateExecutionEngine`, so naming
  `mr` in `hive-site.xml` stops the server from starting (`mr execution engine is not supported!`);
  the config therefore keeps the vendor default of Tez, and clients switch per session with
  `set hive.execution.engine=mr;` — which the SQL smoke does through
  `HMS_SMOKE_SQL_HDP_SESSION_INIT`. That `set` is validated too (`hive execution engine mr is not
  supported.`), and passing it is the single thing `hive.in.test=true` buys here. Tez itself is not
  an option: it needs a ResourceManager and the Tez tarball in HDFS, and this distribution ships
  neither. The *metadata* path — every RPC the proxy actually serves — is unaffected; only query
  execution differs from a real HDP cluster.
- **Emulation.** The distribution's native libraries are x86_64 only, so on Apple Silicon the whole
  service runs under `linux/amd64` and is noticeably slow to start (allow a couple of minutes).

## MapReduce under Kerberos

Two things are needed before a kerberized `INSERT` can run, and `LocalJobRunner` hides both behind
`return code 2`:

- **A delegation-token renewer.** MapReduce collects HDFS tokens before starting a job and names a
  renewer for them; with none configured it fails with `Can't get Master Kerberos principal for use
  as renewer`. There is no ResourceManager here, so `yarn.resourcemanager.principal` points at
  HiveServer2 itself. It must be in `core-site.xml` — `hive-site.xml` does not reach the job's
  `Configuration`.
- **Hadoop's native libraries.** A secure shuffle goes through `SecureIOUtils`, which refuses to
  run without them (`Secure IO is not possible without native code extensions`). The Maven-resolved
  classpath carries Java classes only, so the image copies `lib/native` from the matching Hadoop
  distribution image.

Those libraries are built for x86_64 only. On an arm64 host (Apple Silicon) they cannot load, so
the HiveServer2 service runs emulated: `HS2_PLATFORM=linux/amd64` in `.env.kerberos`. Emulation
makes it noticeably slower, and it is only needed for the Kerberos profile — the plain profile has
no secure shuffle and runs natively.

## Notes that cost time to find

- Derby must create its own database directory, so the volume mounts one level above it
  (`/opt/hms/db`), never on `metastore_db` itself.
- The compose network is named explicitly: the default name contains an underscore, and
  `HiveMetaStoreClient` rejects a metastore URI whose hostname has one.
- The vendored standalone jars carry no schema `.sql`, so ACID tables are created programmatically
  by `InitSchema` (`TxnDbUtil.prepDb`); DataNucleus auto-creates the rest on first use.
- `CREATE TABLE ... TBLPROPERTIES('transactional'='true')` fails with "The table must be stored
  using an ACID compliant format": the standalone metastore has no `hive-exec`, so its
  transactional validation cannot load `OrcOutputFormat` and rejects the format it just got.
  Tables for ACID-adjacent smoke steps are therefore plain ORC — `add_write_notification_log` only
  needs the table to exist, not to be transactional.
- `HiveMetaStoreClient` in the Hortonworks jars builds its `URI[]` through
  `Arrays.asList(...).toArray()`, which returns `Object[]` on JDK 9+ and throws a
  `ClassCastException` inside `resolveUris`. That branch runs only for the default
  `RANDOM` URI selection, so the smoke CLI pins `metastore.thrift.uri.selection=SEQUENTIAL`.
- The metastore needs `metastore.expression.proxy` and `metastore.task.threads.always` overridden:
  their defaults name classes that live in a full Hive distribution, not in the standalone jar.
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

## What the stand has already caught

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
  with `Failed to specify server's Kerberos principal name` — **after** the drop had already
  succeeded, leaving the data orphaned. Hence `catalog.<name>.conf.dfs.namenode.kerberos.principal`
  per catalog in the Kerberos profile.

## Verified end to end

Kerberos, from a client ticket down to the backend:

```
smoke-user@SMOKE.LOCAL --SASL--> hive/hs2 --SASL--> hive/proxy --SASL--> hive/hms-{hdp,apache}
```

`show databases` returns `default` and `apache__default`, DDL through the proxy succeeds, and the
audit log records `"authenticatedUser":"hive"` — HiveServer2's own principal, because the stand
runs with `hive.server2.enable.doAs=false`. Turn doAs on (plus `hadoop.proxyuser.hive.*`) to
exercise end-user impersonation instead.
