# Smoke stand

A local docker-compose stand for running the `scripts/run-real-installation-smoke-*.sh` suites
against real Hive metastores instead of a production cluster.

Two standalone metastores sit behind one proxy:

| Service | Backend | Role | Host port |
| --- | --- | --- | --- |
| `hms-hdp` | Hortonworks `3.1.0.3.1.0.0-78` | default catalog — owns ACID/txn state | 19084 |
| `hms-apache` | Apache `3.1.3` | non-default catalog — synthetic lock shim, proxy-side purge | 19083 |
| `proxy` | the fat jar under test | front door | 19085 thrift, 19090 management |
| `hs2` | HiveServer2 3.1.3, points at the proxy | SQL layer: Beeline scenarios | 10000, 10002 |
| `namenode` / `datanode` | Hadoop `3.1.3` | HDFS: warehouses and external-table data | 19870 UI, 18020 |
| `kdc` | MIT Kerberos, realm `SMOKE.LOCAL` | only with `--profile kerberos` | 18848/udp |

Databases are exposed as `<catalog>__<db>`: `hdp__default`, `apache__default`. Warehouses live on
HDFS (`/warehouse/hdp`, `/warehouse/apache`), and `/external` is the allowlisted root for
external-table purge.

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

## What the stand is not

- **Not a real installation.** The metastores run from the jars vendored in `hive-metastore/`,
  without the rest of an HDP distribution: no Ranger, no Atlas, no HA. It validates protocol and
  routing behaviour, not stack integration.
- **HDFS is not kerberized.** The Kerberos profile secures the client, HiveServer2, the proxy and
  both metastores, but the namenode and datanode stay on simple auth, so the kerberized services
  set `ipc.client.fallback-to-simple-auth-allowed=true`. Securing HDFS (nn/dn keytabs, SASL data
  transfer) is a separate piece of work.
- **No YARN/Tez.** Queries run as local MapReduce, which is enough for DDL, reads and small
  writes, but says nothing about distributed execution. Note that MapReduce jobs fail under the
  Kerberos profile (`LocalJobRunner` against the unsecured HDFS); metadata, locks and DDL work
  there, but verify write paths in the plain profile.

## Notes that cost time to find

- Derby must create its own database directory, so the volume mounts one level above it
  (`/opt/hms/db`), never on `metastore_db` itself.
- The compose network is named explicitly: the default name contains an underscore, and
  `HiveMetaStoreClient` rejects a metastore URI whose hostname has one.
- The vendored standalone jars carry no schema `.sql`, so ACID tables are created programmatically
  by `InitSchema` (`TxnDbUtil.prepDb`); DataNucleus auto-creates the rest on first use.
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
- The readiness probe no longer disturbs SASL: 15 `/readyz` scrapes followed by a Kerberos smoke
  run pass, where the old probe would have rewritten the process-wide UGI configuration.
- The transactional-DDL guard fires on `create_table_with_environment_context`, the RPC Beeline
  actually sends — the method the guard did not cover before.
- External-table purge deletes real HDFS data for `APACHE_3_1_3` catalogs, off the request thread.

## Verified end to end

Kerberos, from a client ticket down to the backend:

```
smoke-user@SMOKE.LOCAL --SASL--> hive/hs2 --SASL--> hive/proxy --SASL--> hive/hms-{hdp,apache}
```

`show databases` returns `default` and `apache__default`, DDL through the proxy succeeds, and the
audit log records `"authenticatedUser":"hive"` — HiveServer2's own principal, because the stand
runs with `hive.server2.enable.doAs=false`. Turn doAs on (plus `hadoop.proxyuser.hive.*`) to
exercise end-user impersonation instead.
