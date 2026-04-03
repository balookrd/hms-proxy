# HMS Proxy

[![CI](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml)

Russian documentation: [README.ru.md](README.ru.md), [SMOKE.ru.md](SMOKE.ru.md)

Java proxy for Hive Metastore that exposes one external HMS Thrift endpoint and routes
requests to multiple backend metastores by catalog.

## Primary scenarios

### 1. Multi-catalog federation

Use one production-facing HMS Thrift endpoint for HiveServer2 and direct HMS API clients, while
routing requests to multiple backend metastores by explicit `catName` or by legacy
`catalog<separator>db` database names.

### 2. Apache ↔ HDP compatibility bridge

Expose an Apache Hive Metastore `3.1.3` front door, downgrade selected calls for older
Hortonworks `3.1.0.x` backends, and optionally present the proxy itself as a Hortonworks
front door through an HDP `standalone-metastore` jar.

### 3. Kerberized proxy for HS2/HMS

Run the proxy as the security boundary between clients and backend metastores with optional
Kerberos/SASL on the front door, optional outbound Kerberos to backends, and optional
impersonation of the authenticated Kerberos caller.

## RPC behavior matrix

| RPC group | Status | Behavior |
| --- | --- | --- |
| Catalog-aware metadata reads and writes with explicit namespace (`catName`, `dbName`, `fullTableName`) | supported | Routed to the resolved catalog/backend normally. |
| Legacy reads and writes using `catalog<separator>db` database names | supported | Routed by externalized database name; table/database objects are rewritten on the way back. |
| Apache `3.1.3` wrapper RPCs against older Hortonworks `3.1.0.x` backends | degraded | Proxy retries selected `*_req` APIs through older legacy methods such as `get_table`. |
| Session-level and global read-only RPCs without catalog context | degraded | Routed to `routing.default-catalog`. |
| Read-only service APIs missing on a backend (`TApplicationException` on notifications, privilege refresh/introspection, token/key listings except delegation-token issuance, txn/lock/compaction status) | degraded | Proxy returns an empty compatibility response instead of failing the caller. |
| ACID / txn / lock lifecycle RPCs without routable namespace (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`) | degraded | Pinned to `routing.default-catalog`; validate carefully before using in multi-catalog mode. |
| Global write operations without clear catalog context | rejected | Proxy fails the request when more than one catalog is configured. |
| Catalog registry management (`create_catalog`, `drop_catalog`) | rejected | Catalog definitions are managed in proxy config, not through client RPCs. |
| HDP-only front-door methods with an explicit Apache bridge mapping | supported | Proxy adapts selected Hortonworks request-wrapper methods to Apache equivalents. |
| HDP-only methods that require a matching Hortonworks runtime (`add_write_notification_log`, `get_tables_ext`, `get_all_materialized_view_objects_for_rewriting`) | passthrough | Forwarded only to compatible Hortonworks backends/front doors when configured. |
| HDP-only methods without a safe Apache mapping | rejected | Proxy fails explicitly instead of returning a misleading success response. |

## Public compatibility matrix

This matrix is the public contract for how to think about the proxy. It is intentionally grouped by
client shape, advertised front door, backend runtime, auth mode, and method family rather than by
individual thrift method names.

| Client version | Front-door profile | Backend profile | Auth mode | Method families | Expected result |
| --- | --- | --- | --- | --- | --- |
| Apache Hive / Spark clients that speak Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | `APACHE_3_1_3` | `NONE` or `KERBEROS` | catalog-aware reads/writes, legacy `catalog<separator>db` routing, view rewrite | Supported as the baseline deployment. |
| Apache Hive / Spark clients that speak Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | read paths and selected metadata writes that can fall back from `*_req` APIs | Supported with compatibility downgrade; some calls are degraded to legacy RPCs. |
| Hortonworks clients that only expect a Hortonworks front-door identity via `getVersion()` | `HORTONWORKS_*` without standalone jar | `APACHE_3_1_3` or Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | overlapping Apache/HDP method families | Supported when changing the advertised profile is enough for the client. |
| Hortonworks clients that call HDP-only thrift request-wrapper methods | `HORTONWORKS_*` with standalone jar | Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | mapped HDP-only methods, runtime-specific passthrough methods | Supported when the matching Hortonworks front-door and backend runtime jars are configured. |
| Hortonworks clients that call HDP-only thrift request-wrapper methods | `HORTONWORKS_*` with standalone jar | `APACHE_3_1_3` | `NONE` or `KERBEROS` | HDP-only passthrough methods such as `add_write_notification_log` | Rejected explicitly when the target backend does not provide a compatible Hortonworks runtime. |
| HiveServer2 / Beeline SQL workloads across multiple catalogs | `APACHE_3_1_3` or `HORTONWORKS_*` | mixed Apache + Hortonworks backends | `NONE` or `KERBEROS` | reads, DDL/DML, namespace rewrite, optional view rewrite | Supported as long as routing can resolve the target catalog. |
| HiveServer2 / direct HMS clients using txn/lock lifecycle RPCs without namespace in the payload | any | mixed Apache + Hortonworks backends | `NONE` or `KERBEROS` | `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat` | Degraded: pinned to `routing.default-catalog`; treat this as a single-catalog control plane unless you validated otherwise. |
| Kerberized HiveServer2 / HMS clients that require end-user identity on the backend | any | any | `KERBEROS` with optional impersonation | front-door SASL, local delegation-token issuance, backend `set_ugi()` impersonation | Supported when proxy-user rules and backend impersonation permissions are configured correctly. |
| Clients attempting global writes without a resolvable catalog or dynamic catalog registry management | any | any | `NONE` or `KERBEROS` | ambiguous writes, `create_catalog`, `drop_catalog` | Rejected by design. |

## Scope notes

- legacy database references without a catalog prefix are routed to `routing.default-catalog`
- this keeps Spark/Hive compatibility while still avoiding ambiguous metadata writes
- in practice this means ACID write lifecycle is supported only for the default catalog unless the
  request payload itself carries routable namespace information such as `dbName` or `fullTableName`
- Hive ACID, locks, tokens, and other truly global metastore operations still need careful
  validation in your environment before turning them on behind a multi-catalog proxy

## Build

```bash
mvn -o test
mvn -o package
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
```

Build version is computed from git for every commit in the form `0.1.<git-distance>-<short-sha>`.

## Run

```bash
java -jar "target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar" /etc/hms-proxy/hms-proxy.properties
```

`mvn package` produces both a regular jar and a runnable fat jar with classifier `fat`.
The fat jar file name changes with every new commit.

For Java 17+ with Hadoop 2.x Kerberos libraries, start with:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -jar "target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar" /etc/hms-proxy/hms-proxy.properties
```

## Observability

### Management listener

The proxy can expose a lightweight HTTP listener for health checks, readiness, and Prometheus
metrics. The listener is disabled by default and turns on automatically when `management.port`
is configured:

```properties
management.bind-host=0.0.0.0
management.port=19083
```

You can also enable it explicitly:

```properties
management.enabled=true
# Optional; defaults to server.bind-host
management.bind-host=0.0.0.0
# Optional; defaults to server.port + 1000
management.port=19083
```

Quick checks:

```bash
curl -s http://127.0.0.1:19083/healthz
curl -s http://127.0.0.1:19083/readyz
curl -s http://127.0.0.1:19083/metrics
```

### Health and readiness endpoints

Available endpoints:

- `/healthz` returns process liveness and uptime
- `/readyz` checks backend connectivity, returns per-backend `connected` / `degraded` state,
  and includes Kerberos login status plus TGT freshness for front-door and outbound backend credentials
- `/metrics` exposes Prometheus text format metrics

`/healthz` is intended for simple liveness checks and only answers whether the proxy process is up.

`/readyz` is intended for load balancers, orchestration probes, and operational diagnostics. The
response includes:

- overall readiness status
- backend connectivity summary
- per-backend `connected`, `degraded`, `lastSuccessEpochSecond`, `lastFailureEpochSecond`,
  `lastProbeEpochSecond`, and `lastError`
- Kerberos status for the front door and outbound backend credentials
- Kerberos TGT freshness via `tgtExpiresAtEpochSecond` and `secondsUntilExpiry` when available

### Prometheus metrics

Current Prometheus metrics:

- `hms_proxy_requests_total{method,catalog,backend,status}`
- `hms_proxy_request_duration_seconds{method,catalog,backend}`
- `hms_proxy_backend_failures_total{backend,exception}`
- `hms_proxy_backend_fallback_total{method,from_api,to_api}`
- `hms_proxy_routing_ambiguous_total`
- `hms_proxy_default_catalog_routed_total{method}`

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: hms-proxy
    static_configs:
      - targets:
          - hms-proxy-01.example.com:19083
```

Metric semantics:

- `status` in `hms_proxy_requests_total` is one of `ok`, `error`, or `fallback`
- `catalog=all, backend=fanout` means a request was broadcast to multiple backends
- `hms_proxy_backend_failures_total` counts backend-side invocation failures grouped by backend and exception type
- `hms_proxy_backend_fallback_total` counts compatibility fallbacks returned after backend failures
- `hms_proxy_routing_ambiguous_total` counts requests rejected because the proxy saw conflicting namespace hints
- `hms_proxy_default_catalog_routed_total` counts requests that were routed to the default catalog because no explicit catalog namespace was present

### Structured audit log

The proxy emits one structured audit log record per request through the logger
`io.github.mmalykhin.hmsproxy.audit`. Each record is a single-line JSON object with fields such as
`requestId`, `method`, `catalog`, `backend`, `status`, `durationMs`, `remoteAddress`,
`authenticatedUser`, `routed`, `fanout`, `fallback`, and `defaultCatalogRouted`.

Example:

```json
{"event":"hms_proxy_audit","requestId":42,"method":"get_table","catalog":"catalog1","backend":"catalog1","status":"ok","durationMs":8,"routed":true,"fanout":false,"fallback":false,"defaultCatalogRouted":false,"remoteAddress":"10.20.30.40","authenticatedUser":"alice@EXAMPLE.COM"}
```

### Grafana dashboard

A ready-to-import Grafana dashboard is included in
`monitoring/grafana/hms-proxy-dashboard.json`. It covers request rate, latency, backend failures,
fallbacks, default-catalog routing, and ambiguous routing events.

## Routing model

- Catalog-aware HMS clients can send `catName=dbCatalog, dbName=sales`
- Legacy HMS clients can use database names like `catalog1.sales`
- `get_all_databases()` returns prefixed names like `catalog1.sales`
- table objects returned to legacy callers are rewritten back to external names
- if a request carries a non-proxy `catName` such as Hive's default `hive`, the proxy falls back
  to `dbName`/default-catalog routing for compatibility
- ACID requests that include routable namespace in the payload, for example
  `get_valid_write_ids`, `allocate_table_write_ids`, `compact`, `compact2`,
  `add_dynamic_partitions`, `fire_listener_event`, or `repl_tbl_writeid_state`, are routed by that
  payload
- ACID/txn/lock lifecycle requests that only carry ids, for example `open_txns`, `commit_txn`,
  `abort_txn`, `check_lock`, `unlock`, or `heartbeat`, are pinned to `routing.default-catalog`
- by default, externalized HMS objects use proxy catalog ids in `catName`/`catalogName`
- for older HiveServer2 flows, you can enable `federation.preserve-backend-catalog-name=true`
  so externalized HMS objects keep the backend catalog name such as `hive` while `dbName`
  still uses the proxy namespace like `catalog2__default`
- optional view-definition rewrite can also translate `viewExpandedText` and `viewOriginalText`
  for `VIRTUAL_VIEW` / `MATERIALIZED_VIEW` table objects:

```properties
federation.view-text-rewrite.mode=rewrite
federation.view-text-rewrite.preserve-original-text=true
```

With `mode=rewrite`, the proxy rewrites `catalog<separator>db.table` references in view SQL on the
way to the backend, and rewrites the current catalog's backend db references back to the external
namespace on the way out. If `preserve-original-text=true`, only `viewExpandedText` is rewritten
while `viewOriginalText` is left untouched.

The catalog/database separator is configurable:

```properties
# Optional, defaults to "."
routing.catalog-db-separator=__
```

With that setting, legacy names become `catalog1__sales` instead of `catalog1.sales`.

## Transactional DDL guard

You can ask the proxy to protect table creation or table alteration when the incoming table
metadata marks a managed table as transactional:

- `transactional=true`
- any non-empty `transactional_properties`

The rule applies to `create_table`, `alter_table`, and `alter_table_with_environment_context`.
It is evaluated only for `MANAGED_TABLE`. External tables are left unchanged.

Reject mode:

```properties
guard.transactional-ddl.mode=reject
```

Rewrite mode:

```properties
guard.transactional-ddl.mode=rewrite
```

In rewrite mode the proxy rewrites the incoming table to `EXTERNAL_TABLE`, adds
`external.table.purge=true`, and removes `transactional` plus `transactional_properties`.

You can also scope it to specific client IPs or CIDR ranges:

```properties
guard.transactional-ddl.mode=reject
guard.transactional-ddl.client-addresses=10.10.0.15,10.20.0.0/16,2001:db8::/64
```

If `guard.transactional-ddl.client-addresses` is set, the rule is evaluated only for matching
clients. If it is omitted, all clients are covered.

You can also choose which HMS version the proxy advertises on the front door:

```properties
compatibility.frontend-profile=APACHE_3_1_3
```

or for Hortonworks clients:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
```

or for HDP `3.1.0.3.1.5.6150-1` clients:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
```

That changes the value returned by `getVersion()` while keeping the same proxy routing logic.

For a real Hortonworks front door, point the proxy to an HDP `standalone-metastore` jar:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_0_78
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

For HDP `3.1.0.3.1.5.6150-1`, point the proxy to the matching jar:

```properties
compatibility.frontend-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
compatibility.frontend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

For isolated Hortonworks backend runtimes, you can also point the proxy to a backend jar explicitly:

```properties
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar
```

or:

```properties
compatibility.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

For Hortonworks backend runtimes the proxy forces `hive.metastore.uri.selection=SEQUENTIAL`
inside the isolated metastore client. This avoids a known HDP 3.1.0.x client bug in the
random URI selection path when `HiveMetaStoreClient` resolves backend metastore URIs.

Backend runtime is configured explicitly per catalog. If `catalog.<name>.runtime-profile` is not
set, the proxy uses `APACHE_3_1_3` for that catalog:

```properties
catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_0_78
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar

catalog.apache.runtime-profile=APACHE_3_1_3
```

For HDP `3.1.0.3.1.5.6150-1`, configure the catalog the same way:

```properties
catalog.hdp.runtime-profile=HORTONWORKS_3_1_0_3_1_5_6150_1
catalog.hdp.backend-standalone-metastore-jar=/opt/hms-proxy/hive-metastore/hive-standalone-metastore-3.1.0.3.1.5.6150-1.jar
```

With that jar present, the proxy can open the selected Hortonworks backend runtime in an isolated
classloader. Runtime choice is not autodetected from the backend server version; it follows the
configured `catalog.<name>.runtime-profile`.

For the front door, the proxy instantiates the Hortonworks thrift `Processor` in an isolated
classloader and bridges overlapping RPCs to the internal Apache `3.1.3` handler automatically.
Selected HDP-only methods are also adapted to Apache equivalents:

- `get_database_req` -> `get_database` for HDP `3.1.0.3.1.5.6150-1`
- `create_table_req` -> `create_table` / `create_table_with_environment_context` / `create_table_with_constraints`
- `truncate_table_req` -> `truncate_table`
- `alter_table_req` -> `alter_table` / `alter_table_with_environment_context`
- `alter_partitions_req` -> `alter_partitions` / `alter_partitions_with_environment_context`
- `rename_partition_req` -> `rename_partition`
- `get_partitions_by_names_req` -> `get_partitions_by_names`
- `update_table_column_statistics_req` -> `set_aggr_stats_for`
- `update_partition_column_statistics_req` -> `set_aggr_stats_for`
- `add_write_notification_log` -> direct Hortonworks passthrough only to a Hortonworks backend
- `get_tables_ext` -> direct Hortonworks passthrough only to a Hortonworks `3.1.0.3.1.5.6150-1` backend
- `get_all_materialized_view_objects_for_rewriting` -> direct Hortonworks passthrough only to a Hortonworks `3.1.0.3.1.5.6150-1` backend via `routing.default-catalog`

Some HDP-only methods still do not have a safe Apache mapping, so they remain unsupported and fail
explicitly rather than returning a misleading success response.

View/materialized-view notes:
- the proxy can rewrite SQL text only with `federation.view-text-rewrite.mode=rewrite`
- rewrite is intentionally parser-less and conservative: it targets `db.table` references, not full
  Hive SQL grammar
- cross-catalog references in inbound SQL like `catalog2__dim.table_x` are internalized for the
  backend, but outbound rewrite is only guaranteed for the current table namespace
- comments, string literals, and unusual quoted identifiers may still need manual validation in
  your environment

## Debug logging

Detailed debug tracing for the proxy package is enabled by default through the bundled
`log4j.properties` config.
Each client call gets a `requestId`, and the logs include:

- incoming HMS request method and arguments
- selected backend catalog
- proxied thrift method and rewritten arguments
- backend response or backend error
- final client response or client-visible error

If the logs are too noisy, override the level at startup, for example:

Override it with a custom `log4j.properties` if needed.
The proxy also bootstraps the bundled `log4j.properties` at runtime if no appenders are configured,
so a plain `java -jar ...` launch still gets console logs.

## HiveServer2

Point HiveServer2 `hive.metastore.uris` to this proxy instead of a single backend HMS.
For multi-catalog deployments, prefer Hive versions and clients that preserve catalog fields.
If your clients are older, use `catalog<separator>db.table` naming consistently, with
`<separator>` taken from `routing.catalog-db-separator`.

For Beeline/HS2 SQL workloads, a non-dot separator is usually easier to use than `.`.
Recommended example:

```properties
routing.catalog-db-separator=__
```

If HiveServer2 metadata writes behave differently through the proxy than directly against the
backend HMS, try enabling:

```properties
federation.preserve-backend-catalog-name=true
```

This keeps `catName`/`catalogName` from the backend, typically `hive`, while still routing by the
externalized `dbName`.

If your workloads depend on Hive views or materialized views across multiple catalogs, also test
with:

```properties
federation.view-text-rewrite.mode=rewrite
federation.view-text-rewrite.preserve-original-text=true
```

That combination preserves the user-facing `viewOriginalText` while still rewriting
`viewExpandedText` for backend compatibility.

Then a non-default catalog is used through the external database name:

```sql
USE catalog2__sales;
SHOW TABLES IN catalog2__sales;
SELECT * FROM `catalog2__sales`.orders LIMIT 10;
```

If you keep the default separator `.`, older Hive SQL clients can treat `catalog.db.table`
ambiguously, so `__` is usually the safer choice.

## Security

### Without Kerberos

The default mode. No keytab or principals required:

```properties
server.port=9083
security.mode=NONE

catalogs=warehouse
catalog.warehouse.conf.hive.metastore.uris=thrift://hms-backend:9083
routing.default-catalog=warehouse
```

### With Kerberos

Security is configured in two independent parts: the front door (clients → proxy) and the
backend connections (proxy → HMS).

**Front door** — proxy listens with SASL:

```properties
security.mode=KERBEROS
security.server-principal=hive/_HOST@REALM.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab

# Optional: principal used when connecting to backends.
# Defaults to server-principal if not set.
security.client-principal=hive/_HOST@REALM.COM

# Optional: dedicated keytab used when connecting to backend metastores.
# Defaults to security.keytab if not set.
security.client-keytab=/etc/security/keytabs/hms-proxy-client.keytab
```

`security.server-principal` and `security.keytab` are required when `security.mode=KERBEROS`.
The keytab must exist and be readable — the proxy will fail to start otherwise.
`_HOST` is replaced with the proxy machine's canonical hostname before Kerberos login. If your DNS
canonical name differs from the keytab/KDC principal name, use the explicit FQDN instead.

When Kerberos is enabled on the front door, delegation-token methods
(`get_delegation_token`, `renew_delegation_token`, `cancel_delegation_token`)
are served locally by the proxy's own token manager instead of being forwarded
to backend metastores.

That also means proxy-user authorization for service principals such as HiveServer2 must be
configured on the proxy front door itself, not only on backend HMS instances. If HiveServer2
connects as `hive/_HOST@REALM.COM` and requests `get_delegation_token("alice", ...)`, the proxy
must see matching `hadoop.proxyuser.hive.*` rules through `core-site.xml` or
`security.front-door-conf.*` overrides.

The proxy reads delegation-token store settings from the usual Hive configuration
resources visible to the proxy process through `HiveConf`, typically `hive-site.xml`.
You can also set them directly in `hms-proxy.properties` via
`security.front-door-conf.<hive-conf-key>=...`, which is often simpler when the
launcher script does not put `hive-site.xml` on the proxy classpath.
If no persistent token store is configured, Hive falls back to
`org.apache.hadoop.hive.metastore.security.MemoryTokenStore`, and then a proxy
restart invalidates existing HiveServer2 delegation tokens.

### Front-door proxy-user rules for delegation-token issuance

`hadoop.proxyuser.<service>.*` is not related to ZooKeeper transport. It is only needed when a
service principal such as HiveServer2 asks the proxy to issue an end-user delegation token via
`get_delegation_token("alice", ...)`.

Example:

```properties
# Allow HiveServer2's Kerberos principal to request end-user delegation tokens from the proxy.
security.front-door-conf.hadoop.proxyuser.hive.hosts=hs2-1.example.com,hs2-2.example.com
security.front-door-conf.hadoop.proxyuser.hive.groups=*
```

### ZooKeeper token storage

Example with `ZooKeeperTokenStore` configured directly in the proxy config:

```properties
security.front-door-conf.hive.cluster.delegation.token.store.class=org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString=zk1:2181,zk2:2181,zk3:2181
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.znode=/hms-proxy-delegation-tokens
# Optional: ACL applied to newly created ZooKeeper nodes for the token store.
# security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.acl=sasl:hive:cdrwa
# Optional: token max lifetime in milliseconds.
# security.front-door-conf.hive.cluster.delegation.token.max-lifetime=604800000
```

When `ZooKeeperTokenStore` is enabled and `security.mode=KERBEROS`, the proxy now also
auto-populates `hive.metastore.kerberos.principal` and `hive.metastore.kerberos.keytab.file`
for the front-door `HiveConf` from `security.server-principal` and `security.keytab`. That lets
Hive's built-in `ZooKeeperTokenStore` client authenticate to ZooKeeper over SASL/Kerberos
without requiring a separate JAAS file in the common case. If you need different credentials for
ZooKeeper, set those `hive.metastore.kerberos.*` keys explicitly through `security.front-door-conf.*`.

In other words, the default ZooKeeper client credentials are:

- principal: `security.server-principal`
- keytab: `security.keytab`

These are the front-door service credentials. They are not taken from
`security.client-principal` or `security.client-keytab`, which are used for outbound backend HMS
connections instead.

At startup the proxy also pre-configures Hive's `HiveZooKeeperClient` JAAS entry for the
front-door token store from those same credentials before starting the local delegation-token
manager. In the usual setup that means you do not need a separate `-Djava.security.auth.login.config`
file just for ZooKeeper SASL.

If ZooKeeper must use different credentials from the proxy front door, override them explicitly:

```properties
security.front-door-conf.hive.metastore.kerberos.principal=hive-zk/_HOST@REALM.COM
security.front-door-conf.hive.metastore.kerberos.keytab.file=/etc/security/keytabs/hms-proxy-zk.keytab
```

On startup the proxy logs which `HiveConf` resources it found and which front-door
overrides were applied. If you see `Found configuration file null` from Hive or the
proxy warns that it is using `MemoryTokenStore`, the process did not see a persistent
delegation-token store config. If `get_delegation_token` fails with
`User: hive/... is not allowed to impersonate alice`, the proxy is missing front-door
`hadoop.proxyuser.<service>.hosts/groups` rules for that Kerberos caller.

### Kerberos impersonation

If you want backend HMS calls to execute as the authenticated front-door Kerberos user instead of
the proxy service principal, enable:

```properties
security.impersonation-enabled=true
```

When enabled, the proxy derives the caller identity from the inbound Kerberos/SASL session and
keeps a separate cached backend HMS client per user and per catalog, issuing `set_ugi()` once when
that cached client is opened. This avoids leaking one user's identity into another user's requests
while also avoiding a full backend reconnect on every RPC.

You can also override this per backend:

```properties
# Global default for catalogs that do not specify their own setting.
security.impersonation-enabled=false

catalog.catalog1.impersonation-enabled=true
catalog.catalog2.impersonation-enabled=false
```

This lets you enable caller impersonation only for selected backends while leaving the others on
the proxy service principal.

This mode requires `security.mode=KERBEROS` on the proxy listener. If a legacy client explicitly
calls `set_ugi`, the proxy will ignore the requested username and use the authenticated Kerberos
caller instead.

If internal HiveServer2 traffic works as user `hive`, but your personal Kerberos user fails even
with admin SQL privileges, that usually means backend impersonation is working as designed:

- service traffic from `hive` is allowed to continue on the proxy service principal
- real end users are sent to the backend through `set_ugi()` and therefore need backend Hadoop
  proxy-user permission for `security.client-principal`
- SQL admin rights in Hive do not replace Hadoop proxy-user authorization

In that case either allow proxy-user impersonation on the backend HMS for the proxy outbound
principal, or disable impersonation for that specific backend with
`catalog.<name>.impersonation-enabled=false`.

**Backend connections** — you can set shared defaults for all backend thrift clients via
`backend.conf.*`, then override per catalog via `catalog.<name>.conf.*`:

```properties
backend.conf.hive.metastore.client.socket.timeout=45s
backend.conf.hive.metastore.execute.setugi=true
backend.conf.hive.metastore.uris=thrift://shared-hms.internal:9083
```

Per-catalog values win over shared backend defaults:

```properties
catalog.catalog1.conf.hive.metastore.uris=thrift://hms-a.internal:9083
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```

Per catalog you can also restrict write traffic:

```properties
catalog.catalog1.access-mode=READ_ONLY
catalog.catalog2.access-mode=READ_WRITE_DB_WHITELIST
catalog.catalog2.write-db-whitelist=sales,analytics
```

Supported modes:

- `READ_WRITE`: default behavior
- `READ_ONLY`: only read RPCs are allowed for that catalog
- `READ_WRITE_DB_WHITELIST`: writes are allowed only when the resolved backend database is listed in
  `catalog.<name>.write-db-whitelist`

This means you can add arbitrary HiveConf keys used by `HiveMetaStoreClient` startup, not only
the metastore URI and Kerberos settings.

When `hive.metastore.sasl.enabled=true` is set for any catalog, the proxy opens outbound HMS
connections using `security.client-principal` and `security.client-keytab`. If those are omitted,
it falls back to `security.server-principal` and `security.keytab`.

Front and backend security are independent: you can run the front door without Kerberos and
still connect to Kerberos-protected backends, or vice versa. In that case, set
`security.mode=NONE` and provide `security.client-principal` plus `security.client-keytab`.

**Full example:**

```properties
server.port=9083
server.bind-host=0.0.0.0

security.mode=KERBEROS
security.server-principal=hive/_HOST@REALM.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab
security.client-principal=hive/_HOST@REALM.COM
security.client-keytab=/etc/security/keytabs/hms-proxy-client.keytab

catalogs=catalog1,catalog2
routing.default-catalog=catalog1

catalog.catalog1.conf.hive.metastore.uris=thrift://hms-a.internal:9083
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM

catalog.catalog2.conf.hive.metastore.uris=thrift://hms-b.internal:9083
catalog.catalog2.conf.hive.metastore.sasl.enabled=true
catalog.catalog2.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```

## Manual HMS smoke client

For the scenarios in [SMOKE.md](SMOKE.md), the repo now includes a runnable direct HMS API smoke
client:

- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn`
- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli notification`

The current smoke coverage is summarized in the [SMOKE.md](SMOKE.md) test matrix by:

- client version
- front-door profile
- backend profile
- auth mode
- method families
- expected result

Build the jar first:

```bash
mvn -DskipTests package
```

For Java 17+ with Kerberos and Hadoop 2.x libraries, use the same JVM flags as the proxy:

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

That mode performs:

- `open_txns`
- `allocate_table_write_ids`
- `lock`
- `check_lock`
- `get_valid_write_ids`
- `commit_txn`

The `notification` mode is for Hortonworks-only `add_write_notification_log`, so it also needs
the HDP standalone metastore jar:

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

Useful notes:

- `--server-principal` must be the proxy front-door principal, not the backend HMS principal
- `--client-principal` and `--keytab` are the Kerberos credentials used by this smoke client
- extra HiveConf overrides can be passed via repeated `--conf key=value`
- `notification` should succeed for a Hortonworks-routed catalog and fail with
  `requires a Hortonworks backend runtime` for an Apache-routed catalog
