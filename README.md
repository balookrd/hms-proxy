# HMS Proxy

Java proxy for Hive Metastore that exposes one external HMS Thrift endpoint and routes
requests to multiple backend metastores by catalog.

## What it supports

- one production-facing HMS Thrift endpoint for HiveServer2 and direct HMS API clients
- routing by explicit `catName` in newer HMS requests
- routing by legacy database names in `catalog<separator>db` form for older clients
- static catalog registry, so one proxy can front tables stored in different storage systems
- optional Kerberos/SASL on the front door
- optional impersonation of the authenticated Kerberos caller on backend HMS requests

## Important scope notes

- catalog definitions are managed in the proxy config, not via `create_catalog` or `drop_catalog`
- legacy database references without a catalog prefix are routed to `routing.default-catalog`
- session-level compatibility calls and other global read-only HMS operations without catalog context are routed to `routing.default-catalog`
- global write operations without clear catalog context are rejected when more than one catalog is configured
- when a backend HMS returns `TApplicationException` for selected read-only service APIs
  (notifications, privilege refresh/introspection, token/key listings except delegation-token issuance,
  txn/lock/compaction status),
  the proxy returns an empty compatibility response instead of failing the caller
- this keeps Spark/Hive compatibility while still avoiding ambiguous metadata writes
- Hive ACID, locks, tokens, and other truly global metastore operations need careful validation
  in your environment before turning them on behind a multi-catalog proxy

## Build

```bash
mvn -o test
mvn -o package
```

## Run

```bash
java -jar target/hms-proxy-0.1.0-SNAPSHOT-fat.jar /etc/hms-proxy/hms-proxy.properties
```

`mvn package` produces both a regular jar and a runnable fat jar with classifier `fat`.

For Java 17+ with Hadoop 2.x Kerberos libraries, start with:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -jar target/hms-proxy-0.1.0-SNAPSHOT-fat.jar /etc/hms-proxy/hms-proxy.properties
```

## Routing model

- Catalog-aware HMS clients can send `catName=dbCatalog, dbName=sales`
- Legacy HMS clients can use database names like `catalog1.sales`
- `get_all_databases()` returns prefixed names like `catalog1.sales`
- table objects returned to legacy callers are rewritten back to external names
- if a request carries a non-proxy `catName` such as Hive's default `hive`, the proxy falls back
  to `dbName`/default-catalog routing for compatibility
- by default, externalized HMS objects use proxy catalog ids in `catName`/`catalogName`
- for older HiveServer2 flows, you can enable `compatibility.preserve-backend-catalog-name=true`
  so externalized HMS objects keep the backend catalog name such as `hive` while `dbName`
  still uses the proxy namespace like `catalog2__default`

The catalog/database separator is configurable:

```properties
# Optional, defaults to "."
routing.catalog-db-separator=__
```

With that setting, legacy names become `catalog1__sales` instead of `catalog1.sales`.

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
compatibility.preserve-backend-catalog-name=true
```

This keeps `catName`/`catalogName` from the backend, typically `hive`, while still routing by the
externalized `dbName`.

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
`_HOST` is replaced with the machine's FQDN at runtime (standard Hadoop behaviour).

When Kerberos is enabled on the front door, delegation-token methods
(`get_delegation_token`, `renew_delegation_token`, `cancel_delegation_token`)
are served locally by the proxy's own token manager instead of being forwarded
to backend metastores.

The proxy reads delegation-token store settings from the usual Hive configuration
resources visible to the proxy process through `HiveConf`, typically `hive-site.xml`.
You can also set them directly in `hms-proxy.properties` via
`security.front-door-conf.<hive-conf-key>=...`, which is often simpler when the
launcher script does not put `hive-site.xml` on the proxy classpath.
If no persistent token store is configured, Hive falls back to
`org.apache.hadoop.hive.metastore.security.MemoryTokenStore`, and then a proxy
restart invalidates existing HiveServer2 delegation tokens.

Example with ZooKeeper token storage configured directly in the proxy config:

```properties
security.front-door-conf.hive.cluster.delegation.token.store.class=org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.connectString=zk1:2181,zk2:2181,zk3:2181
security.front-door-conf.hive.cluster.delegation.token.store.zookeeper.znode=/hms-proxy-delegation-tokens
```

On startup the proxy logs which `HiveConf` resources it found and which front-door
overrides were applied. If you see `Found configuration file null` from Hive or the
proxy warns that it is using `MemoryTokenStore`, the process did not see a persistent
delegation-token store config.

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

**Backend connections** — configured per catalog via `catalog.<name>.conf.*`:

```properties
catalog.catalog1.conf.hive.metastore.uris=thrift://hms-a.internal:9083
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```

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
