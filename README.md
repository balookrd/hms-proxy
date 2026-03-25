# HMS Proxy

Java proxy for Hive Metastore that exposes one external HMS Thrift endpoint and routes
requests to multiple backend metastores by catalog.

## What it supports

- one production-facing HMS Thrift endpoint for HiveServer2 and direct HMS API clients
- routing by explicit `catName` in newer HMS requests
- routing by legacy database names in `catalog.db` form for older clients
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
If your clients are older, use `catalog.db.table` naming consistently.

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

This mode requires `security.mode=KERBEROS` on the proxy listener. If a legacy client explicitly
calls `set_ugi`, the proxy will ignore the requested username and use the authenticated Kerberos
caller instead.

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
