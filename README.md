# HMS Proxy

Java proxy for Hive Metastore that exposes one external HMS Thrift endpoint and routes
requests to multiple backend metastores by catalog.

## What it supports

- one production-facing HMS Thrift endpoint for HiveServer2 and direct HMS API clients
- routing by explicit `catName` in newer HMS requests
- routing by legacy database names in `catalog.db` form for older clients
- static catalog registry, so one proxy can front tables stored in different storage systems
- optional Kerberos/SASL on the front door

## Important scope notes

- catalog definitions are managed in the proxy config, not via `create_catalog` or `drop_catalog`
- operations without clear catalog context are rejected when more than one catalog is configured
- this is a safe default for production because it avoids writing metadata into the wrong backend
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

## Routing model

- Catalog-aware HMS clients can send `catName=dbCatalog, dbName=sales`
- Legacy HMS clients can use database names like `catalog1.sales`
- `get_all_databases()` returns prefixed names like `catalog1.sales`
- table objects returned to legacy callers are rewritten back to external names

## Debug logging

Detailed debug tracing for the proxy package is enabled by default through `slf4j-simple`.
Each client call gets a `requestId`, and the logs include:

- incoming HMS request method and arguments
- selected backend catalog
- proxied thrift method and rewritten arguments
- backend response or backend error
- final client response or client-visible error

If the logs are too noisy, override the level at startup, for example:

```bash
java -Dorg.slf4j.simpleLogger.log.io.github.mmalykhin.hmsproxy=info ...
```

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
```

`security.server-principal` and `security.keytab` are required when `security.mode=KERBEROS`.
The keytab must exist and be readable — the proxy will fail to start otherwise.
`_HOST` is replaced with the machine's FQDN at runtime (standard Hadoop behaviour).

**Backend connections** — configured per catalog via `catalog.<name>.conf.*`:

```properties
catalog.catalog1.conf.hive.metastore.uris=thrift://hms-a.internal:9083
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```

Front and backend security are independent: you can run the front door without Kerberos and
still connect to Kerberos-protected backends, or vice versa.

**Full example:**

```properties
server.port=9083
server.bind-host=0.0.0.0

security.mode=KERBEROS
security.server-principal=hive/_HOST@REALM.COM
security.keytab=/etc/security/keytabs/hms-proxy.keytab

catalogs=catalog1,catalog2
routing.default-catalog=catalog1

catalog.catalog1.conf.hive.metastore.uris=thrift://hms-a.internal:9083
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM

catalog.catalog2.conf.hive.metastore.uris=thrift://hms-b.internal:9083
catalog.catalog2.conf.hive.metastore.sasl.enabled=true
catalog.catalog2.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
```
