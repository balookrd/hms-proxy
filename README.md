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
java -cp "/opt/hms-proxy/lib/*:/opt/hms-proxy/hms-proxy-0.1.0-SNAPSHOT.jar" \
  io.github.mmalykhin.hmsproxy.HmsProxyApplication \
  /etc/hms-proxy/hms-proxy.properties
```

`mvn package` in this repository produces a thin application jar. In production, package the
runtime dependencies next to the jar or build a container image that adds the Maven runtime
classpath explicitly.

## Routing model

- Catalog-aware HMS clients can send `catName=dbCatalog, dbName=sales`
- Legacy HMS clients can use database names like `catalog1.sales`
- `get_all_databases()` returns prefixed names like `catalog1.sales`
- table objects returned to legacy callers are rewritten back to external names

## HiveServer2

Point HiveServer2 `hive.metastore.uris` to this proxy instead of a single backend HMS.
For multi-catalog deployments, prefer Hive versions and clients that preserve catalog fields.
If your clients are older, use `catalog.db.table` naming consistently.

## Kerberos

With `security.mode=KERBEROS`, the proxy starts a SASL-protected Thrift listener and logs in
using the configured keytab. Backend HMS connections can also use Kerberos by passing the usual
Hive settings via `catalog.<name>.conf.*`.
