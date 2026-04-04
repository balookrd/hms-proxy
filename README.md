# HMS Proxy

[![CI](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/balookrd/hms-proxy/actions/workflows/ci.yml)

Russian documentation: [README.ru.md](README.ru.md), [SMOKE.ru.md](SMOKE.ru.md)

HMS Proxy is a catalog-aware Hive Metastore federation and compatibility proxy for mixed
Apache Hive and Hortonworks Data Platform environments.

It gives you one production-facing HMS Thrift endpoint that can federate catalogs across
multiple backend metastores, bridge Apache/HDP API differences, and establish a clear
security boundary between clients and backend HMS services.

## Three pillars

### 1. Federation

Use one production-facing HMS Thrift endpoint for HiveServer2 and direct HMS API clients, while
routing requests to multiple backend metastores by explicit `catName` or by legacy
`catalog<separator>db` database names.

This lets you centralize catalog-aware routing and selective exposure without forcing clients to
understand backend metastore layout.

### 2. Compatibility bridge

Expose an Apache Hive Metastore `3.1.3` front door, downgrade selected calls for older
Hortonworks `3.1.0.x` backends, and optionally present the proxy itself as a Hortonworks
front door through an HDP `standalone-metastore` jar.

This makes the proxy a practical bridge for mixed Apache/HDP estates and staged migrations, not
just a request router.

### 3. Security boundary

Run the proxy as the security boundary between clients and backend metastores with optional
Kerberos/SASL on the front door, optional outbound Kerberos to backends, and optional
impersonation of the authenticated Kerberos caller.

This keeps authentication, identity propagation, and backend exposure policy concentrated in one
place.

## Canonical routing model

Two naming layers appear throughout this README:

| Term | Meaning |
| --- | --- |
| External name | The client-facing proxy namespace. Examples: `catName=catalog2`, `catalogName=catalog2`, legacy `dbName=catalog2__sales`. |
| Internal name | The real names on the selected backend HMS. Example: backend `catName=hive`, `dbName=sales`. |
| Default catalog | `routing.default-catalog`, used when a request does not carry a trustworthy catalog hint. |
| Ambiguous request | The request carries conflicting catalog hints, or a multi-catalog write does not resolve to exactly one catalog. |

Routing is then decided like this:

| Request shape | How the proxy picks a backend | What happens without namespace |
| --- | --- | --- |
| Object-scoped reads and writes | Prefer explicit proxy `catName`. Otherwise parse external `dbName` / `fullTableName` such as `catalog2__sales`. Otherwise use `routing.default-catalog` for compatibility. | Routed to `routing.default-catalog`. |
| Session-level and global reads | Use `routing.default-catalog`. | Routed to `routing.default-catalog`. |
| ACID RPCs whose payload still names an object | Route by that payload, for example `dbName` or `fullTableName` in `get_valid_write_ids`, `allocate_table_write_ids`, `compact`, or `add_dynamic_partitions`. | If no routable object namespace remains, fall through to the next row. |
| Txn / lock lifecycle RPCs that only carry ids | Pin to `routing.default-catalog`, for example `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, and `heartbeat`. | Routed to `routing.default-catalog`; non-ACID `SELECT` and eligible `NO_TXN` DDL locks on non-default catalogs can use the synthetic shim. |
| Global writes and catalog-registry changes | Require exactly one owned namespace in a multi-catalog deployment. | If that ownership is ambiguous, the proxy fails safely instead of guessing a target catalog. |

If a request carries a backend `catName` such as `hive` instead of a proxy catalog id, the proxy
treats that field as non-authoritative for compatibility and falls back to `dbName` or
`routing.default-catalog`.

For mutations, the proxy prefers policy-driven guarantees over best-effort guessing: deterministic
routing, explicit namespace ownership, no silent split-brain writes, and safe failure when a
mutation stays ambiguous.

These switches change client-visible names or SQL text, not backend selection:

| Switch | Effect |
| --- | --- |
| `routing.catalog-db-separator` | Changes the external legacy spelling, for example `catalog2__sales` instead of `catalog2.sales`. |
| `federation.preserve-backend-catalog-name=true` | Returns backend `catName` / `catalogName` such as `hive`, but routing still follows the external `dbName` or explicit proxy catalog. |
| `federation.view-text-rewrite.mode=rewrite` | Rewrites view SQL between external and internal names; it does not change backend selection for the RPC itself. |

## RPC behavior matrix

| RPC group | Status | Behavior |
| --- | --- | --- |
| Catalog-aware metadata reads and writes with explicit namespace (`catName`, `dbName`, `fullTableName`) | supported | Routed to the resolved catalog/backend normally. |
| Legacy reads and writes using `catalog<separator>db` database names | supported | Routed by externalized database name; table/database objects are rewritten on the way back. |
| Apache `3.1.3` wrapper RPCs against older Hortonworks `3.1.0.x` backends | degraded | Proxy retries selected `*_req` APIs through older legacy methods such as `get_table`. |
| Session-level and global read-only RPCs without catalog context | degraded | Routed to `routing.default-catalog`, including `getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns`, and `get_open_txns_info`. |
| Read-only service APIs missing on a backend (`TApplicationException` on notifications, privilege refresh/introspection, token/key listings except delegation-token issuance, txn/lock/compaction status) | degraded | Proxy returns an empty compatibility response instead of failing the caller. |
| ACID / txn / lock lifecycle RPCs without routable namespace (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat`) | degraded | Pinned to `routing.default-catalog`; non-ACID `SELECT` locks and eligible non-transactional `NO_TXN` DDL locks on non-default catalogs can use the proxy's synthetic lock shim, but this is still not a distributed ACID coordinator. |
| Global write operations without clear catalog context | rejected | Proxy enforces deterministic routing and explicit namespace ownership: namespace-less service writes such as `setMetaConf`, `grant_role`, `revoke_role`, and `add_token` fail safely instead of guessing a catalog. |
| Catalog registry management (`create_catalog`, `drop_catalog`) | rejected | Catalog ownership is policy-managed in proxy config, not delegated to client RPCs. |
| HDP-only front-door methods with an explicit Apache bridge mapping | supported | Proxy adapts selected Hortonworks request-wrapper methods to Apache equivalents. |
| HDP-only methods that require a matching Hortonworks runtime (`add_write_notification_log`, `get_tables_ext`, `get_all_materialized_view_objects_for_rewriting`) | passthrough | Forwarded only to compatible Hortonworks backends/front doors when configured. |
| HDP-only methods without a safe Apache mapping | rejected | Proxy fails explicitly instead of returning a misleading success response. |

## Public compatibility matrix

This matrix is the public contract for how to think about the proxy. It is intentionally grouped by
client shape, advertised front door, backend runtime, auth mode, and method family rather than by
individual thrift method names. The table is generated from [capabilities.yaml](capabilities.yaml)
and each capability row is linked to smoke-test coverage in the test suite.

For a method-level spreadsheet view of backend support, routing mode, fallback strategy, and
semantic risk, see [COMPATIBILITY.md](COMPATIBILITY.md).

Refresh the generated table with:

```bash
mvn -o -q -Dtest=CapabilityMatrixDocSyncTest -Dcapabilities.updateReadme=true test
```

<!-- BEGIN GENERATED: capability-matrix -->
| Client version | Front-door profile | Backend profile | Auth mode | Method families | Expected result |
| --- | --- | --- | --- | --- | --- |
| Apache Hive / Spark clients that speak Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | `APACHE_3_1_3` | `NONE` or `KERBEROS` | catalog-aware reads/writes, legacy `catalog<separator>db` routing, view rewrite | Supported as the baseline deployment. |
| Apache Hive / Spark clients that speak Apache HMS `3.1.3` request wrappers | `APACHE_3_1_3` | Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | read paths, selected metadata writes that can fall back from `*_req` APIs | Supported with compatibility downgrade; some calls are degraded to legacy RPCs. |
| Hortonworks clients that only expect a Hortonworks front-door identity via `getVersion()` | `HORTONWORKS_*` without standalone jar | `APACHE_3_1_3` or Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | overlapping Apache/HDP method families | Supported when changing the advertised profile is enough for the client. |
| Hortonworks clients that call HDP-only thrift request-wrapper methods | `HORTONWORKS_*` with standalone jar | Hortonworks `3.1.0.x` | `NONE` or `KERBEROS` | mapped HDP-only methods, runtime-specific passthrough methods | Supported when the matching Hortonworks front-door and backend runtime jars are configured. |
| Hortonworks clients that call HDP-only thrift request-wrapper methods | `HORTONWORKS_*` with standalone jar | `APACHE_3_1_3` | `NONE` or `KERBEROS` | HDP-only passthrough methods such as `add_write_notification_log` | Rejected explicitly when the target backend does not provide a compatible Hortonworks runtime. |
| HiveServer2 / Beeline SQL workloads across multiple catalogs | `APACHE_3_1_3` or `HORTONWORKS_*` | mixed Apache + Hortonworks backends | `NONE` or `KERBEROS` | reads, DDL/DML, namespace rewrite, optional view rewrite | Supported as long as routing can resolve the target catalog. |
| HiveServer2 / direct HMS clients using txn/lock lifecycle RPCs without namespace in the payload | any | mixed Apache + Hortonworks backends | `NONE` or `KERBEROS` | `open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `unlock`, `heartbeat` | Degraded: pinned to `routing.default-catalog`; eligible non-ACID `SELECT` and `NO_TXN` DDL locks can still be synthesized on non-default catalogs, but otherwise treat this as a single-catalog control plane unless you validated otherwise. |
| Kerberized HiveServer2 / HMS clients that require end-user identity on the backend | any | any | `KERBEROS` with optional impersonation | front-door SASL, local delegation-token issuance, backend `set_ugi()` impersonation | Supported when proxy-user rules and backend impersonation permissions are configured correctly. |
| Clients attempting mutations without explicit namespace ownership or dynamic catalog registry management | any | any | `NONE` or `KERBEROS` | policy-guarded ambiguous mutations, `create_catalog`, `drop_catalog` | Safely failed by design to preserve deterministic routing, explicit namespace ownership, and no silent split-brain writes. |
<!-- END GENERATED: capability-matrix -->

## Scope notes

- legacy database references without a catalog prefix are routed to `routing.default-catalog`
- this keeps Spark/Hive compatibility while still preserving deterministic routing and avoiding silent split-brain metadata writes
- in practice this means ACID write lifecycle is supported only for the default catalog unless the
  request payload itself carries routable namespace information such as `dbName` or `fullTableName`
- the proxy can synthesize non-ACID `SHARED_READ` `SELECT` locks and eligible non-transactional
  `NO_TXN` DDL locks for non-default catalogs so HiveServer2 read and non-ACID DDL paths stop
  depending on backend txn state alignment
- that synthetic lock state is in-memory by default and can be moved to ZooKeeper for multi-instance
  proxy failover, but it still does not make ACID writes or write-id coordination multi-catalog safe
- Hive ACID, locks, tokens, and other truly global metastore operations still need careful
  validation in your environment before turning them on behind a multi-catalog proxy

## Latency-aware backend routing

The proxy can also apply optional latency-aware backend handling for slow or intermittently failing
metastores. This layer is disabled by default, so existing deployments keep the previous routing
behavior until you opt in.

When enabled, the proxy can:

- keep a per-catalog latency budget via `catalog.<name>.latency-budget-ms`
- track per-backend latency EWMA and derive adaptive backend socket timeouts from recent response times
- open a circuit after repeated transport or time-budget failures, then allow a half-open retry after
  `routing.circuit-breaker.open-state-ms`
- poll backend readiness in the background instead of only probing on demand through `/readyz`
- run only safe read-only fanout RPCs in parallel when `routing.hedged-read.enabled=true`
- omit degraded backends from those safe fanout reads when
  `routing.degraded-routing-policy=SAFE_FANOUT_READS`

This is intentionally narrow in scope: hedged reads and degraded omission apply only to safe
read-only fanout methods, currently `get_all_databases`, `get_databases`, and `get_table_meta`.
Single-backend writes and namespace-sensitive mutations still follow the deterministic routing model
described above and do not race multiple metastores.

## Build

```bash
mvn -o test
mvn -o package
mvn -q -DforceStdout help:evaluate -Dexpression=project.version
```

Build version is computed from git for every commit in the form `0.1.<git-distance>-<short-sha>`.

GitHub Actions publishes prerelease builds automatically:
- every push to `main` creates a `build-<project.version>` tag and attaches the built jars to a prerelease
- a nightly run is scheduled for `00:00 UTC` every day and publishes a `nightly-YYYYMMDD` prerelease from the current `main` head
- each prerelease also gets auto-generated GitHub release notes for that tag

Manual releases are published through the `Release` workflow:
- start it with `workflow_dispatch`
- pass only `major_minor` such as `1.7`
- the workflow computes the next patch version automatically and publishes a GitHub release tag like `v1.7.0`, `v1.7.1`, and so on

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
  `lastProbeEpochSecond`, `lastLatencyMs`, `latencyEwmaMs`, `baselineTimeoutMs`,
  `adaptiveTimeoutMs`, `latencyBudgetMs`, `circuitState`, `consecutiveFailures`,
  `circuitRetryAtEpochMs`, and `lastError`
- Kerberos status for the front door and outbound backend credentials
- Kerberos TGT freshness via `tgtExpiresAtEpochSecond` and `secondsUntilExpiry` when available

If `routing.backend-state-polling.enabled=true`, readiness reflects the most recent background probe
results. Otherwise `/readyz` measures backend probe latency on demand and returns the same fields.

### Prometheus metrics

Current Prometheus metrics:

- `hms_proxy_requests_total{method,catalog,backend,status}`
- `hms_proxy_request_duration_seconds{method,catalog,backend}`
- `hms_proxy_backend_failures_total{backend,exception}`
- `hms_proxy_backend_fallback_total{method,from_api,to_api}`
- `hms_proxy_routing_ambiguous_total`
- `hms_proxy_default_catalog_routed_total{method}`
- `hms_proxy_filtered_objects_total{method,catalog,object_type}`
- `hms_proxy_synthetic_read_lock_events_total{operation,catalog,store_mode,result}`
- `hms_proxy_synthetic_read_lock_store_failures_total{operation,store_mode,exception}`
- `hms_proxy_synthetic_read_lock_handoffs_total{operation,catalog,store_mode}`
- `hms_proxy_synthetic_read_locks_active{store_mode}`
- `hms_proxy_synthetic_read_lock_store_info{store_mode}`

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: hms-proxy
    static_configs:
      - targets:
          - hms-proxy-01.example.com:19083
```

Metric semantics:

- `status` in `hms_proxy_requests_total` is one of `ok`, `error`, `fallback`, or `throttled`
- `catalog=all, backend=fanout` means a request was broadcast to multiple backends
- `hms_proxy_backend_failures_total` counts backend-side invocation failures grouped by backend and exception type
- `hms_proxy_backend_fallback_total` counts compatibility fallbacks returned after backend failures
- `hms_proxy_routing_ambiguous_total` counts requests that safely failed because the proxy saw conflicting namespace hints and refused to guess
- `hms_proxy_default_catalog_routed_total` counts requests that were routed to the default catalog because no explicit catalog namespace was present
- `hms_proxy_rate_limited_total` counts requests rejected by overload protection with labels for the limiting dimension, configured scope, method family, and resolved catalog
- `hms_proxy_filtered_objects_total` counts databases or tables hidden by selective federation exposure rules before they are returned to the client
- `hms_proxy_synthetic_read_lock_events_total` tracks synthetic lock shim lifecycle transitions such as `acquire`, `check_lock`, `heartbeat`, `unlock`, `release_txn`, and `cleanup`
- `hms_proxy_synthetic_read_lock_store_failures_total` counts in-memory or ZooKeeper store failures grouped by operation and exception type
- `hms_proxy_synthetic_read_lock_handoffs_total` counts cases where one proxy instance continues serving a synthetic lock originally acquired through another instance
- `hms_proxy_synthetic_read_locks_active` exposes the number of currently visible synthetic locks for the configured store backend
- `hms_proxy_synthetic_read_lock_store_info` is a constant-info gauge that marks whether this proxy runs with `in_memory` or `zookeeper` synthetic lock storage

Despite the historical `synthetic_read_lock` metric names, the shim now also serves eligible
non-transactional `NO_TXN` DDL locks on non-default catalogs.

The bundled Grafana dashboard at `monitoring/grafana/hms-proxy-dashboard.json` includes panels for
synthetic lock activity, handoffs, store failures, and active lock counts, plus a `store_mode`
templating variable for quickly switching between `in_memory` and `zookeeper` views.

### Structured audit log

The proxy emits one structured audit log record per request through the logger
`io.github.mmalykhin.hmsproxy.audit`. Each record is a single-line JSON object with fields such as
`requestId`, `method`, `operationClass`, `catalog`, `backend`, `status`, `durationMs`,
`remoteAddress`, `authenticatedUser`, `routed`, `fanout`, `fallback`, and
`defaultCatalogRouted`.

Example:

```json
{"event":"hms_proxy_audit","requestId":42,"method":"get_table","operationClass":"metadata_read","catalog":"catalog1","backend":"catalog1","status":"ok","durationMs":8,"routed":true,"fanout":false,"fallback":false,"defaultCatalogRouted":false,"remoteAddress":"10.20.30.40","authenticatedUser":"alice@EXAMPLE.COM"}
```

### Grafana dashboard

A ready-to-import Grafana dashboard is included in
`monitoring/grafana/hms-proxy-dashboard.json`. It covers request rate, latency, backend failures,
fallbacks, default-catalog routing, and ambiguous routing events.

### Selective federation exposure

You can publish only part of a backend namespace while keeping routing and write policy unchanged.
This is useful for gradual migration, safer multi-tenant rollout, and avoiding accidental metadata
exposure.

Per catalog:

```properties
# Backward-compatible default
catalog.catalog1.expose-mode=ALLOW_ALL

# Safer rollout mode: only matched objects are visible
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-db-patterns=sales,finance_.*
catalog.catalog1.expose-table-patterns.sales=orders_.*,events
catalog.catalog1.expose-table-patterns.finance_.*=audit_.*
```

Rules:

- regexes are matched case-insensitively
- matching is done against backend names, not externalized proxy names
- matching uses the whole string; `sales` matches only `sales`, while `sales_.*` matches `sales_eu`
- `catalog.<name>.expose-db-patterns` is an allowlist for backend database names inside that catalog
- `catalog.<name>.expose-table-patterns.<dbRegex>` is an allowlist for table names inside databases whose backend db name matches `<dbRegex>`
- table rules narrow visibility for matching databases; unmatched tables are filtered out
- with `DENY_BY_DEFAULT`, databases without a matching db rule or table rule are hidden
- a matching table rule can expose a database even when no db-level rule is present, but only matching tables inside that database stay visible

The filter is applied on metadata read paths such as `get_all_databases`, `get_databases`,
`get_table*`, `get_tables*`, `get_table_meta`, and Hortonworks `get_tables_ext`.

Behavior by API shape:

- list-style RPCs such as `get_all_databases`, `get_all_tables`, `get_tables`, `get_table_meta`, and `get_tables_ext` silently drop hidden objects from the response
- direct lookups such as `get_database`, `get_table`, and `get_table_req` fail as "not found" when the target object is filtered out
- `hms_proxy_filtered_objects_total{method,catalog,object_type}` counts both hidden databases and hidden tables

Examples:

```properties
# 1. Publish only one database during migration
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-db-patterns=sales

# 2. Publish one database, but only selected tables inside it
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-table-patterns.sales=orders_.*,customers

# 3. Keep the catalog open by default, but narrow one sensitive database
catalog.catalog1.expose-mode=ALLOW_ALL
catalog.catalog1.expose-table-patterns.audit=.*_public
```

What those examples mean:

- example 1 exposes only backend db `sales`
- example 2 makes backend db `sales` visible and exposes only tables matching `orders_.*` plus `customers`
- example 3 leaves all databases visible, but in backend db `audit` returns only tables matching `.*_public`

For non-default catalogs, remember that filters still use backend db names such as `sales`, not
external names like `catalog2__sales`.

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

## Rate limiting / overload protection

The proxy can also reject bursts before they become backend overload. This protection is local to
each proxy instance and uses token-bucket limits with:

- a steady refill rate via `requests-per-second`
- an optional short burst allowance via `burst`
- independent buckets, so a request must pass every configured scope that applies to it

Supported scopes:

- per authenticated client principal: `rate-limit.principal.*`
- per exact source IP: `rate-limit.source.*`
- per source CIDR rule: `rate-limit.source-cidr.<name>.*`
- per HMS method family: `rate-limit.method-family.<family>.*`
- per logical catalog: `rate-limit.catalog.<catalog>.*`
- per high-risk RPC class: `rate-limit.rpc-class.<class>.*`

Supported method families:

- `metadata_read`
- `metadata_write`
- `service_global_read`
- `service_global_write`
- `acid_namespace_bound_write`
- `acid_id_bound_lifecycle`
- `admin_introspection`
- `compatibility_only_rpc`

Supported RPC classes:

- `write`
- `ddl`
- `txn`
- `lock`

Important behavior:

- per-principal limits apply only when the front door exposes an authenticated user, for example Kerberos/SASL
- `rate-limit.source-cidr.<name>` is an aggregate bucket for the whole configured CIDR rule, not a separate bucket per IP
- if one source IP matches several CIDR rules, all matching rules are enforced
- per-catalog limits are enforced when the request actually resolves or touches a catalog/backend; fanout reads can therefore consume more than one catalog bucket
- one RPC can match several classes at once, for example a lock/txn write can count toward `write`, `txn`, and `lock`
- when a limit is exceeded the client receives a `MetaException` and the request is recorded with `status="throttled"`

Example:

```properties
# Per authenticated principal
rate-limit.principal.requests-per-second=60
rate-limit.principal.burst=120

# Per exact source IP
rate-limit.source.requests-per-second=30
rate-limit.source.burst=60

# Aggregate bucket for a whole subnet or pool
rate-limit.source-cidr.hs2-pool.cidrs=10.10.0.0/16,10.20.0.0/16
rate-limit.source-cidr.hs2-pool.requests-per-second=200
rate-limit.source-cidr.hs2-pool.burst=300

# Method-family shaping
rate-limit.method-family.metadata_read.requests-per-second=600
rate-limit.method-family.metadata_read.burst=1000

# Per catalog
rate-limit.catalog.catalog1.requests-per-second=300
rate-limit.catalog.catalog2.requests-per-second=120

# Extra protection for high-risk RPC classes
rate-limit.rpc-class.write.requests-per-second=80
rate-limit.rpc-class.ddl.requests-per-second=15
rate-limit.rpc-class.txn.requests-per-second=30
rate-limit.rpc-class.lock.requests-per-second=50
```

Recommended production starting point:

- use `principal` for runaway HS2 sessions or bad end users
- use `source` and `source-cidr` for tooling, scanners, or large client pools
- use `method-family.metadata_read` to cap scan-heavy metadata discovery
- use `catalog.<name>` to keep one hot catalog from starving the others
- keep stricter limits on `ddl`, `txn`, and `lock` than on plain metadata reads

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

This only changes the external legacy spelling from the canonical routing model above.

If HiveServer2 metadata writes behave differently through the proxy than directly against the
backend HMS, try enabling:

```properties
federation.preserve-backend-catalog-name=true
```

This only changes the returned `catName`/`catalogName`, typically to backend values such as
`hive`. Backend selection still follows the canonical routing model above.

If your workloads depend on Hive views or materialized views across multiple catalogs, also test
with:

```properties
federation.view-text-rewrite.mode=rewrite
federation.view-text-rewrite.preserve-original-text=true
```

That combination rewrites view SQL between external and internal names while preserving the
user-facing `viewOriginalText`. It does not change backend selection for the RPC itself.

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

### ZooKeeper storage for synthetic read locks

The proxy also has a narrow synthetic lock shim for non-default catalogs. It covers
non-ACID `SHARED_READ` `SELECT` locks and eligible non-transactional `NO_TXN` DDL locks
such as `CREATE TABLE` and partition rename/drop flows that Hive still runs through the
txn/lock APIs. The proxy serves those locks locally when backend txn ids do not line up
across catalogs. This does not turn the proxy into a distributed ACID coordinator.

By default that synthetic lock state is kept in memory, which is fine for a single proxy
instance. For HA / load-balanced deployments you can persist it in ZooKeeper so
`check_lock`, `unlock`, `heartbeat`, `commit_txn`, and `abort_txn` can continue through a
different proxy instance after the first one dies.

Example:

```properties
synthetic-read-lock.store.mode=ZOOKEEPER
synthetic-read-lock.store.zookeeper.connect-string=zk1:2181,zk2:2181,zk3:2181
synthetic-read-lock.store.zookeeper.znode=/hms-proxy-synthetic-read-locks
# synthetic-read-lock.store.zookeeper.connection-timeout-ms=15000
# synthetic-read-lock.store.zookeeper.session-timeout-ms=60000
# synthetic-read-lock.store.zookeeper.base-sleep-ms=1000
# synthetic-read-lock.store.zookeeper.max-retries=3
```

When `security.mode=KERBEROS`, the synthetic read-lock store uses the same
`security.server-principal` and `security.keytab` credentials for ZooKeeper SASL/Kerberos
as the front door by default, similar to the delegation-token store setup above.

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

Per catalog you can also define a latency budget for the latency-aware routing layer:

```properties
catalog.catalog1.latency-budget-ms=1500
catalog.catalog2.latency-budget-ms=5000
```

And you can independently restrict which metadata is visible from each backend:

```properties
catalog.catalog1.expose-mode=DENY_BY_DEFAULT
catalog.catalog1.expose-db-patterns=sales
catalog.catalog1.expose-table-patterns.sales=orders_.*,events
```

Supported modes:

- `READ_WRITE`: default behavior
- `READ_ONLY`: only read RPCs are allowed for that catalog
- `READ_WRITE_DB_WHITELIST`: writes are allowed only when the resolved backend database is listed in
  `catalog.<name>.write-db-whitelist`

Exposure modes:

- `ALLOW_ALL`: default metadata exposure behavior for `catalog.<name>.expose-mode`
- `DENY_BY_DEFAULT`: metadata is hidden unless matched by `catalog.<name>.expose-db-patterns` or
  `catalog.<name>.expose-table-patterns.<dbRegex>`

This means you can add arbitrary HiveConf keys used by `HiveMetaStoreClient` startup, not only
the metastore URI and Kerberos settings.

**Latency-aware routing** — optional backend-state polling, adaptive timeouts, circuit breaking,
safe hedged fanout reads, and degraded omission are configured with `routing.*` properties:

```properties
routing.backend-state-polling.enabled=true
routing.backend-state-polling.interval-ms=10000
routing.adaptive-timeout.enabled=true
routing.adaptive-timeout.initial-ms=5000
routing.adaptive-timeout.min-ms=1000
routing.adaptive-timeout.max-ms=60000
routing.adaptive-timeout.multiplier=4.0
routing.adaptive-timeout.alpha=0.2
routing.circuit-breaker.enabled=true
routing.circuit-breaker.failure-threshold=3
routing.circuit-breaker.open-state-ms=30000
routing.hedged-read.enabled=true
routing.hedged-read.max-parallelism=8
routing.degraded-routing-policy=SAFE_FANOUT_READS
```

The adaptive timeout starts from `routing.adaptive-timeout.initial-ms`, then follows backend
latency EWMA within the configured min/max bounds. Transport failures and latency-budget breaches
count toward the circuit breaker. Once a backend crosses `routing.circuit-breaker.failure-threshold`,
the proxy fails fast for that backend until the open window expires, then lets one half-open retry
decide whether to close or reopen the circuit.

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
routing.backend-state-polling.enabled=true
routing.adaptive-timeout.enabled=true
routing.circuit-breaker.enabled=true
routing.hedged-read.enabled=true
routing.degraded-routing-policy=SAFE_FANOUT_READS

catalog.catalog1.conf.hive.metastore.uris=thrift://hms-a.internal:9083
catalog.catalog1.conf.hive.metastore.sasl.enabled=true
catalog.catalog1.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
catalog.catalog1.latency-budget-ms=1500

catalog.catalog2.conf.hive.metastore.uris=thrift://hms-b.internal:9083
catalog.catalog2.conf.hive.metastore.sasl.enabled=true
catalog.catalog2.conf.hive.metastore.kerberos.principal=hive/_HOST@REALM.COM
catalog.catalog2.latency-budget-ms=5000
```

## Manual HMS smoke client

For the scenarios in [SMOKE.md](SMOKE.md), the repo now includes a runnable direct HMS API smoke
client:

- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli txn`
- `io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock`
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

The `lock` mode is for direct lock lifecycle smoke, especially the synthetic shim on
non-default catalogs. It opens one txn, acquires the requested lock, runs `check_lock`,
optionally sends `heartbeat`, optionally calls `unlock`, and then aborts or commits the txn.

Example `CREATE TABLE`-style DB lock on a non-default catalog:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock \
  --uri thrift://proxy-host:9083 \
  --db apache__default \
  --lock-type SHARED_READ \
  --lock-level DB \
  --operation-type NO_TXN \
  --transactional false
```

Example partition rename/drop style lock on a non-default catalog:

```bash
java \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp target/hms-proxy-$(mvn -q -DforceStdout help:evaluate -Dexpression=project.version)-fat.jar \
  io.github.mmalykhin.hmsproxy.tools.HmsMetastoreSmokeCli lock \
  --uri thrift://proxy-host:9083 \
  --db apache__default \
  --table smoke_managed_tbl \
  --partition p=2026-04-01 \
  --lock-type EXCLUSIVE \
  --lock-level PARTITION \
  --operation-type NO_TXN \
  --transactional false
```

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
- `lock` is the quickest way to reproduce non-default catalog `NO_TXN` shim cases such as
  `CREATE TABLE` and partition rename/drop without going through Beeline
- `notification` should succeed for a Hortonworks-routed catalog and fail with
  `requires a Hortonworks backend runtime` for an Apache-routed catalog

### Automated Real-installation Smoke

For repeated checks on a real proxy installation, use separate runners for `simple` and `kerberos`
front doors:

- [`scripts/run-real-installation-smoke-simple.sh`](scripts/run-real-installation-smoke-simple.sh)
- [`scripts/run-real-installation-smoke-kerberos.sh`](scripts/run-real-installation-smoke-kerberos.sh)

They wrap the same smoke client and run a fail-fast scenario of:

- optional Beeline / HiveServer2 SQL smoke from [SMOKE.md](SMOKE.md)
- direct txn/ACID smoke
- non-default catalog DB lock smoke
- optional partition lock smoke
- optional Hortonworks notification smoke

Start from the matching example config:

```bash
cp scripts/hms-real-installation-smoke.simple.env.example scripts/hms-real-installation-smoke.simple.env
cp scripts/hms-real-installation-smoke.kerberos.env.example scripts/hms-real-installation-smoke.kerberos.env
```

Then edit the relevant `HMS_SMOKE_*` values and run:

```bash
scripts/run-real-installation-smoke-simple.sh --scenario all
scripts/run-real-installation-smoke-kerberos.sh --scenario all
```

Or run a narrower slice:

```bash
scripts/run-real-installation-smoke-simple.sh --scenario sql
scripts/run-real-installation-smoke-simple.sh --scenario locks
scripts/run-real-installation-smoke-kerberos.sh --scenario notification
```

Notes:

- by default the runner picks the newest `target/hms-proxy-*-fat.jar`
- you can override the jar path with `HMS_SMOKE_FAT_JAR`
- if `HMS_SMOKE_BEELINE_JDBC_URL` is configured, `all` also runs the Beeline / HiveServer2 SQL smoke from `SMOKE.md`
- SQL smoke uses `HMS_SMOKE_HDP_READ_TABLE` / `HMS_SMOKE_APACHE_READ_TABLE` and can optionally run transactional SQL and materialized-view checks
- if `HMS_SMOKE_TXN_SECONDARY_DB` and `HMS_SMOKE_TXN_SECONDARY_TABLE` are set, the runner executes a second direct txn smoke target
- if `HMS_SMOKE_NOTIFICATION_*` is not configured, the notification step is skipped in `all`
- if `HMS_SMOKE_NOTIFICATION_NEGATIVE_DB` and `HMS_SMOKE_NOTIFICATION_NEGATIVE_TABLE` are set, the runner also executes the negative Apache-backend notification check from `SMOKE.md`
- the simple runner auto-loads `scripts/hms-real-installation-smoke.simple.env` when it exists
- the Kerberos runner auto-loads `scripts/hms-real-installation-smoke.kerberos.env` when it exists
