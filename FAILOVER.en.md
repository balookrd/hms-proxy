# Fault tolerance and Hive Metastore backend unavailability

Russian version: [FAILOVER.md](FAILOVER.md)

This document describes how `hms-proxy` handles unavailability, network failures, and degradation of remote Hive Metastore (HMS) backends across various lifecycle stages.

---

## 1. Proxy startup with an unreachable backend

During application startup ([`HmsProxyApplication`](src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java)):
1. The proxy parses configuration and initializes the catalog router ([`CatalogRouter`](src/main/java/io/github/mmalykhin/hmsproxy/routing/CatalogRouter.java)).
2. For each configured catalog (`catalog.<name>.*`), a [`CatalogBackend`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java) instance is instantiated, opening its underlying runtime layer ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)).
3. What happens next depends on `catalog.<name>.startup-mode`:

| Mode | Behavior when the backend is unreachable at startup |
| --- | --- |
| `STRICT` (default) | The session factory immediately attempts an initial connection (`initialSession`). If the backend is unreachable, the proxy exhausts retries, throws `MetaException`, and exits with a non-zero exit code (startup fail-fast), without opening client listener ports. The default catalog (`routing.default-catalog`) always runs in `STRICT`. |
| `LENIENT` (soft startup) | If a remote or secondary backend fails the initial connection, the proxy logs a warning `proceeding in degraded mode (startup-mode=LENIENT)`, continues startup, and opens client listener ports. Sessions for this catalog are established lazily: on the first request or after network recovery. |

> [!NOTE]
> Setting `catalog.<name>.startup-mode=LENIENT` is recommended for secondary catalogs in remote datacenters or across WAN links: a transient inter-DC outage then does not block the local proxy instance from starting and serving local traffic.

---

## 2. Backend failure while the proxy is running

If a backend metastore becomes unavailable while the proxy is running, the following mechanisms apply:

### 2.1. Session pools and single-shot retry
* All remote calls to a backend are served by session pools:
  * A shared session pool ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)) sized by `catalog.<name>.shared-session-pool-size`.
  * A per-user impersonation session pool ([`CatalogBackend.ImpersonationClient`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java)) sized by `catalog.<name>.impersonation-pool-max-size`.
* When a transport failure occurs (`TTransportException`, TCP connection reset, broken pipe), the corrupted session is marked invalid and discarded (`discard`).
* The proxy borrows or establishes a fresh session and executes exactly one automatic retry (`retrying once`), but only when a replay is safe: the call changes no metastore state (a read), or the `TTransportException` is of type `NOT_OPEN`, meaning the request was never sent. A writing call (`create_table`, `add_partition`, `open_txns`, `commit_txn`, `lock`, ...) whose reply was lost (read timeout, connection reset, EOF) is not replayed, because the backend may already have applied it. The failure goes back to the client, which decides on its own retry (`RetryingMetaStoreClient`). The decision is made by `ThriftFailureClassifier.isSafeToReplay` from `HmsOperationPolicy.replayableAfterLostReply`; a method the proxy does not know is treated as a write.
* Before the retry the proxy closes every idle session of the same pool (for impersonation, that user's pool). A metastore usually drops its connections together, and without this the retry borrowed the next dead session: with three dead sessions a read failed although the backend was answering again. The exception is a read timeout (`SocketTimeoutException`): the connection was alive, so the other pooled sessions stay. Impersonation sessions closed this way show up as `hms_proxy_impersonation_session_evictions_total{reason="stale_idle"}`.
* A call that cannot be replayed is not sent unchecked on a session that sat in the pool longer than `catalog.<name>.session-validate-after-idle-ms` (default `5000`, `0` turns the check off): the proxy sends `getStatus` on it first. If the check fails with a transport error, the session is closed together with the other idle ones and the call goes out once on a new session. Reads are not checked - the retry covers them. Check outcomes are counted in `hms_proxy_backend_session_validations_total{catalog,pool="shared|impersonation",outcome="alive|stale"}`.
* The check leaves two cases open. A connection dropped between `getStatus` and the call itself fails the call as before; such failures are counted in `hms_proxy_backend_calls_not_replayed_total{catalog,method,phase="send|receive|unknown"}`. A connection the network silently stopped delivering (no FIN/RST) holds `getStatus` until the catalog's `hive.metastore.client.socket.timeout`.
* If the retry also fails, the failure is recorded by the admission control subsystem.

### 2.2. Circuit breaker
When enabled via `routing.circuit-breaker.enabled=true`:
1. Failure tracking. Connectivity errors (`TTransportException`), socket timeouts (`SocketTimeoutException`), and protocol desyncs (`TApplicationException` of type `WRONG_METHOD_NAME`, `BAD_SEQUENCE_ID`, `INVALID_MESSAGE_TYPE`) are tracked in the backend runtime status ([`ProxyRuntimeState`](src/main/java/io/github/mmalykhin/hmsproxy/observability/ProxyRuntimeState.java)). Any other `TApplicationException` is a reply from a live backend (`UNKNOWN_METHOD` during capability negotiation, `INTERNAL_ERROR`, which libthrift uses to wrap any failure of a method that declares no exceptions), so it neither opens the circuit nor marks the backend unreachable.
2. Tripping to `OPEN`. When consecutive failures exceed the configured threshold (`routing.circuit-breaker.failure-threshold`, default: `3`), the circuit trips to `OPEN`.
3. Client fast-fail. All subsequent requests to this catalog are rejected immediately without blocking threads or waiting for socket timeouts:
   ```text
   MetaException: Backend catalog '<name>' rejected method '<method>' because circuit_open; next retry window in <X>ms
   ```
4. Probing with `HALF_OPEN`. Once the cooldown window elapses (`routing.circuit-breaker.open-state-ms`, default: `30000` ms), the circuit enters `HALF_OPEN`. Exactly one client call is admitted to probe backend availability:
   * If successful, the circuit resets to `CLOSED` and failure counters are cleared.
   * If it fails, the circuit re-enters `OPEN` for another cooldown interval.

### 2.3. Adaptive socket timeout
When enabled via `routing.adaptive-timeout.enabled=true`:
* The proxy computes an Exponentially Weighted Moving Average (EWMA) of backend response latency.
* On detecting elevated latency or transient timeouts, the proxy dynamically adjusts the client socket timeout within `[min-timeout-ms, max-timeout-ms]` so that connections are not dropped by timeout during temporary remote HMS load spikes.

### 2.4. Normalizing network errors into `MetaException`
* In the Hive Thrift IDL, infrastructure network exceptions (`TTransportException`, `TApplicationException`) are not declared in the `throws` clauses of most methods. Without special handling, the Thrift processor would intercept them and return a generic `TApplicationException("Internal error processing <method>")`, obscuring the root cause.
* [`BackendErrorNormalizer`](src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendErrorNormalizer.java) catches infrastructure exceptions and converts them into `MetaException`, which Hive methods declare in `throws`:
   ```text
   MetaException: Backend catalog 'hdp' failed in method 'get_table' with TTransportException: java.net.SocketException: Connection reset
   ```
  Upstream engines (Spark, HiveServer2, Trino, Impala) receive the original network failure cause in the exception text.
* The metastore client reports a failed connect itself, as `MetaException("Could not connect to meta store using any of the URIs provided…")`. It names the host but not the catalog, so the proxy wraps it too:
   ```text
   MetaException: Backend catalog 'apache' failed in method 'create_table' with MetaException: Could not connect to meta store using any of the URIs provided. Most recent failure: org.apache.thrift.transport.TTransportException: java.net.UnknownHostException: hms-apache
   ```
  Other `MetaException`s and other exceptions from `org.apache.hadoop.hive.metastore.api` (`NoSuchObjectException` and so on) reach the client unchanged.

### 2.5. Empty responses for service methods on failure
* For secondary and diagnostic metastore calls whose failure does not compromise metadata consistency (e.g., `get_active_resource_plan`, `get_all_resource_plans`, `get_runtime_stats`), the compatibility layer ([`CompatibilityLayer`](src/main/java/io/github/mmalykhin/hmsproxy/compatibility/CompatibilityLayer.java)) intercepts failures and returns an empty valid payload instead of failing the user session.
* For critical methods (schema reads, locks, transactions, privileges), failures are not masked: callers receive an explicit exception.

### 2.6. Table and partition metadata caches, serve-stale-on-error mode
* To reduce the number of requests over high-latency inter-DC WAN links, thread-safe in-memory caches are available:
  * [`TableMetadataCache`](src/main/java/io/github/mmalykhin/hmsproxy/routing/TableMetadataCache.java) (`routing.cache.table-metadata.*`) for `get_table`.
  * [`PartitionMetadataCache`](src/main/java/io/github/mmalykhin/hmsproxy/routing/PartitionMetadataCache.java) (`routing.cache.partition-metadata.*`) for `get_partition*`, `get_part_specs*`.
* SingleFlight coalescing: concurrent lookups for the same table or partition query are deduplicated into one backend call, so a cache miss does not cause a cache stampede across the WAN link.
* DDL invalidation: mutations (`alter_table*`, `drop_table*`, `truncate_table*`, `drop_database`) immediately invalidate cached table, partition, and database entries.
* Distributed DDL invalidation via ZooKeeper for multiple proxy instances (`routing.cache.distributed-invalidation.mode=ZOOKEEPER`):
  * When scaling `hms-proxy` horizontally across multiple replicas behind an L4/L7 load balancer, a mutating DDL operation processed on any instance publishes an invalidation event to a shared ZooKeeper znode (`routing.cache.distributed-invalidation.base-path`, default: `/hms-proxy-cache-invalidation`).
  * All connected `hms-proxy` replicas receive a notification via Apache Curator `PathChildrenCache` and invalidate their local cached table, partition, and database metadata.
  * Echo suppression: each replica is identified by `instance-id` and filters out its own published events.
  * Invalidation on ZooKeeper loss: if the ZooKeeper connection state changes to `LOST` or `SUSPENDED`, the proxy invalidates all local metadata caches to prevent serving stale data during network partitions.
  * Event retention sweep: a background cleaner periodically (`purge-interval-ms`) purges expired event znodes older than the retention window (`event-retention-ms`, default 5 minutes).
* Serve-stale-on-error (`routing.cache.serve-stale-on-error=true`): when remote backends experience network outages, timeouts, or connection resets, expired entries are served for the duration of the stale grace period (`routing.cache.stale-grace-period-ms`, default 5 minutes). During a transient WAN drop, read queries get metadata from the cache instead of an error.

### 2.7. Per-catalog concurrent call limit
To avoid flooding low-bandwidth inter-DC links and starving connection pools, each catalog can enforce a limit on concurrent in-flight RPCs:

| Parameter | Default | What it does |
| --- | --- | --- |
| `catalog.<name>.max-concurrent-calls` | `0` (unlimited) | Maximum number of concurrent calls to the catalog. |
| `catalog.<name>.concurrency-timeout-ms` | `10000` ms | How long a call waits for a free slot. |

RPCs exceeding the limit wait on a fair semaphore up to `concurrency-timeout-ms`. If no slot becomes free, the request fails with a `MetaException`, and the client thread does not hang longer than this timeout.

### 2.8. Read fallback to a shadow catalog replica
If a catalog has a local read replica available (e.g., replicated MySQL/PostgreSQL metastore DB), `hms-proxy` can fall back to it during primary outages:

| Parameter | Value | What it does |
| --- | --- | --- |
| `catalog.<name>.fallback-catalog` | `<shadow_catalog_name>` | Replica catalog that receives reads. |
| `catalog.<name>.fallback-on-outage` | `true` | Enables fallback on outage. |

* Read fallback: when the primary catalog fails (circuit breaker `OPEN`, `TTransportException`, timeouts), read-only RPCs (`get_table`, `get_database`, `get_partitions`, etc.) are rerouted to the shadow replica.
* Write guard (no split-brain): mutating operations (`create_*`, `alter_*`, `drop_*`, `truncate_*`, Iceberg commits) never fall back to the replica and fail with an explicit exception, so the replica does not diverge from the primary catalog.
* Tracked via Prometheus metric `hms_proxy_catalog_fallback_total`.

---

## 3. Fanout requests across catalogs (`SHOW DATABASES`, `get_table_meta`)

Operations that query all configured backends concurrently or sequentially ([`FanoutExecutor`](src/main/java/io/github/mmalykhin/hmsproxy/routing/FanoutExecutor.java)) behave according to `routing.degraded-routing-policy`:

| `routing.degraded-routing-policy` value | Behavior when one catalog fails |
| --- | --- |
| `STRICT` (default) | A failure in any catalog fails the entire fanout operation. Callers receive a `MetaException` indicating which backend failed. |
| `SAFE_FANOUT_READS` | The proxy omits the failed backend from the result set and returns an aggregated view from the remaining catalogs. A warning is logged (`omitting degraded backend catalog=<name> from safe fanout method=<method>`), and the request metric is tagged with `degraded=true`. |

`SAFE_FANOUT_READS` applies only to the read-only metadata methods `get_all_databases`, `get_databases`, `get_table_meta`. Writes and targeted catalog requests remain strict.

---

## 4. Impact of `default-catalog` and secondary catalog failures

The impact of a backend failure depends on its configured role.

### Secondary catalog failure
* Only queries referencing databases and tables in that catalog are impacted (e.g., `catalog2__analytics.events`).
* Requests directed to `default-catalog` and other healthy catalogs continue operating normally.

### `default-catalog` failure
* Control-plane outage:
  * Global Thrift RPCs without an explicit database name (`getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns`, `get_open_txns_info`) are pinned directly to `default-catalog` and will fail.
  * Hive transaction and lock coordination (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `heartbeat`) is backed by `default-catalog`. When down, transactional DDL/DML operations cannot proceed.
  * Schema mutations and writes via the Iceberg REST Catalog gateway (`WriteRouteGate`) are permitted exclusively for `default-catalog` tables. Consequently, Iceberg write operations are halted.

---

## 5. Iceberg REST Catalog responses when the backend is unreachable

* When the backend backing an Iceberg catalog is unreachable, the HTTP handler ([`IcebergHttpHandler`](src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java)) translates metastore exceptions into standard Iceberg REST responses:
  * The client receives HTTP 500 (Internal Server Error) or HTTP 503 (Service Unavailable).
  * The response body contains a structured `ErrorResponse` JSON with error details and messages expected by Iceberg REST clients (Spark, Trino, Flink, PyIceberg).

---

## 6. Health checks and backend state metrics

The management HTTP server ([`ManagementHttpServer`](src/main/java/io/github/mmalykhin/hmsproxy/app/ManagementHttpServer.java)) exposes endpoints and metrics for backend health tracking:

### 6.1. `/healthz` and `/readyz` endpoints
* `/healthz` (liveness probe):
  * Always returns HTTP 200 OK (`{"status":"ok","alive":true,...}`) as long as the proxy JVM process is running and accepting HTTP connections. Used for container liveness probes.
* `/readyz` (readiness probe):
  * Verifies connectivity across configured backends; the set of catalogs it counts is configurable:
    * By default, the probe evaluates all configured catalogs.
    * For remote or secondary catalogs, administrators can configure `catalog.<name>.required-for-readiness=false`. In this case, an outage of the remote catalog does not mark the proxy as degraded: `/readyz` continues returning HTTP 200 OK (`{"status":"ready",...}`), while reporting `"requiredForReadiness":false,"connected":false` for that catalog. A remote datacenter outage then does not take the local proxy instance out of the local load balancer.
    * The global flag `management.readyz.require-all-catalogs=false` ties proxy readiness to the `default-catalog` only.
  * The endpoint returns HTTP 503 Service Unavailable with `"status":"degraded"` when any required backend (`requiredForReadiness=true`) is not connected, is marked `degraded`, or has a circuit breaker outside `CLOSED` (`OPEN` or `HALF_OPEN`), and also when front-door or backend Kerberos is unhealthy. `"backendConnectivity":false` is set only in the first case, when a required backend is not connected.
  * Load balancers (HAProxy, Envoy, Kubernetes Ingress/Service) use this response to remove the proxy instance from the active routing pool.
  * Without background polling (`routing.backend-state-polling.enabled=false`) `/readyz` probes the backends itself, at most once per `management.readiness-cache-ms`. The probes follow the same rules as background polling (the shared [`BackendProbeRunner`](src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendProbeRunner.java)): a probe that finished before it is awaited counts by its own result even when a hung probe of another backend used up the shared deadline, and a backend whose previous probe has not returned is not probed again and is reported as `previous probe still running`. On this path a hung probe of a remote catalog used to use up the shared deadline, and `/readyz` answered 503 for about 25 seconds with the `default-catalog` in `lastError: TimeoutException: readiness probe deadline exceeded`.

### 6.2. Background backend polling
* When `routing.backend-state-polling.enabled=true` is set, a background scheduler periodically tests backend reachability using `getStatus` calls with the `probe-timeout-ms` timeout.
* Backend failures and recoveries are detected without user traffic, and external `/readyz` scrapes do not wait for a backend network call.

### 6.3. Prometheus metrics (`/metrics`)
Backend outages update these metrics:

| Metric | What it shows |
| --- | --- |
| `hms_proxy_backend_failures_total{backend="<name>", error="<class>"}` | Total backend failure count by error type. |
| `hms_proxy_backend_status{backend="<name>", state="connected\|degraded"}` | Current backend connectivity status. |
| `hms_proxy_circuit_state{backend="<name>"}` | Current circuit breaker state (`0 = CLOSED`, `1 = OPEN`, `2 = HALF_OPEN`). |
| `hms_proxy_backend_session_acquire_timeouts_total{backend="<name>", reason="borrow\|reconnect"}` | Session acquisition timeouts under overload or backend unresponsiveness. |
| `hms_proxy_impersonation_session_evictions_total{catalog="<name>", reason="transport_failure\|stale_idle\|stale_probe"}` | Impersonation sessions closed after a dropped connection: `transport_failure` - a call failed on it, `stale_idle` - it sat idle in the pool when the drop was found, `stale_probe` - it failed the pre-write check. |
| `hms_proxy_backend_session_validations_total{catalog="<name>", pool, outcome="alive\|stale"}` | Checks of an idle session before a call that cannot be replayed. |
| `hms_proxy_backend_calls_not_replayed_total{catalog="<name>", method, phase}` | Calls returned to the client with a transport error and no replay. Growth means clients see failed writes. |
