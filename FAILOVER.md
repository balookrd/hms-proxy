# Fault Tolerance and Hive Metastore Backend Unavailability

Russian version: [FAILOVER.ru.md](FAILOVER.ru.md)

This document describes how **hms-proxy** handles unavailability, network failures, and degradation of remote Hive Metastore (HMS) backends across various lifecycle stages.

---

## 1. Startup Behavior

During application startup ([`HmsProxyApplication`](src/main/java/io/github/mmalykhin/hmsproxy/app/HmsProxyApplication.java)):
1. The proxy parses configuration and initializes the catalog router ([`CatalogRouter`](src/main/java/io/github/mmalykhin/hmsproxy/routing/CatalogRouter.java)).
2. For each configured catalog (`catalog.<name>.*`), a [`CatalogBackend`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java) instance is instantiated, opening its underlying runtime layer ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)).
3. As part of session pool initialization, the session factory immediately attempts to establish an initial connection (`initialSession`) using `HiveMetaStoreClient` or the isolated runtime client.

> [!WARNING]
> **Startup Fail-Fast**: If any configured backend is unreachable during startup, `HiveMetaStoreClient` exhausts its connection retries (`hive.metastore.connect.retries`) and throws a `MetaException`. Consequently, the proxy terminates immediately with a non-zero exit code and **does not open client listener ports**.

---

## 2. Runtime Behavior on Backend Outage

If a backend metastore becomes unavailable while the proxy is running, the following protection mechanisms take effect:

### 2.1. Session Pooling and Automatic Single-Shot Retry
* All remote calls to a backend are served by session pools:
  * A shared session pool ([`BackendRuntime`](src/main/java/io/github/mmalykhin/hmsproxy/backend/BackendRuntime.java)) sized by `catalog.<name>.shared-session-pool-size`.
  * A per-user impersonation session pool ([`CatalogBackend.ImpersonationClient`](src/main/java/io/github/mmalykhin/hmsproxy/backend/CatalogBackend.java)) sized by `catalog.<name>.impersonation-pool-max-size`.
* When a transport failure occurs (`TTransportException`, TCP connection reset, broken pipe), the corrupted session is marked invalid and discarded (`discard`).
* The proxy borrows or establishes a fresh session and executes **exactly one automatic retry** (`retrying once`).
* If the retry also fails, the failure is recorded by the admission control subsystem.

### 2.2. Circuit Breaker
When enabled via `routing.circuit-breaker.enabled=true`:
1. **Failure Tracking**: Connectivity errors (`TTransportException`), socket timeouts (`SocketTimeoutException`), and protocol desyncs are tracked in the backend runtime status ([`ProxyRuntimeState`](src/main/java/io/github/mmalykhin/hmsproxy/observability/ProxyRuntimeState.java)).
2. **Tripping to `OPEN`**: When consecutive failures exceed the configured threshold (`routing.circuit-breaker.failure-threshold`, default: `3`), the circuit trips to `OPEN`.
3. **Client Fast-Fail**: All subsequent requests to this catalog are rejected immediately without blocking threads or waiting for socket timeouts:
   ```text
   MetaException: Backend catalog '<name>' rejected method '<method>' because circuit_open; next retry window in <X>ms
   ```
4. **Probing with `HALF_OPEN`**: Once the cooldown window elapses (`routing.circuit-breaker.open-state-ms`, default: `30000` ms), the circuit enters `HALF_OPEN`. Exactly one client call is admitted to probe backend availability:
   * If successful, the circuit resets to `CLOSED` and failure counters are cleared.
   * If it fails, the circuit re-enters `OPEN` for another cooldown interval.

### 2.3. Adaptive Socket Timeout
When enabled via `routing.adaptive-timeout.enabled=true`:
* The proxy computes an Exponentially Weighted Moving Average (EWMA) of backend response latency.
* On detecting elevated latency or transient timeouts, the proxy dynamically adjusts the client socket timeout within `[min-timeout-ms, max-timeout-ms]`, preventing spurious disconnects during temporary remote HMS load spikes.

### 2.4. Error Normalization for Thrift Clients
* In the Hive Thrift IDL, infrastructure network exceptions (`TTransportException`, `TApplicationException`) are not declared in the `throws` clauses of most methods. Without special handling, the Thrift processor would intercept them and return a generic `TApplicationException("Internal error processing <method>")`, obscuring the root cause.
* For transparent client-side diagnostics, [`BackendErrorNormalizer`](src/main/java/io/github/mmalykhin/hmsproxy/routing/BackendErrorNormalizer.java) catches infrastructure exceptions and normalizes them into Hive's standard `MetaException`:
   ```text
   MetaException: Backend catalog 'hdp' failed in method 'get_table' with TTransportException: java.net.SocketException: Connection reset
   ```
  This preserves the full diagnostic cause for upstream engines (Spark, HiveServer2, Trino, Impala).

### 2.5. Compatibility Fallbacks for Service Methods
* For secondary and diagnostic metastore calls whose failure does not compromise metadata consistency (e.g., `get_active_resource_plan`, `get_all_resource_plans`, `get_runtime_stats`), the compatibility layer ([`CompatibilityLayer`](src/main/java/io/github/mmalykhin/hmsproxy/compatibility/CompatibilityLayer.java)) intercepts failures and returns an empty valid payload instead of failing the user session.
* For critical methods (schema reads, locks, transactions, privileges), failures are never masked — callers are guaranteed to receive an explicit exception.

---

## 3. Federation and Fanout Requests (`SHOW DATABASES`, `get_table_meta`)

When servicing operations that query all configured backends concurrently or sequentially ([`FanoutExecutor`](src/main/java/io/github/mmalykhin/hmsproxy/routing/FanoutExecutor.java)):

* **Default Policy — `STRICT` (`routing.degraded-routing-policy=STRICT`)**:
  A failure in any catalog fails the entire fanout operation. Callers receive a `MetaException` indicating which backend failed.
* **Degraded Policy — `SAFE_FANOUT_READS` (`routing.degraded-routing-policy=SAFE_FANOUT_READS`)**:
  The proxy omits the failed backend from the result set and returns an aggregated view from all healthy catalogs.
  * A warning is logged: `omitting degraded backend catalog=<name> from safe fanout method=<method>`.
  * The request metric is tagged with `degraded=true`.
  * This degradation applies strictly to safe read-only metadata methods (`get_all_databases`, `get_databases`, `get_table_meta`). Writes and targeted catalog requests remain strict.

---

## 4. Role Separation: `default-catalog` vs Secondary Catalogs

The operational impact of a backend failure depends directly on its configured role:

### Secondary Catalog Failure
* Only queries referencing databases and tables in that catalog are impacted (e.g., `catalog2__analytics.events`).
* Requests directed to `default-catalog` and other healthy catalogs continue operating normally.
* Namespace isolation prevents an issue in an external/remote metastore from degrading the primary cluster.

### `default-catalog` Failure
* **Critical Control-Plane Outage**:
  * Global Thrift RPCs without an explicit database name (`getMetaConf`, `get_all_functions`, `get_metastore_db_uuid`, `get_current_notificationEventId`, `get_open_txns`, `get_open_txns_info`) are pinned directly to `default-catalog` and will fail.
  * Hive transaction and lock coordination (`open_txns`, `commit_txn`, `abort_txn`, `check_lock`, `heartbeat`) is backed by `default-catalog`. When down, transactional DDL/DML operations cannot proceed.
  * Schema mutations and writes via the Iceberg REST Catalog gateway (`WriteRouteGate`) are permitted exclusively for `default-catalog` tables. Consequently, Iceberg write operations are halted.

---

## 5. Iceberg REST Catalog Gateway Behavior

* When the backend backing an Iceberg catalog is unreachable, the HTTP handler ([`IcebergHttpHandler`](src/main/java/io/github/mmalykhin/hmsproxy/restcatalog/IcebergHttpHandler.java)) translates metastore exceptions into standard Iceberg REST responses:
  * The client receives an **HTTP 500 (Internal Server Error)** or **HTTP 503 (Service Unavailable)**.
  * The response body contains a structured `ErrorResponse` JSON with error details and messages expected by Iceberg REST clients (Spark, Trino, Flink, PyIceberg).

---

## 6. Monitoring, Health Checks, and Observability

The management HTTP server ([`ManagementHttpServer`](src/main/java/io/github/mmalykhin/hmsproxy/app/ManagementHttpServer.java)) exposes endpoints and metrics for backend health tracking:

### 6.1. Health Endpoints
* **`/healthz` (Liveness probe)**:
  * Always returns **HTTP 200 OK** (`{"status":"ok","alive":true,...}`) as long as the proxy JVM process is running and accepting HTTP connections. Used for container liveness probes.
* **`/readyz` (Readiness probe)**:
  * Verifies connectivity across all configured backends.
  * If any backend is unreachable, disconnected, or has an `OPEN` circuit breaker, the endpoint returns **HTTP 503 Service Unavailable** (`{"status":"degraded","backendConnectivity":false,...}`).
  * Load balancers (HAProxy, Envoy, Kubernetes Ingress/Service) use this response to automatically remove the proxy instance from the active routing pool.

### 6.2. Background Health Polling
* When `routing.backend-state-polling.enabled=true` is set, a background scheduler periodically tests backend reachability using lightweight `getStatus` calls with a configurable `probe-timeout-ms`.
* This detects backend failures and recoveries proactively without waiting for user traffic or blocking external `/readyz` scrapes.

### 6.3. Prometheus Metrics (`/metrics`)
Backend outages update several key observability counters:
* `hms_proxy_backend_failures_total{backend="<name>", error="<class>"}` — total backend failure count by error type.
* `hms_proxy_backend_status{backend="<name>", state="connected|degraded"}` — current backend connectivity status.
* `hms_proxy_circuit_state{backend="<name>"}` — current Circuit Breaker state (`0 = CLOSED`, `1 = OPEN`, `2 = HALF_OPEN`).
* `hms_proxy_backend_session_acquire_timeouts_total{backend="<name>", reason="borrow|reconnect"}` — session acquisition timeouts under overload or backend unresponsiveness.
* `hms_proxy_impersonation_session_evictions_total{backend="<name>", reason="transport_failure"}` — impersonation sessions dropped due to transport errors.
