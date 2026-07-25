# Changelog

This changelog summarizes the full commit history of the repository from the first commit through
`2026-07-25`. Entries are grouped by commit date and focused on user-visible changes. The first
tagged release, `v1.0.0`, was cut on 2026-04-29.

For a Russian version, see [CHANGELOG.ru.md](CHANGELOG.ru.md).

## 2026-07-25

### Fixed

- The bundled default `log4j.properties` no longer silently discards proxy output. The proxy
  package logger was set to `DEBUG` with `additivity=false` and no appenders of its own, so every
  proxy line — including the structured audit record — was rendered and then thrown away. The
  audit logger now has its own appender, `logs/hms-proxy-audit.log` (rolling at 100MB with 10
  backups, no layout prefix so the file stays valid JSON lines).
- Per-request debug tracing is off by default. The proxy package now logs at `INFO`, so
  `DebugLogUtil` no longer renders every request argument and backend response on every RPC. Set
  `log4j.logger.io.github.mmalykhin.hmsproxy=DEBUG` to opt back in.
- The management HTTP listener now serves requests from a dedicated thread pool
  (`management.threads`, default 4) instead of the single built-in dispatcher thread. A `/readyz`
  call blocked on an unreachable backend or KDC no longer stalls `/healthz` liveness checks or
  `/metrics` scrapes.
- `DebugLogUtil` renders into a single budget-bounded buffer, so a collection of large Thrift
  objects stops being materialized once the ~4000-character budget is spent instead of building and
  clipping each element in full.

### Changed

- Removed the `logs/hms-proxy-daily.log` `DailyRollingFileAppender` from the default logging
  config. It had no backup limit and grew without bound, and with three root appenders every
  third-party log line was written three times. The default is now stderr plus the size-bounded
  `logs/hms-proxy.log`.
- `/readyz` caches backend and Kerberos probe results for `management.readiness-cache-ms` (default
  2000) and refreshes them single-flight, so frequent scrapes no longer fan out one round of
  network probes per request. The response carries a new `probeAgeMs` field; set the property to
  `0` to probe on every request. Per-backend state fields are still rendered from current runtime
  state on every call.
- Documented that management endpoints are unauthenticated and that `/readyz` exposes Kerberos
  principals and backend error details, so the port belongs on an isolated monitoring network.

## 2026-05-26

### Added

- Hive 4.1.x backend adapter. `APACHE_4_1_0` can now be configured as a
  per-catalog backend runtime profile (`catalog.<name>.runtime-profile=APACHE_4_1_0`)
  when the catalog's external HMS already runs Hive 4. The new `Hive4BackendAdapter`
  upgrades the two positional read methods Hive 4 removed (`get_table`,
  `get_table_objects_by_name`) to their `*_req` equivalents and unwraps the
  response back to the Apache 3.1.3 return type; everything else flows through
  the standard isolated `IMetaStoreClient` and the binary-compatible Thrift
  delegation. `BackendRuntime` and `BackendInvocationSession` now activate the
  isolated classloader for any profile whose new `MetastoreRuntimeProfile#requiresIsolation()`
  returns true (Hortonworks 3.1.0.x or Hive 4.1.0).

## 2026-05-25

### Added

- Multiple Thrift front-end listeners on different ports via
  `additional-frontends.<name>.*`. Each additional listener advertises its own
  `frontend-profile` (and uses its own `standalone-metastore-jar` for
  non-`APACHE_3_1_3` profiles) but shares the same `RoutingMetaStoreProxy`,
  federation, security, audit and Prometheus stack with the primary listener.
  This unblocks running, for example, an Apache 3.1.3 listener on 9083 and a
  Hortonworks 3.1.0.x listener on 9084 in the same JVM; clients have to be
  routed to the right port because the Thrift protocol has no version
  negotiation. Validation: unique listener names, unique `bindHost:port`
  bindings, port collision with primary rejected, readable jar required for
  non-Apache profiles.
- Hive 4.1.x front-door bridge (`compatibility.frontend-profile=APACHE_4_1_0`).
  Accepts Hive 4 Thrift clients and serves them against an Apache 3.1.3 backend
  via an isolated classloader and a dynamic Proxy, symmetric to the existing
  `HortonworksFrontendBridge`. Covers the 199 methods shared with Apache 3.1.3
  via binary-compatible Thrift delegation, plus explicit positional mappings for
  the Hive 4-only `*_req` wrappers most clients reach for on the read and
  standard-DDL paths (`get_database_req`, `get_databases_req`, `get_table_req`,
  `get_partition*_req`, `get_fields_req`, `create_table_req`, `drop_table_req`,
  `alter_table_req`, `truncate_table_req`, etc.). Truly Hive 4-only APIs (data
  connectors, scheduled queries, stored procedures, packages, ACID v2
  extensions) respond with `TApplicationException UNKNOWN_METHOD`.
- `hive-metastore/hive-standalone-metastore-common-4.1.0.jar` bundled for the
  isolated frontend runtime.
- `APACHE_4_1_0` enum value in `FrontendProfile` and `MetastoreRuntimeProfile`.
  The latter rejects being used as a backend (`BackendAdapterFactory` throws)
  — Hive 4 is supported as a front-door profile only.

## 2026-05-19

### Changed

- The isolated backend classloader is now reused across `BackendRuntime` reloads of the same
  profile + jar pair instead of being rebuilt every time. Reduces classloader churn (and the
  metaspace/PermGen-style pressure that came with it) when several catalogs share the same
  isolated runtime, and shortens reconnect/reload latency.

## 2026-05-03

### Added

- New `Bump version series to 1.0` switch for nightly artifacts: jgitver now produces
  `hms-proxy-1.0.<distance>-<sha>.jar` instead of `0.1.<distance>-<sha>.jar`, matching the
  intended release series after `v1.0.0`. Tagged builds are unaffected (`hms-proxy-1.0.0.jar`
  at `v1.0.0`).

### Changed

- `/readyz` backend probes now run on a dedicated bounded executor sized by
  `routing.backend-state-polling.max-parallelism`, with a shared deadline propagated through
  `probeConnectivity(timeoutMs)` so the socket itself honours the probe budget. Previously,
  when backend-state polling was disabled, every readiness request fanned out
  `checkConnectivity()` through the common `ForkJoinPool` and joined without any timeout,
  letting a slow or hung HMS turn `/readyz` into a load source and starve the common pool.
  The probe executor is shut down with the management server.

### Fixed

- Parallel fanout workers no longer mutate the parent `RequestObservation` through
  `ThreadLocal` propagation. Each worker now owns a throwaway observation and surfaces the
  compat-fallback signal back through `FanoutTaskResult`, so the parent observation is updated
  only on the request thread (previously compat-fallback paths from worker threads could race
  on non-volatile state).

## 2026-05-02

### Changed

- Prometheus metrics now bound label cardinality. The `exception` label on
  `hms_proxy_backend_failures_total` and `hms_proxy_synthetic_read_lock_store_failures_total`
  is normalized against a known-exception whitelist; unknown exception classes collapse to
  `other`. Each metric also enforces a soft cap of 5000 distinct label series — once reached,
  new label combinations are routed to a single `overflow` series instead of growing the
  internal map and Prometheus output without bound.

- Adaptive socket timeout now throttles backend reconnects to prevent reconnect storms under
  volatile latency. Hysteresis was widened from a fixed 1 s delta to `max(2 s, 25 % of the
  current applied timeout)`, and a configurable cooldown
  (`routing.adaptive-timeout.reconnect-cooldown-ms`, default 30 s) blocks back-to-back
  reconnects. Each reconnect previously evicted the impersonation cache and forced a full
  Kerberos re-login, which made oscillation costly.

- **Impersonation:** each user now gets a per-user borrow/return pool of backend Thrift
  sessions instead of a single shared session serialized through one transport. Pool size and
  idle TTL are configurable per catalog via `catalog.<name>.impersonation-pool-max-size`
  (default `4`) and `catalog.<name>.impersonation-session-idle-ttl-ms` (default `0` = never
  close idle). Borrow timeout is bounded by the catalog's `latency-budget-ms`; transport
  failures discard only the faulted session and retry once on a fresh one. Adaptive-timeout
  reconnects and per-user LRU eviction close all sessions held by the affected user.

- Backend session pool borrow now fails fast instead of waiting forever. The shared pool
  borrow path uses `tryAcquire` bounded by the catalog's `latencyBudgetMs` (or 30 s default);
  exhaustion logs a warning and surfaces as `MetaException` to the client. The pool's
  `reconnectShared()` and `close()` paths use the same bounded acquire to prevent management
  operations from hanging when in-flight RPCs hold permits.

- Backend health polling is now parallel and bounded. The new
  `routing.backend-state-polling.max-parallelism` knob (default: number of catalogs) sizes a
  dedicated `ThreadPoolExecutor` that submits all probes concurrently under a shared deadline,
  replacing the sequential `Future.get` per backend. With 20 backends and a 5 s timeout, a
  poll cycle drops from roughly 100 s to roughly 5 s.

### Added

- Two new Prometheus counters expose adaptive-timeout dynamics:
  `hms_proxy_adaptive_timeout_reconnect_total{catalog}` for applied reconnects and
  `hms_proxy_adaptive_timeout_reconnect_skipped_total{catalog,reason}` for events suppressed
  by hysteresis or cooldown. The bundled Grafana dashboard ships with three new panels — an
  overall reconnect rate stat, per-catalog reconnect timeseries, and a stacked breakdown of
  suppressed events by reason.

- New Prometheus metrics for the per-user impersonation pool:
  `hms_proxy_impersonation_pool_users{catalog}` (distinct users currently cached),
  `hms_proxy_impersonation_pool_sessions{catalog,state=active|idle}` (sessions by state),
  `hms_proxy_impersonation_session_acquire_timeouts_total{catalog}` (per-user borrow
  timeouts) and
  `hms_proxy_impersonation_session_evictions_total{catalog,reason=idle|transport_failure|user_evicted|user_capacity}`.
  The Grafana dashboard ships with a new "Impersonation Pool" section with four panels for
  these metrics.

- New Prometheus counter `hms_proxy_backend_session_acquire_timeouts_total{catalog,operation}`
  for fail-fast events on the shared backend session pool. `operation=borrow` covers regular
  RPC dispatch; `operation=reconnect` covers admin reconnect attempts that could not quiesce
  the pool. Matching panels were added to the Grafana dashboard.

### Fixed

- `Gauge` values are now stored as `AtomicLong` (via `Double.doubleToRawLongBits`) instead of
  `DoubleAdder`, giving lock-free atomic `set()`/read. The previous `DoubleAdder` path used
  `add(-current); add(value)` under a lock, which let concurrent readers observe a partial
  update between the two adds.
- Reflection cache for `ThriftReflectionCache` switched from a static `ConcurrentHashMap`
  keyed by `Class<?>` to `ClassValue` so that entries are tied to the `Class` lifecycle and
  released when the isolated runtime is reloaded. Prevents classloader leaks on repeated
  isolated-runtime reloads.
- `IsolatedInvocationBridge` and `TBase` cross-classloader conversion now cache `Method` and
  `Constructor` lookups, eliminating repeated `getMethod`/`getConstructor` reflection on hot
  paths.

## 2026-04-29

### Added

- Console log output is now also written to two file appenders: `logs/hms-proxy.log` (rolling at
  50MB with 10 backups) and `logs/hms-proxy-daily.log` (date-suffixed). Log history survives
  restarts and is available for offline analysis.

### Changed

- Rewrote the Grafana dashboard to cover all 13 exported metrics, grouped into six sections —
  Requests & Latency, Backend Operations, Routing, Rate Limiting, Metadata Filtering, Synthetic
  Read Locks. Added panels for the previously missing `hms_proxy_rate_limited_total`,
  `hms_proxy_filtered_objects_total`, and `hms_proxy_synthetic_read_lock_store_info` metrics.
- Restructured GitHub Actions release workflows around a single reusable `_release-build.yml`
  pipeline. The manual `Release` dispatch now only computes the next `vX.Y.Z` and prints
  instructions for creating a signed tag locally; pushing the tag triggers `Tag Release` which
  builds and publishes. Pushes to `main` publish a rolling `nightly` prerelease that replaces the
  previous per-commit `build-*` and dated `nightly-*` releases.

### Fixed

- The Maven artifact version on tagged commits now reflects the tag (for example,
  `hms-proxy-1.0.0.jar` at `v1.0.0`) instead of the snapshot pattern. The jgitver
  `tagVersionPattern` was hardcoded to the same expression as the non-tagged path; it is now set
  to the default `${v}`. Snapshot builds on non-tagged commits keep the existing
  `0.1.<distance>-<sha>` naming.

## 2026-04-28

### Fixed

- When the management HTTP or metastore Thrift listener cannot bind its configured `host:port`
  (for example, the port is already in use), the proxy now logs an explicit ERROR identifying
  which listener failed and why before letting the exception propagate, instead of emitting only
  a raw stack trace on its way to a non-zero exit.

## 2026-04-20

### Changed

- Replaced the per-catalog single shared backend session and `synchronized` invocation gate with a
  borrow/return pool sized by `catalog.<name>.shared-session-pool-size` (default `1`). Non-impersonated
  calls to the same catalog can now run in parallel up to the pool size instead of serializing
  through one Thrift transport. **Note:** the default of `1` preserves the previous serialized
  behavior — to actually benefit from parallelism, set `catalog.<name>.shared-session-pool-size`
  explicitly (e.g. `8` or `16`) per catalog. Higher values keep more idle Thrift sessions open to the
  backend HMS (with proportional Kerberos cost when applicable) and lengthen `reconnectShared` drains.
- **Breaking:** `synthetic-read-lock.store.mode` must now be set explicitly, both in the
  properties file and when building `ProxyConfig` programmatically. The previous silent
  `IN_MEMORY` default was unsafe for multi-instance deployments — synthetic SELECT locks on
  non-default catalogs were lost on proxy restart or load-balancer failover without any signal at
  startup. Choose `IN_MEMORY` for single-instance setups (the startup `WARN` about lost SELECT
  locks still fires) or `ZOOKEEPER` for HA. If `synthetic-read-lock.store.zookeeper.*` is
  configured, `ZOOKEEPER` is inferred. In-process builders can use the new helper
  `ProxyConfig.SyntheticReadLockStoreConfig.inMemory()`.
- Refactored configuration and operation-policy internals by splitting nested config records into
  top-level types, reorganizing the config package into topical subpackages, and decomposing the
  HMS operation registry into per-category contributors. Public behavior is unchanged outside the
  explicit synthetic-read-lock configuration requirement above.

### Fixed

- Single-shot transport-failure retry now discards only the failed pooled session instead of
  resetting the entire shared connection.
- `TApplicationException` is no longer treated as a backend transport failure, avoiding pointless
  retries against healthy servers for dispatch-level errors such as unsupported HDP wrapper RPCs.
- Silenced non-critical compile and test warnings, and added a minimal test logging configuration
  to avoid noisy "No appenders could be found" output in the test suite.

### Docs

- Documented shared-session pool tuning guidance, including the need to set
  `catalog.<name>.shared-session-pool-size` explicitly to get parallelism and the operational
  trade-offs around idle sessions, Kerberos cost, and reconnect drain time.

## 2026-04-19

### Changed

- Hardened backend health probing and `/readyz`: probes now use ephemeral sessions, `/readyz`
  checks backends in parallel, and its JSON escaping now covers control characters fully.
- Refactored routing and configuration internals into smaller components: renamed
  `RoutingMetaStoreHandler` to `RoutingMetaStoreProxy`, split `BackendCallDispatcher`,
  `RoutingHandler`, and `ProxyConfigLoader` into focused collaborators and parsers, and
  consolidated per-RPC metadata into declarative policy registries.
- Reduced package coupling across routing, config, backend, frontend, federation, and utility
  layers by moving shared types to more appropriate packages and routing against the
  `FederationOperations` interface.

### Fixed

- Fixed parallel fanout head-of-line blocking by harvesting completed futures under a shared
  deadline instead of waiting on each backend sequentially.
- Backend health probes no longer mutate live shared sessions or evict impersonation clients when
  probe timeouts drift.
- Fixed stale `capabilities.yaml` references after the `RoutingMetaStoreProxy` rename.

## 2026-04-18

### Added

- Added configuration knobs for per-request hedged-read fanout deadlines, backend-state probe
  deadlines, and impersonation-client cache sizing and idle TTL.

### Changed

- Improved routing hot paths by replacing the synchronized rate limiter with a lock-free GCRA
  implementation and caching Thrift reflection used in namespace translation and table-name
  extraction.
- Refactored routing, namespace translation, and handler wiring into smaller components to reduce
  package coupling and improve unit-test coverage.

### Fixed

- Bounded parallel fanout and backend-state probes so hung backends cannot block requests or starve
  the single-threaded poller indefinitely.
- Cancelled pending fanout futures on timeout to prevent thread-pool exhaustion.
- Fixed blocking reconnect I/O under synchronization in backend clients.
- Fixed a `ThreadLocal` leak in `RoutingMetaStoreHandler`.
- Added cycle detection in namespace translation to avoid infinite recursion on cyclic Thrift object
  graphs.

## 2026-04-15

### Added

- Added best-effort external-table drop purge that removes table data from the routed catalog
  filesystem when enabled.

### Docs

- Documented external-table drop purge configuration and behavior in both READMEs and the example
  properties file.

## 2026-04-14

### Added

- Added external-table location rewrite for routed catalogs, with the source filesystem defaulting
  to the default catalog when not configured explicitly.

### Changed

- Made configuration mode parsing case-insensitive, including view rewrite modes.

### Fixed

- Fixed view-definition compatibility handling for statistics requests that use union payloads.

## 2026-04-13

### Docs

- Expanded smoke coverage and smoke guides for view-rewrite and UDF scenarios, including the
  real-installation smoke script.

## 2026-04-07

### Added

- Added `REWRITE_TO_NON_TRANSACTIONAL` and `REWRITE_MANAGED_TO_EXTERNAL` transactional DDL guard
  modes, and renamed the existing modes for clarity.

### Fixed

- Silenced benign SASL `ERROR` logs caused by probe connections that open a socket without sending
  SASL data.

## 2026-04-06

### Changed

- Updated the packaged build to ship project dependencies in a separate `lib/` directory.

### Fixed

- Fixed Hortonworks frontend compatibility so HDP-only exceptions and `alter_partitions_req`
  payloads are translated across the classloader boundary correctly.
- Corrected ACID routing so `allocate_table_write_ids` and `get_valid_write_ids` reach the default
  backend, while transactional table creation on non-default catalogs now fails fast with a clear
  `MetaException`.

## 2026-04-05

### Changed

- Refactored request handling into an interceptor chain that separates rate limiting, lock
  handling, compatibility adaptation, and routing.

### Fixed

- Fixed request-context loss in parallel fanout tasks and removed an unbounded fanout queue.

## 2026-04-04

### Added

- Added latency-aware backend routing with per-catalog latency budgets, adaptive timeouts,
  circuit-breaker state with half-open retry, optional backend-state polling, and degraded routing
  for safe read-only fanout RPCs.
- Added request overload protection with token-bucket rate limits for client principal, source IP,
  source CIDR pools, HMS method families, catalogs, and high-risk RPC classes.
- Added dedicated protection classes for `write`, `ddl`, `txn`, and `lock` RPCs.
- Added Prometheus visibility for throttled requests via `hms_proxy_rate_limited_total` and
  `status="throttled"` in `hms_proxy_requests_total`.

### Docs

- Documented latency-aware routing knobs, per-catalog latency budgets, and the expanded `/readyz`
  backend-state payload in both READMEs and the example properties file.
- Documented overload-protection configuration and operating model in both READMEs and the example
  properties file.

## 2026-04-03

### Added

- Added a synthetic proxy read-lock shim for non-ACID `SELECT` flows on non-default catalogs.
- Added direct smoke coverage for synthetic non-transactional `NO_TXN` lock flows, including
  `CREATE TABLE`-style DB locks and partition rename/drop style locks on non-default catalogs.
- Added ZooKeeper-backed persistence for synthetic read-lock state so transactions can continue
  through another proxy instance after failover.
- Added synthetic lock observability: Prometheus metrics, active-lock gauges, handoff counters,
  store-failure counters, and dashboard panels for synthetic lock activity.

### Changed

- Persistent token-store RPCs are now handled locally by the proxy instead of being forwarded.
- Backend lock failures are now surfaced as `MetaException` results for clearer client behavior.

### Fixed

- Fixed namespace-less HMS routing policy and documented its current behavior.
- Fixed synthetic lock handling for non-transactional `NO_TXN` DDL locks on non-default catalogs,
  including `CREATE TABLE` and partition rename flows routed through Hive txn/lock APIs.
- Ensured front-door security starts before backend runtimes.
- Avoided UGI fallback before the front-door keytab login is established.
- Configured ZooKeeper SASL JAAS before token manager startup.
- Fixed the ZooKeeper integration test so environments that cannot bind local ports now skip the
  embedded `TestingServer` case instead of failing the whole suite.

### Docs

- Documented ZooKeeper token-store credentials, overrides, namespace-less routing behavior, and the
  expanded synthetic `NO_TXN` lock smoke scenarios.

## 2026-04-02

### Added

- Added management HTTP endpoints for health, readiness, and metrics.
- Added Prometheus metrics and the initial Grafana dashboard bundle.
- Added structured audit logging and Kerberos readiness checks.
- Added per-catalog access modes.
- Added support for Hortonworks `3.1.5` metastore runtimes.
- Added HDP passthrough support for table extensions and materialized views.
- Added a view-definition rewrite compatibility layer.
- Added GitHub Actions CI.

### Changed

- Separated compatibility and federation layers to simplify routing and translation flow.
- Refactored routing policy to be independent from the compatibility bridge.
- Added compatibility fallbacks for more HDP request paths.
- Cached unsupported wrapper RPC detection for Hortonworks backends.
- Aligned Curator dependencies for the fat JAR.

### Fixed

- Enabled Kerberos authentication for `ZooKeeperTokenStore`.
- Ensured the front-door ZooKeeper token store uses the keytab login user.
- Limited transactional DDL mode to managed tables.

### Docs

- Expanded observability documentation.
- Added compatibility and test matrices.
- Clarified proxyuser versus ZooKeeper configuration.
- Updated general documentation around the new management and compatibility features.

## 2026-04-01

### Added

- Added a manual HMS smoke client.
- Added a transactional DDL guard.

### Changed

- Unified transactional DDL guard configuration and behavior.
- Generalized HDP request compatibility handling.
- Improved smoke test scenarios and coverage.
- Added `jgitver`-based versioning support.

### Fixed

- Fixed several metastore routing edge cases.

## 2026-03-31

### Added

- Added the Hortonworks front-end compatibility bridge.
- Added Russian documentation and bilingual smoke guides.
- Added vendored standalone metastore JARs for supported runtimes.

### Changed

- Refactored metastore runtimes and expanded Hortonworks bridge coverage.
- Clarified transaction routing policy for multi-catalog mode.
- Pinned ACID lifecycle RPC routing to the default catalog.
- Reorganized the repository by module and package.
- Split source and tests into package-based layout.
- Added fallback to the Apache runtime for selected HDP cases.

### Fixed

- Resolved `_HOST` Kerberos principals.
- Fixed isolated Hive class loading.
- Fixed HDP isolation regressions introduced during refactoring.
- Fixed the application main-class package.

## 2026-03-30

### Docs

- Clarified front-door delegation-token proxyuser requirements.

## 2026-03-28

### Changed

- Narrowed compatibility routing that falls back to the default backend.

## 2026-03-27

### Added

- Added managed and ACID table support with regression coverage.
- Added shared backend `HiveConf` overrides.

### Changed

- Preserved backend catalog names during compatibility internalization.
- Kept default catalog names unprefixed when translating namespaces.

### Fixed

- Applied a batch of routing and compatibility fixes around multi-catalog behavior.

## 2026-03-26

### Added

- Added ZooKeeper-backed storage for token-related state.
- Added `routing.catalog-db-separator` configuration.

### Changed

- Split impersonation logic into clearer paths and refactored related request handling.

### Fixed

- Applied a broad set of fixes around token storage, routing, and request handling.

## 2026-03-25

### Added

- Added per-user caching for impersonation flows.
- Added front-door delegation-token support.
- Added test coverage for global-function handling.

### Fixed

- Fixed `get_all_functions()` and related global-function paths.
- Fixed keytab handling and several delegation-token and impersonation issues.

## 2026-03-23

### Added

- Added client keytab support.
- Added initial impersonation support.

### Fixed

- Applied the first stabilization fixes for authentication and request flow.

## 2026-03-19

### Changed

- Added debug logging and refined logging configuration.
- Updated dependencies used in the fat-JAR build.

### Fixed

- Fixed log configuration issues discovered during early packaging work.

## 2026-03-17

### Added

- Added Maven Shade Plugin support for fat-JAR packaging.

### Docs

- Expanded the security section with Kerberos and non-Kerberos configuration examples.

### Fixed

- Removed the unnecessary tools dependency from the runtime path.

## 2026-03-16

### Fixed

- Applied an early round of stabilization fixes after the initial bootstrap.

## 2026-03-12

### Added

- Initial repository bootstrap.
- First working implementation commit.
