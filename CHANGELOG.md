# Changelog

This changelog summarizes the full commit history of the repository from the first commit through
`2026-04-21`. The project has not published tagged releases yet, so entries are grouped by commit
date and focused on user-visible changes.

For a Russian version, see [CHANGELOG.ru.md](CHANGELOG.ru.md).

## 2026-04-21

### Changed

- **Breaking:** `synthetic-read-lock.store.mode` must now be set explicitly. The previous silent
  `IN_MEMORY` default was unsafe for multi-instance deployments — synthetic SELECT locks on
  non-default catalogs were lost on proxy restart or load-balancer failover without any signal at
  startup. Choose `IN_MEMORY` for single-instance setups (the startup `WARN` about lost SELECT
  locks still fires) or `ZOOKEEPER` for HA. If `synthetic-read-lock.store.zookeeper.*` is
  configured, `ZOOKEEPER` is inferred.

## 2026-04-20

### Changed

- Replaced the per-catalog single shared backend session and `synchronized` invocation gate with a
  borrow/return pool sized by `catalog.<name>.shared-session-pool-size` (default `1`). Non-impersonated
  calls to the same catalog can now run in parallel up to the pool size instead of serializing
  through one Thrift transport. **Note:** the default of `1` preserves the previous serialized
  behavior — to actually benefit from parallelism, set `catalog.<name>.shared-session-pool-size`
  explicitly (e.g. `8` or `16`) per catalog. Higher values keep more idle Thrift sessions open to the
  backend HMS (with proportional Kerberos cost when applicable) and lengthen `reconnectShared` drains.

### Fixed

- Single-shot transport-failure retry now discards only the failed pooled session instead of
  resetting the entire shared connection.
- Silenced non-critical compile and test warnings, and added a minimal test logging configuration
  to avoid noisy "No appenders could be found" output in the test suite.

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
