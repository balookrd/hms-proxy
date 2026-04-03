# Changelog

This changelog summarizes the full commit history of the repository from the first commit through
`2026-04-03`. The project has not published tagged releases yet, so entries are grouped by commit
date and focused on user-visible changes.

For a Russian version, see [CHANGELOG.ru.md](CHANGELOG.ru.md).

## 2026-04-03

### Added

- Added a synthetic proxy read-lock shim for non-ACID `SELECT` flows on non-default catalogs.
- Added ZooKeeper-backed persistence for synthetic read-lock state so transactions can continue
  through another proxy instance after failover.
- Added synthetic lock observability: Prometheus metrics, active-lock gauges, handoff counters,
  store-failure counters, and dashboard panels for synthetic lock activity.

### Changed

- Persistent token-store RPCs are now handled locally by the proxy instead of being forwarded.
- Backend lock failures are now surfaced as `MetaException` results for clearer client behavior.

### Fixed

- Fixed namespace-less HMS routing policy and documented its current behavior.
- Ensured front-door security starts before backend runtimes.
- Avoided UGI fallback before the front-door keytab login is established.
- Configured ZooKeeper SASL JAAS before token manager startup.

### Docs

- Documented ZooKeeper token-store credentials, overrides, and namespace-less routing behavior.

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
