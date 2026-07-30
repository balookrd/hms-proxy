# Closing the pointer guard's read-then-write race with the Iceberg table lock

Design for making `IcebergTablePointerGuard`'s repair atomic against a concurrent Iceberg commit,
by holding the same Hive table lock Iceberg itself takes across the guard's read and the backend's
`alter_table`.

## Problem

The guard reads the table's current record (`get_table`) and the backend then applies
`alter_table`. Those are two calls, so a commit that lands between them is still overwritten.

On Hive 4 backends that is no longer data loss: the repaired alter carries
`expected_parameter_key`/`expected_parameter_value`, and the metastore fails it loudly. On the 3.1
line - Apache 3.1.3 and HDP 3.1.0, which is all of today's production traffic - the metastore
ignores both keys, so the window stays open (verified by grep over the metastore jars). This is
the "Out of scope" item of
`2026-07-29-iceberg-pointer-guard-metastore-keyed-design.md`.

## What the stand shows about the locks involved

Measured on the Kerberos stand with `hdp` as the default catalog, one SQL `INSERT` into an Iceberg
table (proxy trace log, single run):

| time | call | lock state |
| --- | --- | --- |
| `08,045` | Hive locks for its own transaction (txnid 957) | `LockComponent(db=_dummy_database, table=_dummy_table)` and nothing else |
| `08,249` | `alter_table_with_environment_context`, `alterTableOpType=DROPPROPS` - the request the guard repairs | no lock held on the table |
| `12,982` | HiveServer2's Iceberg commit takes its lock | `LockRequest(txnid=0, components=[LockComponent(db=default, table=<table>)])` |
| `13,033` | that commit's `alter_table` | **inside** the lock above |
| `13,097` | `unlock` | the lock was held 115 ms |

Two facts follow, and they decide the design:

1. **Hive takes no lock on the target table of the `INSERT`.** Only the `_dummy_database`
   placeholder. So a lock the guard takes while serving that `INSERT`'s alter cannot queue behind
   a lock the same statement holds.
2. **A genuine Iceberg commit calls `alter_table` from inside the table lock.** Anything that
   acquires that lock before deciding what kind of alter it is would therefore block on a lock
   held by the very caller waiting for its answer - a self-deadlock on every honest commit,
   released only by a timeout.

The lock both writers take is identical in shape, and `MetastoreLock.createLock` is byte-for-byte
the same in Iceberg 1.6.1 (the `iceberg-hive-runtime` inside HiveServer2) and 1.9.2 (the proxy's
own REST path):

```java
LockComponent component = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, databaseName);
component.setTablename(tableName);
LockRequest request = new LockRequest(List.of(component), user, hostName);
request.setAgentInfo(agentInfo);   // Hive 2+
```

No `txnid`, no `operationType`, table level, EXCLUSIVE. `iceberg.engine.hive.lock-enabled`
defaults to `true` (`TableProperties.HIVE_LOCK_ENABLED_DEFAULT`), which the trace above confirms
in practice on both front doors.

The database name has to be the **backend** one: `LockRequestSplit` rewrites every component of a
client's lock request into backend names before forwarding it, so a guard lock built from
`namespace.backendDbName()` names the same object as both writers' locks.

## Decision

**Read first, lock only to repair.**

```
get_table (no lock)
  -> not an Iceberg table, or a forward commit  -> return, no lock was ever requested
  -> a repair is needed:
       lock          (EXCLUSIVE, TABLE, backendDb.table - Iceberg's exact request)
       get_table     (again: the record may have moved while we waited)
       merge + CAS   (over the record read under the lock)
       alter_table   (still under the lock)
       unlock
```

The extra read under the lock is what makes the repair atomic; without it the guard would hold the
lock over a value read before it. The lock is requested only on the path that rewrites the
request, which is the DROPPROPS alter opening an `INSERT` - two per SQL writer on the stand - so
ordinary Hive traffic and honest Iceberg commits pay nothing.

Because the guard now spans the backend call, `protectPointer` becomes `protect(...)` returning an
`AutoCloseable` that releases the lock, and `RoutingHandler` wraps the `invokeDirect` of the alter
in it. Everything about locking and about Iceberg stays inside the guard.

**A repair does not disturb concurrent committers.** The merge keeps `metadata_location`
unchanged, so a commit that was waiting for the lock still finds the pointer it based itself on
and proceeds.

## When the lock is not granted

Never refuse the alter: refusing would fail an ordinary Hive write whenever the metastore's lock
table hiccups, which is a worse failure than the one being prevented. So the guard waits up to
`lock-acquire-timeout-ms` (default 10 s) polling `check_lock` with a 50 ms → 500 ms backoff, and
on timeout, error, or a lock left in `WAITING` applies today's behaviour: merge the request and
let it through unprotected, with its own counter value.

10 s is chosen against what actually holds the lock - a commit, 91-115 ms on the stand - with room
for a large manifest write, while staying far below a HiveServer2 metastore client's socket
timeout, so a wait can never surface at the client as a transport error. `0` means one attempt and
no waiting.

Backoff bounds are not configurable: they are one timer with the timeout, and a second knob would
only let the two contradict each other.

## Heartbeat, and releasing the lock

**No heartbeat.** The lock is held across two RPCs with no file I/O - tens of milliseconds - while
`hive.txn.timeout` is 300 s by default, three orders of magnitude above it. A heartbeat would mean
a scheduled task per repair for a hold that ends before the first tick.

The real hazard is the opposite one: a lock that is never released. A standalone 3.1 metastore
without the ACID housekeeping service does not reap it, and a leaked EXCLUSIVE lock blocks every
later commit on that table. So:

- release runs in a `finally`, and again once on failure;
- a release that still fails is logged at ERROR with the lock id and counted;
- `lock`, `check_lock` and `unlock` bypass rate limiting and the circuit breaker
  (`enforceRateLimit=false`): an admission rejection between `lock` and `unlock` would leak the
  lock. The guard's `get_table` keeps going through the metered path, as today.

## Non-default catalogs: no lock, on purpose

A lock taken on a non-default catalog's backend would be real but would guarantee nothing: writers
of those catalogs are served by the synthetic shim, which grants locks without checking conflicts
and never forwards them to the backend, so no other party contends for the object the guard would
hold. The guard therefore skips the lock there and still merges, with its own counter value. Their
writes are refused by `WriteRouteGate` on the REST path anyway; this keeps the Thrift path honest
rather than paying two RPCs for an illusion.

## Configuration

| Key | Default | Meaning |
| --- | --- | --- |
| `routing.iceberg-pointer-guard.lock-enabled` | `true` | Take the Iceberg table lock around a repair |
| `routing.iceberg-pointer-guard.lock-acquire-timeout-ms` | `10000` | Wait budget; `0` = one attempt, no waiting; negative is rejected at startup |

Parsed strictly like the rest of `routing.*` (`PropertyReader.getBoolean`,
`getNonNegativeLong`) - no silent fallback.

## Observability

Same counter, `hms_proxy_iceberg_pointer_guard_events_total{catalog,outcome}`, with new `outcome`
values alongside the existing ones:

| Value | Meaning |
| --- | --- |
| `repair_locked` | The repair was applied under the table lock |
| `repair_lock_timeout` | The lock was not granted within the budget; merged unprotected |
| `repair_lock_failed` | `lock`/`check_lock` failed; merged unprotected |
| `repair_lock_skipped` | Lock deliberately not taken (non-default catalog, or `lock-enabled=false`) |
| `lock_release_failed` | `unlock` failed after a retry - a lock may be stranded on the backend |

`repaired` keeps its meaning ("the merge was applied"), so the acceptance figures stay comparable
with the previous iteration's.

## Tests

In `RoutingMetaStoreProxyIcebergPointerGuardTest`, with the fake backend recording lock traffic:

- A forward commit takes **no** lock at all - the regression test for the self-deadlock the stand
  proved would happen.
- An ordinary Hive table takes no lock.
- A repair takes the lock, re-reads under it, and the alter carries the pointer read under the
  lock, not the one read before it - the case the whole change exists for.
- The lock request has Iceberg's exact shape: one EXCLUSIVE, TABLE component, backend db name,
  table name, no txnid.
- `unlock` runs when `alter_table` throws.
- Lock refused (`WAITING` forever) → the alter still goes through, merged, `repair_lock_timeout`.
- `lock` throwing → the alter still goes through, `repair_lock_failed`.
- A non-default catalog repairs without a lock; `lock-enabled=false` likewise.

Config parsing tests join the other `routing.*` ones: defaults, a rejected negative timeout, a
rejected non-boolean `lock-enabled`.

## Measurement

1. **Baseline**: 12 runs of
   `smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hdp --writers 4 --sql-writers 2
   --sql-engine hdp --kerberos` on the unchanged jar. The loss rate on the 3.1 line has never been
   measured - the earlier figures are all from `hive4`, whose CAS already turns a lost update into
   a loud failure.
2. **After**: the same command at least 12 times. Success is row counts matching the successful
   writers in every run **and** a non-zero `repaired` counter, plus `repair_locked` accounting for
   the repairs.
3. **Cost**: sections B and C of the matrix (both HiveServer2 instances) against the current
   figures in TEST-MATRIX, plus the guard counters, so the price of the locks on the SQL layer is
   a number and not an assertion.

## Documentation

`smoke-stand/TEST-MATRIX.md` and `.ru.md` (row I4 and its detail section),
`src/main/resources/hms-proxy-example.properties`, `CHANGELOG.md` with `CHANGELOG.ru.md`, and the
`AGENTS.md` routing note - the guard, and only the guard, takes the Iceberg table lock, and only
to repair.
