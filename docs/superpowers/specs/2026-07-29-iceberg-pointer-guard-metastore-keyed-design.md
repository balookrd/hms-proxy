# Iceberg pointer guard keyed off metastore state

Design for making `IcebergTablePointerGuard` fire on the `alter_table` shape HiveServer2 actually
sends, and for bounding the cost of the extra metastore read that requires.

## Problem

The guard exists to keep a Hive client's `alter_table` from rolling an Iceberg table's pointer
back (TEST-MATRIX section I, "I4 in detail"). It currently decides whether a request concerns an
Iceberg table by looking at the request: no `metadata_location` in the incoming `Table` means
"not an Iceberg table, return immediately".

Verified on the wire (stand-proxy log, `hive4-kerberos` profile): the
`alter_table_with_environment_context` HiveServer2 opens an `INSERT` with carries params exactly
`{EXTERNAL=TRUE, numFiles, numRows, totalSize, transient_lastDdlTime}` - no `metadata_location`
and no `table_type`. So the guard returns on its first check and is a no-op for the shape that
causes the loss. Six clean runs after the guard landed prove nothing: the WARN it logs when it
repairs a pointer never appeared, and at the observed one-in-eight loss rate a clean six-run
streak happens about 45% of the time anyway.

The metastore applies the parameters of an `alter_table` wholesale, so what that request erases is
not only `metadata_location`: every Iceberg key the record holds and the request omits goes with
it (`table_type`, `storage_handler`, `previous_metadata_location`, the `current-snapshot-*` set,
and any Iceberg table property).

## Decision

**Key the guard off what the metastore currently holds, not off what the client sent.** A table
is an Iceberg table if its metastore record has `metadata_location`. Three cases follow, for an
`alter_table*` whose target is an Iceberg table:

1. **The request carries no pointer at all** (today's real shape) - it would erase the Iceberg
   state. Repair it.
2. **The request carries a pointer that is not the current one and is not a forward commit** -
   it would discard a committed snapshot. Repair it.
3. **The request carries a pointer whose `previous_metadata_location` is the current pointer** -
   an honest Iceberg commit moving the table forward. Pass it through untouched.

"Repair" changes from "stitch the pointer back" to **merge**: the parameters the backend receives
are the metastore's current parameters as the base, the incoming parameters applied on top, and
`metadata_location` / `previous_metadata_location` forced back to the metastore's current values.
Everything the client meant to change (stats, `EXTERNAL`, `transient_lastDdlTime`, its own new
properties) still goes through; nothing the record held and the request omitted is lost.

Merging rather than restoring a fixed list of Iceberg keys is deliberate: the key set Iceberg
writes into HMS varies by Iceberg version, and a list would silently rot. The cost of merging is
that an `alter_table` which legitimately *removes* a property from an Iceberg table becomes a
no-op. That is acceptable because Hive's own `ALTER TABLE ... UNSET TBLPROPERTIES` on an Iceberg
table goes through `HiveIcebergMetaHook` and arrives as case 3 - a forward commit the guard does
not touch. Only a client that drops a property by talking to HMS directly, behind Iceberg's back,
is affected.

The compare-and-swap already implemented (`expected_parameter_key`/`expected_parameter_value`,
Hive 4 backends only) is attached to every repaired alter, case 1 included.

## Bounding the cost

Keying off the metastore means a `get_table` per `alter_table`, including ordinary Hive tables
where stats updates are frequent. Bound it with a **negative TTL cache**: `(catalog, backendDb,
table) -> "not an Iceberg table", valid until T`.

- Cache hit → the guard returns without any round trip. This is the case that matters: ordinary
  Hive tables, which is where the volume is.
- Cache miss → one `get_table`; a record without `metadata_location` is remembered as
  not-Iceberg, a record with one is not cached at all.
- Iceberg tables are never cached: their current pointer must be read fresh on every alter, so a
  positive entry would buy nothing.

Consequences accepted: a table that becomes an Iceberg table outside the proxy is unprotected for
at most one TTL, as is an Iceberg table created under a name a dropped Hive table had. The
proxy's own paths narrow that further - a `create_table` or an `alter_table` carrying
`metadata_location` drops the entry for that name. The cache is per-JVM; nothing here needs to be
shared between proxy instances, because a stale entry only costs protection for one TTL, never
correctness of the repair itself.

**Why not an `EnvironmentContext` prefilter instead.** Hive marks the INSERT-opening alter with
`alterTableOpType=DROPPROPS` and `DO_NOT_UPDATE_STATS`, so filtering on that would cost nothing.
It is rejected as the deciding test: other alters in the same `INSERT` (the stats update that
closes it) and plain `ALTER TABLE` shapes carry a compile-time `Table` snapshot too, without that
context, and would silently lose protection. The cache achieves the same saving without picking
which shapes to protect.

## Configuration

Three keys, parsed strictly like the rest of `routing.*` (`PropertyReader.getBoolean`, explicit
range validation, no silent fallback):

| Key | Default | Meaning |
| --- | --- | --- |
| `routing.iceberg-pointer-guard.enabled` | `true` | Turns the guard, and its reads, off entirely |
| `routing.iceberg-pointer-guard.table-cache-ttl-ms` | `30000` | Negative-cache lifetime; `0` disables caching, so every alter reads |
| `routing.iceberg-pointer-guard.table-cache-max-entries` | `10000` | Bound on cache size; expired entries are evicted first, then the cache is cleared |

`enabled=false` and `table-cache-ttl-ms=0` exist mainly so the stand can measure the guard's cost
against itself.

## Observability

One counter, `hms_proxy_iceberg_pointer_guard_events_total{catalog,outcome}`, with
`outcome` one of `repaired`, `forward_commit`, `not_iceberg`, `cache_suppressed`, `read_failed`.
The read count is everything except `cache_suppressed`, which makes both the hit rate and the
added round trips measurable on the stand without reading logs. The existing WARN on repair stays.

## Failure handling

Unchanged in spirit: a `get_table` that fails is logged at WARN and the alter is passed through
untouched (`read_failed`). Refusing the alter would break ordinary Hive writes whenever the
backend hiccups, which is a worse failure than the one being prevented.

## Tests

`src/test/java/io/github/mmalykhin/hmsproxy/routing/RoutingMetaStoreProxyIcebergPointerGuardTest.java`
grows the cases the current suite is missing, with a counting fake backend:

- An alter with **no `metadata_location`** against an Iceberg record: the forwarded table carries
  the current pointer, `table_type`, `storage_handler` and the `current-snapshot-*` keys, and the
  client's `numRows` survives. This is the case the stand proved is real and the suite did not
  cover.
- A stale pointer is still repaired; a forward commit still passes through untouched.
- An ordinary Hive table (record without `metadata_location`): the alter passes through unchanged,
  and a second alter within the TTL performs **no** second `get_table`.
- After the TTL expires, the read happens again.
- An Iceberg table is never cached: two alters perform two reads.
- `enabled=false`: no read, no rewrite.
- The repaired alter carries the CAS on a Hive 4 backend and does not on a 3.1 backend
  (existing tests, re-pointed at the pointer-less shape).

Config parsing tests live with the other `routing.*` parser tests in
`src/test/java/io/github/mmalykhin/hmsproxy/config`: defaults, a rejected negative TTL, a rejected
zero/negative `max-entries`, a rejected non-boolean `enabled`.

## Stand measurement

Deciding the cost is part of this work, so it is measured, not asserted:

1. Rebuild the fat jar, bring up the `hive4` Kerberos stand.
2. Run `smoke-stand/run-iceberg-concurrency-smoke.sh --prefix hive4 --writers 4 --sql-writers 2
   --sql-engine hdp --kerberos` at least 8 times. Success means row counts match writer counts
   **and** the guard's `repaired` counter is non-zero - the missing evidence last time.
3. Cost: run the SQL sections (B and C, both HiveServer2 instances) with the cache at its default
   and with `table-cache-ttl-ms=0`, comparing `not_iceberg` vs `cache_suppressed` counts and the
   `alter_table` latency the proxy already records. Report the numbers in TEST-MATRIX.

## Documentation

`smoke-stand/TEST-MATRIX.md` (I4 - the "does not fire on the real shape" paragraph is replaced by
what was measured), `src/main/resources/hms-proxy-example.properties`, `CHANGELOG.md` with
`CHANGELOG.ru.md`, `README.md` with `README.ru.md` if the new keys belong in the documented
surface, and an `AGENTS.md` routing note that Iceberg-ness is decided from the metastore record
inside the guard, so no other class grows its own check.

## Out of scope

The read-then-write race is not closed by this change, only narrowed - on Hive 4 backends the CAS
turns a lost update into a loud failure, and on the 3.1 line the window stays open because the
metastore ignores those keys. Holding the Iceberg table lock across read and write is the fix for
that and is a separate piece of work.
