# Keeping a REST commit from stripping an Iceberg table's Hive readability

Design for making the proxy's Iceberg REST commits preserve the Hive-engine storage descriptor, so
a REST write no longer costs a Hive-created Iceberg table its readability on the 3.1 line.

## What this supersedes

The first version of this document described a different defect, taken from the test matrix's H12
row: that Hive 4's `STORED BY ICEBERG` leaves an abstract `org.apache.hadoop.mapred.FileInputFormat`
in the descriptor, which Hive 3.1 cannot instantiate, and that the proxy should normalize it on the
write path.

**That diagnosis was wrong, and it was measured wrong.** A Hive 4 `create table ... stored by
iceberg` lands a perfectly concrete descriptor, and both 3.1 engines read the result. What breaks
the table is the proxy's own REST commit, which happens later in the scenario. Whoever recorded
H12 saw the abstract value and attributed it to the DDL without noticing the REST commit in
between.

The reason nobody caught it: `run-iceberg-interop-smoke.sh` carved `hdp` and `apache` out of every
`--origin hive4` run, citing H12. The scenario asserted the limitation instead of testing it, so it
could not notice that the limitation had the wrong cause - or that the real defect was worse.

## The defect

A REST commit through the proxy rewrites the table's `StorageDescriptor` into the non-Hive-engine
shape and drops the Iceberg storage handler:

| Field | Before the REST commit | After |
| --- | --- | --- |
| `inputFormat` | `org.apache.iceberg.mr.hive.HiveIcebergInputFormat` | `org.apache.hadoop.mapred.FileInputFormat` |
| `outputFormat` | `org.apache.iceberg.mr.hive.HiveIcebergOutputFormat` | `org.apache.hadoop.mapred.FileOutputFormat` |
| `serdeInfo.serializationLib` | `org.apache.iceberg.mr.hive.HiveIcebergSerDe` | `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe` |
| parameter `storage_handler` | present | **gone** |

`metadata_location`, `previous_metadata_location` and `table_type` survive - the pointer guard's
merge protects those - so the table is still a valid Iceberg table. It is simply no longer a table
Hive can plan a scan against.

Both 3.1 engines then fail with:

```
Cannot create an instance of InputFormat class org.apache.hadoop.mapred.FileInputFormat
as specified in mapredWork!
```

Hive 4 keeps reading the table, so the damage is asymmetric and easy to miss from a Hive 4 seat.

This is a **data-accessibility regression the proxy inflicts on its own users**, not a Hive
incompatibility: one REST append is enough to cut the 3.1 line off from a table it could read a
moment earlier.

## Mechanism, and the controlled experiment that pinned it

Iceberg's `HiveTableOperations` writes the Hive-engine descriptor only when the Hive engine is
enabled - by the table property `engine.hive.enabled`, or by `iceberg.engine.hive.enabled` on the
Hadoop `Configuration`, with the table property taking precedence. Otherwise it writes the plain
file-format shape and no storage handler.

REST-*created* tables escape this because `smoke-stand/iceberg-rest-writer` sets
`engine.hive.enabled=true` at create time. A Hive-created table carries no such property, and
`grep -rn 'engine\.hive' src/main` finds nothing: the proxy never sets the configuration form
either. So Iceberg's default applies and the descriptor degrades.

Measured on the stand, same scenario twice, one variable changed:

| Table | After a REST append | 3.1 read |
| --- | --- | --- |
| Hive 4-created, no property | descriptor degraded, `storage_handler` gone | fails |
| Hive 4-created, `engine.hive.enabled=true` set by hand | descriptor intact | returns both rows |

The first attempt at this experiment was inconclusive and is worth recording: the REST append
failed with `Cannot set unknown field named: src` because the probe table had one column while the
writer writes two. The descriptor stayed concrete simply because no commit had happened. An
unnoticed failure there would have "confirmed" the mechanism for the wrong reason.

## The fix

Set `iceberg.engine.hive.enabled=true` on the per-catalog Hadoop `Configuration` that the REST
catalog uses - the one `IcebergRestServices.open(..., hadoopConfForCatalog)` receives, which in
production resolves to `router.requireBackend(catalog).hiveConf()`.

Consequences, each of which the plan verifies rather than assumes:

- A Hive-created table keeps its Hive-readable descriptor across REST commits.
- A REST-created table is unaffected: it already carries the table property, which wins anyway.
- A table already degraded by an earlier commit is **repaired by its next REST commit**, because
  the descriptor is rewritten on every commit. No migration is needed.
- A table that deliberately sets `engine.hive.enabled=false` keeps that choice: the table property
  takes precedence over the configuration.

### Configuration

`rest-catalog.hive-engine-descriptor=true` by default.

A key rather than unconditional behaviour, for the same reason the house style uses keys elsewhere:
this changes what the proxy persists into the metastore. An operator serving only Hive 4 and Spark
clients might legitimately prefer Iceberg's default, and should not need a new build to get it.

### Where it does not go

Not in `Hive4FrontendBridge`, and not keyed to any front door. The degradation happens on the REST
write path regardless of which front door created the table - a Hive 3.1-created table hit by a
REST append degrades exactly the same way. Scoping this to the Hive 4 front door, as the superseded
design did, would have fixed a case that is not even the common one.

## Testing

Unit: the per-catalog `Configuration` handed to the REST catalog carries
`iceberg.engine.hive.enabled=true` by default, and does not when the key is off. This is a
configuration-wiring test; it cannot prove Iceberg's behaviour, and the plan must not pretend it
does.

Stand, on the `hive4` backend, both profiles:

- The four-participant `run-iceberg-interop-smoke.sh --origin hive4` run - with the carve-out
  deleted - **must be observed failing before the fix**. It has already been observed failing once
  during diagnosis; the plan re-confirms it against the jar under test rather than citing that run.
- After the fix the same run passes with all four participants.
- Descriptor assertions before and after a REST commit, so the run fails loudly if the shape
  degrades again - a row count alone would not notice, because Hive 4 keeps reading either way.
- The self-repair claim: take a table already degraded, commit to it through REST, and assert the
  descriptor comes back.

## Documentation to correct

These all state the superseded diagnosis and are wrong as written:

- `smoke-stand/TEST-MATRIX.md` / `.ru.md`: the H12 row and the "the one asymmetry, and it is
  Hive's, not the proxy's" paragraph, including its claim that "the proxy passes the descriptor
  through unchanged in both directions; nothing here is a routing or compatibility decision it
  could make differently". Both halves are false.
- `AGENTS.md`: the sentence repeating the same explanation.
- `README.md` / `.ru.md`: wherever the Hive 4 interop limitation is described.

The matrix should also record what the carve-out cost, because the lesson generalizes: a scenario
that skips the participants who could falsify its claim proves nothing, and this one hid a real
defect for as long as it existed.

## Out of scope

- Repairing degraded tables out-of-band. They repair themselves on the next REST commit; anything
  more is unnecessary.
- The Hive 4 front door's descriptor handling, which the superseded design targeted and which the
  measurements show needs no change.
- `MERGE INTO`, partitioned tables and schema evolution, uncovered for independent reasons
  (matrix section F).
