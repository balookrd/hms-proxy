# Making a Hive 4-created Iceberg table readable by the 3.1 line

Design for normalizing the `StorageDescriptor` of an Iceberg table declared through the Hive 4
front door, so that the record the metastore ends up holding is the one every engine can read.

## Problem

A table created by a Hive 4 HiveServer2 with `STORED BY ICEBERG` is unreadable by both 3.1-line
engines on the stand. Hive 4 leaves `StorageDescriptor.inputFormat` as the abstract
`org.apache.hadoop.mapred.FileInputFormat` (spelling the handler class out explicitly leaves it
`null` instead), because Hive 4 resolves the real format through the storage handler at plan time
and never reads the descriptor's value. Hive 3.1 instantiates whatever the descriptor names and
fails with:

```
Cannot create an instance of InputFormat class org.apache.hadoop.mapred.FileInputFormat
```

This is recorded as H12 in `smoke-stand/TEST-MATRIX.md`, where the Hive 4-origin interop run ends
at 3 rows instead of 5 because the two 3.1 participants cannot read what Hive 4 created.

The defect is Hive's, not the proxy's - but the proxy is the only place where a Hive 4 writer and
a 3.1 reader meet, and it already carries one downgrade of exactly this kind (`EXCL_WRITE` ->
`EXCLUSIVE` in `Hive4FrontendBridge`, for a Hive 4-only enum constant Apache 3.1.3 does not have).

## The target shape is measured, not invented

Tables written by Iceberg's own `HiveTableOperations` - the REST path, and the 3.1 storage handler
itself - carry the concrete `HiveIcebergInputFormat`, and those are exactly the origins that H9-H11
prove readable by **every** engine on the stand, Hive 4 included. So the shape to normalize
towards is one the stand has already measured end to end, not a guess about what Hive 3.1 wants.

## Scope

Requests arriving through the **Hive 4 front door only** (`APACHE_4_1_0` frontend profile). A 3.1
client cannot produce this shape, so widening the trigger to every front door would change traffic
that has no problem today.

The fix is applied on the **write** path, so the record the metastore stores is correct for every
consumer from then on - including consumers that reach the metastore without going through the
proxy. The trade-off accepted here: tables created before this change are not repaired. Repairing
them is a separate decision (a read-path normalization, or a one-off migration) and is explicitly
out of scope.

## Where the code goes

`Hive4FrontendBridge`, immediately after `ThriftValueConverter.convertTBase(...)` in
`handleCreateTableReq` and `handleAlterTableReq`. At that point the table is already a typed
`org.apache.hadoop.hive.metastore.api.Table`, so no reflection is needed.

This differs deliberately from `downgradeHive4OnlyEnumValues`, which has to run *before*
conversion and reflectively: an `EXCL_WRITE` enum constant does not survive the conversion into
the Apache 3.1.3 representation, while a string field does.

The implementation must also check whether a Hive 4 client can reach `create_table` /
`create_table_with_environment_context` on the shared (positional) path rather than the `*_req`
wrappers. If it can, that path gets the same call; if it cannot, the plan records why not.

New class `frontend/IcebergStorageDescriptorNormalizer`: a single function over the structure,
with no access to routing, no metastore call and no I/O. That keeps it independently testable and
small enough to hold in one piece.

## Trigger, and its relationship to the Iceberg-ness rule

`AGENTS.md` states that "is this an Iceberg table?" is decided **only** in
`IcebergTablePointerGuard`, and only from the metastore's own record. That rule exists because the
`alter_table` shape HiveServer2 sends when opening an `INSERT` carries no Iceberg key at all, so a
guard keyed off the request is a no-op for the very shape that loses data.

This normalizer does not answer that question and must not be read as doing so:

- It fires on what the client **explicitly declared** in its DDL - the parameter
  `storage_handler` equal to `org.apache.iceberg.mr.hive.HiveIcebergStorageHandler` - not on
  inferring Iceberg-ness from circumstantial evidence.
- On `create_table` there is no metastore record yet, so reading one is not merely undesirable but
  impossible.
- Its purpose is different: translating a Hive 4 declaration into a portable one, not protecting a
  pointer.

`AGENTS.md` must gain an explicit carve-out sentence for this. Without it, the next reader will
conclude the invariant was broken silently.

## What is rewritten

At most three fields, and only when the incoming value is empty or abstract. Only the first row is
settled; the other two are candidates that the plan's first step either confirms or deletes (see
"The open unknown" below), because what Hive 4 puts in them has not been measured.

| Field | Rewritten when it is | Rewritten to | Status |
| --- | --- | --- | --- |
| `sd.inputFormat` | `null`, empty, or `org.apache.hadoop.mapred.FileInputFormat` | `org.apache.iceberg.mr.hive.HiveIcebergInputFormat` | confirmed - this is the failure H12 records |
| `sd.outputFormat` | `null`, empty, or an abstract `org.apache.hadoop.mapred.*` base class | whatever the REST-created table carries | candidate, pending measurement |
| `sd.serdeInfo.serializationLib` | `null` or empty | whatever the REST-created table carries | candidate, pending measurement |

The target values for the candidate rows are deliberately written as "whatever the REST-created
table carries" rather than guessed class names: the reference descriptor is read off the stand, not
recalled from memory.

A concrete value the client set itself is **never** touched. Overwriting it would break
non-standard configurations that work today, and the failure mode this design targets is
specifically the absent-or-abstract value.

## Configuration

`frontend.hive4.normalize-iceberg-storage-descriptor`, default `true`.

A key rather than unconditional behaviour, unlike the `EXCL_WRITE` downgrade: this one silently
changes what the client sent, and the result is persisted in the metastore for good. If a future
Hive release starts relying on the abstract value, an operator needs a way out that does not
require a new build.

## Error handling

The normalizer never throws. A descriptor of an unexpected shape (no `sd`, no `serdeInfo`) is left
exactly as it arrived. This is a compatibility fix; failing a DDL statement because a descriptor
looked unfamiliar would be a worse outcome than the incompatibility it repairs.

A counter records how often a rewrite actually happened. Without it, production cannot tell "the
normalizer did not fire" from "the normalizer was not needed" - a distinction that matters the
first time someone reports H12 still happening.

## Testing

Unit tests, TDD - the failing test comes first:

- abstract `FileInputFormat` -> rewritten
- `null` -> rewritten
- a concrete third-party value -> **not** touched
- a table without the Iceberg storage handler -> not touched
- the flag off -> not touched
- a descriptor with no `serdeInfo` -> no exception, table unchanged

Stand verification, on the `hive4` backend:

- `run-iceberg-interop-smoke.sh --origin hive4` currently ends at 3 rows. **It must be observed
  failing before the fix**; a run that is green from the start means the scenario is not
  exercising what this design changes, and the evidence would be worthless.
- After the fix the same run reaches 5 rows, both 3.1 engines reading the Hive 4-created table.
- Hive 4 itself must still read its own table after the substitution - the whole design rests on
  Hive 4 ignoring this field, and that assumption gets tested rather than trusted.
- Both profiles, plain and Kerberos.

## The open unknown the plan resolves first

Only `inputFormat` is documented in the test matrix. What Hive 4 puts in `outputFormat` and
`serializationLib` has **not** been measured.

The first step of the implementation plan is therefore empirical: bring the stand up and dump two
real descriptors side by side - one table created by Hive 4, one created through REST. If they
differ only in `inputFormat`, the rewrite table above collapses to a single row and the design
gets simpler. No code is written before that comparison exists.

## Out of scope

- Repairing tables created before this change (read-path normalization or migration).
- The reverse direction: nothing here changes what a 3.1 client sends.
- Partitioned Iceberg tables and schema evolution, which remain uncovered on the stand for
  independent reasons (matrix section F).
