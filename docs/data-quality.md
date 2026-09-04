# Data Quality

Millrace treats validation as a publication requirement, not a monitoring suggestion. Every run
must reconcile source history and candidate target data at the same closed `batch_id`.

## Check types

### Row counts

Counts compare active source rows and candidate target rows for each configured entity. Partitioned
checks narrow failures to a business date or status without weakening the global check.

### Column checksums

Checksums are order-independent aggregates of deterministic row hashes. Values are canonicalized
before hashing:

- Null is represented by a reserved marker.
- Text uses its stored value and an unambiguous length-prefixed encoding.
- Dates use ISO calendar form.
- Timestamps use UTC with microsecond precision.
- Decimals are normalized with `Decimal.normalize()`, which strips trailing zeros rather than
  comparing at a configured fixed scale. This is what lets a DuckDB `DECIMAL(18,2)` value and a
  Snowflake `NUMBER(38,9)` value for the same logical amount canonicalize identically even though
  the two engines declare different scales.
- Booleans use `true` or `false`.

Each entity defines its primary key and included columns in
[`config/reconciliation.yml`](../config/reconciliation.yml). Source and target readers produce the
same canonical tuples before comparison.

### Deletes

A business key tombstoned in source history must not still exist in the target. This is checked
explicitly per entity (`deleted_absent`) rather than left implicit in the row-count and checksum
checks, so a target that wrongly retains a deleted row is diagnosed as a delete failure, not an
unexplained count mismatch.

### Aggregates

Business checks cover values that row counts cannot detect, including order value, item quantity,
minimum and maximum timestamps, and revenue grouped by business date. Aggregates are computed in
Python with exact `Decimal` arithmetic over the fetched rows, not pushed down as a SQL `SUM`, so
the two warehouse engines cannot disagree by way of SQL aggregate semantics.

## Failure behavior

Missing rules, absent datasets, query errors, duplicate keys, checksum differences, count
differences, and aggregate differences all fail the run. The validator:

1. Writes a JSON report with expected and actual values.
2. Records the run and check results in the control schema.
3. Emits validation metrics.
4. Returns a nonzero process status.
5. Leaves stable analytics views unchanged.

Candidate data is retained for diagnosis. Operators can remove expired candidate schemas after
capturing the report and relevant logs.

## Publication

The publication task verifies that the report belongs to the current run, all checks passed, and
the candidate schema exists. On DuckDB, it then updates stable analytics views and the published
run record in one transaction. Snowflake DDL auto-commits, so that swap instead happens through an
atomic `ALTER SCHEMA ... SWAP WITH ...` (see
[docs/architecture.md](architecture.md#publication-atomicity)). Either way, no earlier task can
publish data, and a mismatch or a mid-publish failure leaves the previously published dataset
unchanged.

## Cross-engine comparison

When more than one warehouse target is configured, `python -m millrace.validation compare` runs
the full reconciliation gate against each target using one cached read of source history, so a
DuckDB/Snowflake disagreement can only be a warehouse-side difference, never two independent reads
of a moving source. It reports each target's own pass/fail result and any individual check where
the two targets' expected or actual values differ.

## Controlled mismatch

The failure-injection test completes one valid run, modifies one candidate value, and reruns
validation. The test passes only when reconciliation fails and queries through stable views still
return the previously published values.
