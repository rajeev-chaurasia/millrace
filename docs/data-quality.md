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
- Decimals use a configured fixed scale.
- Booleans use `true` or `false`.

Each entity defines its primary key and included columns in
[`config/reconciliation.yml`](../config/reconciliation.yml). Source and target readers produce the
same canonical tuples before comparison.

### Aggregates

Business checks cover values that row counts cannot detect, including order value, item quantity,
minimum and maximum timestamps, and revenue grouped by business date. Decimal comparisons are exact
at the configured currency scale.

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
the candidate schema exists. It then updates stable analytics views and the published run record in
one DuckDB transaction. No earlier task can publish data.

## Controlled mismatch

The failure-injection test completes one valid run, modifies one candidate value, and reruns
validation. The test passes only when reconciliation fails and queries through stable views still
return the previously published values.
