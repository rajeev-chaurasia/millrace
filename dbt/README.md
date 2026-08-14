# Millrace dbt project

This dbt-duckdb project reads the immutable silver snapshot for one pipeline run and writes all
models into the candidate schema supplied by `target.schema`. It never creates or replaces stable
`analytics` views. Publication remains the responsibility of the validation stage.

## Required environment

- `MILLRACE_DBT_SCHEMA`: isolated candidate schema for the run
- `MILLRACE_S3_ACCESS_KEY`: MinIO access key
- `MILLRACE_S3_SECRET_KEY`: MinIO secret key
- `MILLRACE_S3_ENDPOINT`: S3 endpoint without a URL scheme, defaults to `localhost:9000`
- `MILLRACE_S3_BUCKET`: silver bucket, defaults to `millrace`
- `MILLRACE_S3_REGION`: S3 region, defaults to `us-east-1`
- `MILLRACE_DUCKDB_PATH`: warehouse path, defaults to `../data/millrace.duckdb`

Pass the closed run contract as dbt variables:

```shell
dbt build --profiles-dir . --vars '{run_id: run_001, batch_id: 42, data_interval_start: "2026-08-13T00:00:00+00:00", data_interval_end: "2026-08-14T00:00:00+00:00"}'
```

The optional `silver_root` variable overrides the default `s3://<bucket>/silver` prefix for
isolated tests and archived local backfills.

Use a fresh candidate schema for each run. Reusing a schema is supported for idempotent retries:
dimensions and facts use merge semantics keyed by their source identifiers.
