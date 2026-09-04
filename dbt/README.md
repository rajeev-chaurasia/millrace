# Millrace dbt project

This project reads the immutable silver snapshot for one pipeline run and writes all models into
the candidate schema supplied by `target.schema`. It never creates or replaces stable `analytics`
views or schemas. Publication remains the responsibility of the validation stage.

Two targets are defined in `profiles.yml`: `candidate` (DuckDB, the default) and
`snowflake_candidate` (Snowflake, supplementary). Every model uses the portability macros in
`macros/portability.sql` for the handful of constructs (date-key formatting, timestamp casts, the
date spine, ISO week/day-of-week, day/month names) that either do not exist on Snowflake or would
otherwise disagree in meaning between the two engines. On DuckDB, `{{ source('silver', ...) }}`
resolves through the `external_location` glob to the immutable MinIO silver run prefix directly.
Snowflake has no equivalent, so on that target `source.schema` instead points at a per-run raw
schema (`MILLRACE_RAW_SCHEMA`) that `millrace.warehouse.snowflake_target.load_silver` populates
from the same silver Parquet objects, via an internal stage, before dbt runs.

## Required environment

- `MILLRACE_DBT_SCHEMA`: isolated candidate schema for the run
- `MILLRACE_S3_ACCESS_KEY`: MinIO access key
- `MILLRACE_S3_SECRET_KEY`: MinIO secret key
- `MILLRACE_S3_ENDPOINT`: S3 endpoint without a URL scheme, defaults to `localhost:9000`
- `MILLRACE_S3_BUCKET`: silver bucket, defaults to `millrace`
- `MILLRACE_S3_REGION`: S3 region, defaults to `us-east-1`
- `MILLRACE_DUCKDB_PATH`: warehouse path, defaults to `../data/millrace.duckdb`
- `MILLRACE_RAW_SCHEMA`: Snowflake target only, the per-run raw schema populated by the load stage
- `MILLRACE_SNOWFLAKE_ACCOUNT` / `_USER` / `_PASSWORD` / `_ROLE` / `_WAREHOUSE` / `_DATABASE`:
  Snowflake target only

Pass the closed run contract as dbt variables:

```shell
dbt build --profiles-dir . --vars '{run_id: run_001, batch_id: 42, data_interval_start: "2026-08-13T00:00:00+00:00", data_interval_end: "2026-08-14T00:00:00+00:00"}'
```

Add `--target snowflake_candidate` to build against Snowflake instead of the default DuckDB
target.

The optional `silver_root` variable overrides the default `s3://<bucket>/silver` prefix for
isolated tests and archived local backfills (DuckDB only; Snowflake always reads its per-run raw
schema).

Use a fresh candidate schema for each run. Reusing a schema is supported for idempotent retries:
dimensions and facts use merge semantics keyed by their source identifiers.
