# Runbook

## Start the platform

Create the local environment file once:

```powershell
Copy-Item .env.example .env
```

Start the core services:

```powershell
./scripts/bootstrap.ps1
```

The POSIX equivalent is `./scripts/bootstrap.sh`.

Default endpoints:

- Airflow: `http://localhost:8082`
- Spark master: `http://localhost:8080`
- Spark worker: `http://localhost:8081`
- Kafka Connect: `http://localhost:8083`
- MinIO API: `http://localhost:9000`
- MinIO console: `http://localhost:9001`

Start optional interfaces with Compose profiles:

```powershell
docker compose --profile dashboard --profile monitoring up --detach
```

- Streamlit: `http://localhost:8501`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`

Local credentials are defined in `.env`. They are not suitable outside an isolated workstation.

## Verify health

```powershell
./scripts/health.ps1
docker compose ps
docker compose logs --since 10m airflow-scheduler spark-master connect
```

A ready deployment has healthy long-running services and completed `connect-init`, `minio-init`,
and `airflow-init` jobs.

## Run the demo

```powershell
./scripts/demo.ps1
```

The demo submits deterministic source changes. Airflow consumes them on its configured schedule.
Use `uv run pytest tests/e2e -m e2e` for a triggered run with publication and failure-injection
assertions. Running either path again must not duplicate published facts.

## Investigate a failed load

1. Open the failed Airflow task and record its `run_id` and `batch_id`.
2. Inspect `s3://millrace/reports/run_id=<run_id>/reconciliation.json`.
3. Compare failed checks with the candidate schema named for that run.
4. Inspect dead-letter Parquet for malformed topic records.
5. Check Kafka lag and Spark stage failures in Grafana and the Spark UI.
6. Correct the source, configuration, or code and clear only the failed task.

Do not change stable analytics views manually. The promotion task is the publication boundary.

## Backfill

Trigger the Airflow DAG with the requested UTC data interval. Backfills read archived bronze
partitions, build an isolated candidate, validate at the recorded batch cutoff, and publish only
after success. Kafka retention does not limit a backfill once bronze data exists.

Keep `max_active_runs=1` because local DuckDB permits one writer. This is a DuckDB constraint, not
a Snowflake one; the Airflow DAG only orchestrates the DuckDB path today, so it stays in place
regardless. A Snowflake backfill runs separately via
`python -m millrace.orchestration backfill-run --warehouse snowflake`.

## Snowflake

Snowflake is a supplementary target, off by default. To run against it locally or in CI, set
`MILLRACE_WAREHOUSE_TARGETS=duckdb,snowflake` plus every `MILLRACE_SNOWFLAKE_*` variable
(`.env.example` documents them).

Authentication: use key-pair auth (`MILLRACE_SNOWFLAKE_PRIVATE_KEY_PATH`) rather than a password
on any account that enforces MFA. Snowflake rejects password auth outright for programmatic
connections there (`MFA authentication is required, but none of your current MFA methods are
supported for programmatic authentication`), and dbt-snowflake at the pinned version has no
programmatic-access-token support, so key-pair is the only method that works for both the Python
connector and the dbt build. `.env.example` carries the `openssl` and `ALTER USER` commands.
Placing the key at `config/rsa_key.p8` makes it visible inside the containers automatically, since
`config/` is already mounted there read-only.

Run the backfill inside the Airflow container, not on the host: `config/orchestration.yml` points
`spark.application` at a container path, and dbt lives in the image's `/opt/dbt-venv`.

```shell
docker compose exec -T airflow-scheduler python3 -m millrace.orchestration backfill-run \
  --batch-id <n> --interval-start <iso> --interval-end <iso> \
  --warehouse snowflake --promote
```

Pick an interval that spans real wall-clock ingestion time, not the demo data's business dates:
bronze `event_date` comes from when Debezium captured the change, so an interval ending before
today filters every row out and produces an empty, passing-looking silver snapshot.

`backfill-run` computes `run_id` deterministically from `batch_id` and the interval, so running it
again with `--warehouse duckdb` for the same three arguments builds the DuckDB candidate under the
identical `run_id`. That is what lets `compare` address both targets' candidate schemas from one
context:

```shell
docker compose exec -T airflow-scheduler python3 -m millrace.validation compare \
  --targets duckdb,snowflake \
  --run-id <run_id> --batch-id <n> --interval-start <iso> --interval-end <iso>
```

`compare` exits nonzero if either target's own validation fails, or if any individual check
disagrees between the two, and writes `cross_engine.json` next to the per-target reconciliation
reports.

A Snowflake run failing at the load stage (`load_snowflake_silver`) means the row count `COPY
INTO` loaded does not match the silver Parquet row count; check the Snowflake internal stage's
file list and the `raw_<run>` schema before assuming a data problem. A Snowflake promotion failing
partway through should never leave `analytics` half-swapped: `SnowflakePromotionService` builds the
new views entirely in `analytics_staging` first and swaps schemas in one statement, so recovery is
the same as any other failed run: investigate the candidate, do not touch `analytics` by hand.

## Recovery

### Kafka or Spark restart

Restart the failed service and retry the Airflow ingestion task. Spark resumes from its durable
checkpoint. Run-scoped output paths make the retry deterministic.

### Rebuild a candidate

Remove only the affected candidate schema and run-scoped silver prefix, then retry from the dbt
build task. Never remove the published schema during recovery.

### Reset all local state

```powershell
./scripts/teardown.ps1 -RemoveVolumes
./scripts/bootstrap.ps1
```

Volume removal deletes all local source, Kafka, object, Airflow, and dashboard state.

## Stop the platform

```powershell
./scripts/teardown.ps1
```

Use the POSIX scripts with the same base names on macOS or Linux.
