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

Keep `max_active_runs=1` because local DuckDB permits one writer.

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
