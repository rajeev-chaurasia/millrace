# Demo

## Start the complete stack

```powershell
./scripts/bootstrap.ps1 -Dashboard -Monitoring
```

The command starts the core services and waits for their health checks. Local interfaces use these
default addresses:

- Airflow: `http://localhost:8082`
- Streamlit: `http://localhost:8501`
- Grafana: `http://localhost:3000`
- MinIO: `http://localhost:9001`
- Spark: `http://localhost:8080`

## Load deterministic source changes

```powershell
./scripts/demo.ps1
```

The source demo applies three idempotent batches containing inserts, updates, and deletes. Airflow
then consumes the resulting Kafka CDC events on its configured schedule.

## Run the verified end-to-end demonstration

```powershell
uv run pytest tests/e2e -m e2e
```

The golden test loads the demo batches, triggers an Airflow run, waits for every task, and checks
the published marts. The failure-injection test builds an isolated candidate, changes one customer
email, and proves that:

- Reconciliation fails on `checksum:email`.
- The failed candidate is not published.
- The previous published run remains current.
- Published order facts remain queryable.

## Expected published rows

- `analytics.dim_customer`: 3
- `analytics.dim_product`: 4
- `analytics.fact_order`: 3
- `analytics.fact_order_item`: 6

## Verified local run

The final clean run completed all nine Airflow tasks successfully. On the development workstation,
the run completed in about 80 seconds, including a 33-second bounded Spark ingestion, dbt build,
reconciliation, dbt tests, transactional promotion, metrics, and cleanup. These values describe
the deterministic demo dataset and are not a production throughput benchmark.

## Inspect validation

Open the latest report under the reconciliation artifact volume or view the validation panel in
Streamlit. A passing report contains row-count, partition-count, per-column checksum, and aggregate
results at one `batch_id` cutoff.
