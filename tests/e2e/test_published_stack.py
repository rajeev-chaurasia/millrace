from __future__ import annotations

import json
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).parents[2]
ENV_FILE = PROJECT_ROOT / (".env" if (PROJECT_ROOT / ".env").exists() else ".env.example")
COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    str(ENV_FILE),
    "--file",
    str(PROJECT_ROOT / "compose.yaml"),
]


def _run(
    *arguments: str,
    timeout: int = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        [*COMPOSE, *arguments],
        cwd=PROJECT_ROOT,
        check=check,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _airflow(*arguments: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return _run("exec", "-T", "airflow-scheduler", "airflow", *arguments, timeout=timeout)


def _load_demo_batches() -> None:
    _run(
        "exec",
        "-T",
        "postgres",
        "psql",
        "--username",
        "millrace",
        "--dbname",
        "millrace",
        "--set",
        "ON_ERROR_STOP=on",
        "--command",
        (
            "SELECT control.apply_demo_batch(batch_id) "
            "FROM generate_series(1, 3) AS batches(batch_id);"
        ),
    )


def _wait_for_run(run_id: str, timeout_seconds: int = 900) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        completed = _airflow("dags", "list-runs", "millrace_pipeline", "--output", "json")
        runs: list[dict[str, Any]] = json.loads(completed.stdout)
        state = next((run["state"] for run in runs if run["run_id"] == run_id), None)
        if state == "success":
            return
        if state == "failed":
            raise AssertionError(f"Airflow run {run_id} failed")
        time.sleep(5)
    raise TimeoutError(f"Airflow run {run_id} did not finish")


@pytest.mark.e2e
def test_golden_pipeline_publishes_validated_marts() -> None:
    _load_demo_batches()
    _airflow("dags", "unpause", "millrace_pipeline")
    run_id = f"e2e__{uuid.uuid4().hex}"
    logical_date = datetime.now(UTC).isoformat()
    _airflow(
        "dags",
        "trigger",
        "millrace_pipeline",
        "--run-id",
        run_id,
        "--logical-date",
        logical_date,
    )
    _wait_for_run(run_id)

    query = """
import json
import os
import duckdb

connection = duckdb.connect(os.environ["MILLRACE_DUCKDB_PATH"], read_only=True)
counts = connection.execute(
    '''
    SELECT
        (SELECT count(*) FROM analytics.dim_customer),
        (SELECT count(*) FROM analytics.dim_product),
        (SELECT count(*) FROM analytics.fact_order),
        (SELECT count(*) FROM analytics.fact_order_item)
    '''
).fetchone()
status = connection.execute(
    "SELECT status, checks_failed FROM analytics.current_validation_status LIMIT 1"
).fetchone()
print(json.dumps({"counts": counts, "status": status}))
"""
    completed = _run(
        "exec",
        "-T",
        "airflow-scheduler",
        "python",
        "-c",
        query,
    )
    result = json.loads(completed.stdout)
    assert result == {"counts": [3, 4, 3, 6], "status": ["published", 0]}


@pytest.mark.e2e
def test_mismatch_does_not_replace_published_views() -> None:
    script = """
import json
import os
from pathlib import Path

import duckdb

from millrace.contracts import RunContext
from millrace.settings import get_settings
from millrace.validation.audit import DuckDbAuditWriter, JsonReportSink
from millrace.validation.configuration import candidate_schema, load_reconciliation_config
from millrace.validation.readers import DuckDbCandidateReader, PostgresHistoryReader
from millrace.validation.service import ValidationService
from millrace.warehouse import configure_object_store

settings = get_settings()
config = load_reconciliation_config(settings.reconciliation_config)
connection = duckdb.connect(settings.duckdb_path)
configure_object_store(connection, settings)
published = connection.execute(
    '''
    SELECT validation.run_id, validation.batch_id,
           validation.interval_start, validation.interval_end
    FROM control.validation_runs AS validation
    INNER JOIN control.publication_runs AS publication USING (run_id)
    ORDER BY publication.published_at DESC
    LIMIT 1
    '''
).fetchone()
published_context = RunContext(
    run_id=str(published[0]),
    batch_id=int(published[1]),
    interval_start=published[2],
    interval_end=published[3],
)
failure_context = RunContext(
    run_id=f"{published_context.run_id}_failure",
    batch_id=published_context.batch_id,
    interval_start=published_context.interval_start,
    interval_end=published_context.interval_end,
)
source_schema = candidate_schema(published_context)
failure_schema = candidate_schema(failure_context)
connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{failure_schema}"')
for name in ("customers", "products", "orders", "order_items"):
    connection.execute(
        f'CREATE OR REPLACE TABLE "{failure_schema}".validation_{name} '
        f'AS SELECT * FROM "{source_schema}".validation_{name}'
    )
connection.execute(
    f'UPDATE "{failure_schema}".validation_customers '
    "SET email = ? WHERE customer_id = ?",
    ["corrupted@example.test", 1001],
)
service = ValidationService(
    config=config,
    source=PostgresHistoryReader(settings.postgres_dsn),
    candidate=DuckDbCandidateReader(connection),
    report_sink=JsonReportSink(Path(settings.reports_directory) / "e2e"),
    audit_writer=DuckDbAuditWriter(connection, config.control_schema),
)
report, _ = service.validate(failure_context)
current = connection.execute(
    "SELECT run_id, checks_failed FROM analytics.current_validation_status LIMIT 1"
).fetchone()
print(
    json.dumps(
        {
            "failed": not report.passed,
            "failed_checks": [check.name for check in report.checks if not check.passed],
            "published_run": current[0],
            "published_checks_failed": current[1],
            "original_run": published_context.run_id,
        }
    )
)
"""
    completed = _run(
        "exec",
        "-T",
        "airflow-scheduler",
        "python",
        "-c",
        script,
    )
    result = json.loads(completed.stdout)
    assert result["failed"] is True
    assert result["failed_checks"] == ["checksum:email"]
    assert result["published_run"] == result["original_run"]
    assert result["published_checks_failed"] == 0
