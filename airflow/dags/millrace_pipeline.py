from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import dag, get_current_context, task

from millrace.orchestration.runtime import (
    RunDescriptor,
    build_candidate,
    capture_cutoff,
    cleanup_temporary_files,
    emit_metrics,
    promote_candidate,
    run_spark,
    test_candidate,
    validate_candidate,
    wait_for_readiness,
)


@dag(
    dag_id="millrace_pipeline",
    schedule=os.environ.get("MILLRACE_AIRFLOW_SCHEDULE", "*/15 * * * *"),
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args={
        "owner": "millrace",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=35),
    },
    tags=["millrace", "cdc"],
)
def millrace_pipeline() -> None:
    @task(task_id="readiness", retries=6, retry_delay=timedelta(seconds=30))
    def readiness_task() -> None:
        wait_for_readiness()

    @task(task_id="capture_cutoff", retries=1)
    def cutoff_task(_ready: None) -> RunDescriptor:
        context: dict[str, Any] = get_current_context()
        interval_start = context.get("data_interval_start")
        interval_end = context.get("data_interval_end")
        if (
            not isinstance(interval_start, datetime)
            or not isinstance(interval_end, datetime)
            or interval_start >= interval_end
        ):
            logical_date = context.get("logical_date")
            interval_end = logical_date if isinstance(logical_date, datetime) else datetime.now(UTC)
            interval_start = interval_end - timedelta(minutes=15)
        return capture_cutoff(interval_start, interval_end)

    @task(task_id="bounded_spark_submit", retries=1, execution_timeout=timedelta(minutes=35))
    def spark_task(descriptor: RunDescriptor) -> None:
        run_spark(descriptor)

    @task(task_id="dbt_candidate_build", retries=1)
    def dbt_build_task(descriptor: RunDescriptor, _spark_complete: None) -> None:
        build_candidate(descriptor)

    @task(task_id="reconciliation", retries=0)
    def validation_task(descriptor: RunDescriptor, _build_complete: None) -> str:
        return validate_candidate(descriptor)

    @task(task_id="dbt_tests", retries=1)
    def dbt_test_task(descriptor: RunDescriptor, _report_path: str) -> None:
        test_candidate(descriptor)

    @task(task_id="transactional_promotion", retries=1)
    def promotion_task(
        descriptor: RunDescriptor,
        report_path: str,
        _tests_complete: None,
    ) -> None:
        promote_candidate(descriptor, report_path)

    @task(task_id="publish_metrics", retries=2)
    def metrics_task(descriptor: RunDescriptor, _promotion_complete: None) -> None:
        emit_metrics(descriptor)

    @task(task_id="cleanup", retries=1)
    def cleanup_task(descriptor: RunDescriptor) -> None:
        cleanup_temporary_files(descriptor)

    ready = readiness_task()
    descriptor = cutoff_task(ready)
    spark_complete = spark_task(descriptor)
    build_complete = dbt_build_task(descriptor, spark_complete)
    report_path = validation_task(descriptor, build_complete)
    tests_complete = dbt_test_task(descriptor, report_path)
    promotion_complete = promotion_task(descriptor, report_path, tests_complete)
    metrics_complete = metrics_task(descriptor, promotion_complete)
    cleanup_complete = cleanup_task(descriptor)
    metrics_complete >> cleanup_complete.as_teardown(
        setups=ready,
        on_failure_fail_dagrun=True,
    )


millrace_pipeline()
