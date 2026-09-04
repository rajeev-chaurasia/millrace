from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import LiteralString, Protocol, TypedDict, cast

import boto3
import duckdb
import psycopg
from botocore.config import Config as BotoConfig
from prometheus_client import (
    REGISTRY,
    push_to_gateway,  # pyright: ignore[reportUnknownVariableType]
)

from millrace.contracts import RunContext
from millrace.metrics import PUBLISHED_BATCH_ID, RUNS_TOTAL
from millrace.orchestration.commands import (
    SparkInputMode,
    dbt_candidate_command,
    dbt_test_command,
    spark_submit_command,
)
from millrace.orchestration.configuration import (
    OrchestrationConfig,
    load_orchestration_config,
)
from millrace.settings import Settings, get_settings
from millrace.validation.configuration import (
    candidate_schema,
    quote_identifier,
    quote_relation,
)

logger = logging.getLogger(__name__)


class RunDescriptor(TypedDict):
    run_id: str
    batch_id: int
    interval_start: str
    interval_end: str


class S3ReadinessClient(Protocol):
    def head_bucket(self, *, Bucket: str) -> object: ...


def orchestration_config() -> OrchestrationConfig:
    return load_orchestration_config(
        os.environ.get("MILLRACE_ORCHESTRATION_CONFIG", "config/orchestration.yml")
    )


def stable_run_id(interval_start: datetime, interval_end: datetime, batch_id: int) -> str:
    identity = f"{interval_start.isoformat()}|{interval_end.isoformat()}|{batch_id}"
    suffix = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"millrace_{interval_start:%Y%m%dT%H%M%S}_{batch_id}_{suffix}"


def capture_cutoff(interval_start: datetime, interval_end: datetime) -> RunDescriptor:
    config = orchestration_config()
    settings = get_settings()
    cutoff = config.cutoff
    relation = quote_relation(cutoff.relation)
    batch = quote_identifier(cutoff.batch_column)
    timestamp = quote_identifier(cutoff.event_timestamp_column)
    with psycopg.connect(settings.postgres_dsn) as connection, connection.cursor() as cursor:
        query = f"SELECT MAX({batch}) FROM {relation} WHERE {timestamp} < %s"  # noqa: S608
        cursor.execute(
            cast(LiteralString, query),
            (interval_end,),
        )
        row = cursor.fetchone()
    if row is None or row[0] is None:
        raise RuntimeError("no closed source batch exists for the data interval")
    batch_id = int(row[0])
    return {
        "run_id": stable_run_id(interval_start, interval_end, batch_id),
        "batch_id": batch_id,
        "interval_start": interval_start.isoformat(),
        "interval_end": interval_end.isoformat(),
    }


def wait_for_readiness() -> None:
    config = orchestration_config()
    settings = get_settings()
    deadline = time.monotonic() + config.readiness_timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _check_postgres(settings)
            if "duckdb" in settings.enabled_warehouse_targets:
                _check_duckdb(settings)
            if "snowflake" in settings.enabled_warehouse_targets:
                _check_snowflake(settings)
            _check_object_store(settings)
            return
        except Exception as exc:
            last_error = exc
            time.sleep(min(5.0, max(0.0, deadline - time.monotonic())))
    raise TimeoutError(f"services did not become ready: {last_error}")


def run_spark(descriptor: RunDescriptor, *, backfill: bool = False) -> None:
    config = orchestration_config()
    context = context_from_descriptor(descriptor)
    mode = SparkInputMode.BRONZE_BACKFILL if backfill else SparkInputMode.KAFKA
    _execute(
        spark_submit_command(config.spark, context, mode=mode),
        timeout_seconds=config.spark.timeout_seconds,
    )


def load_snowflake_silver(descriptor: RunDescriptor) -> None:
    """Loads the silver Parquet snapshot into Snowflake before dbt runs against it.
    Has no DuckDB equivalent: DuckDB reads the same Parquet objects directly via
    httpfs at query time, so this stage exists only on the Snowflake path.
    """
    from millrace.warehouse import open_warehouse
    from millrace.warehouse.snowflake_target import load_silver, raw_schema

    config = orchestration_config()
    settings = get_settings()
    context = context_from_descriptor(descriptor)
    gateway = open_warehouse("snowflake", settings)
    try:
        load_silver(gateway, settings, context, temporary_directory=config.temporary_directory)
    finally:
        gateway.close()
    logger.info("loaded silver snapshot into snowflake schema %s", raw_schema(context))


def build_candidate(descriptor: RunDescriptor, *, warehouse: str = "duckdb") -> None:
    config = orchestration_config()
    context = context_from_descriptor(descriptor)
    _execute(
        dbt_candidate_command(config.dbt, context, warehouse=warehouse),
        timeout_seconds=config.dbt.timeout_seconds,
        extra_environment=_dbt_schema_environment(context, warehouse=warehouse),
    )


def test_candidate(descriptor: RunDescriptor, *, warehouse: str = "duckdb") -> None:
    config = orchestration_config()
    context = context_from_descriptor(descriptor)
    _execute(
        dbt_test_command(config.dbt, context, warehouse=warehouse),
        timeout_seconds=config.dbt.timeout_seconds,
        extra_environment=_dbt_schema_environment(context, warehouse=warehouse),
    )


def validate_candidate(descriptor: RunDescriptor, *, warehouse: str = "duckdb") -> str:
    context = context_from_descriptor(descriptor)
    output = _execute(
        _validation_command("validate", context, warehouse=warehouse),
        timeout_seconds=orchestration_config().dbt.timeout_seconds,
    )
    report_path = output.strip().splitlines()[-1] if output.strip() else ""
    if not report_path:
        raise RuntimeError("validation did not return a report path")
    return report_path


def promote_candidate(
    descriptor: RunDescriptor,
    report_path: str,
    *,
    warehouse: str = "duckdb",
) -> None:
    context = context_from_descriptor(descriptor)
    _execute(
        [*_validation_command("promote", context, warehouse=warehouse), "--report", report_path],
        timeout_seconds=orchestration_config().dbt.timeout_seconds,
    )


def _dbt_schema_environment(context: RunContext, *, warehouse: str) -> dict[str, str]:
    environment = {"MILLRACE_DBT_SCHEMA": candidate_schema(context)}
    if warehouse == "snowflake":
        from millrace.warehouse.snowflake_target import raw_schema

        environment["MILLRACE_RAW_SCHEMA"] = raw_schema(context)
    return environment


def emit_metrics(descriptor: RunDescriptor) -> None:
    context = context_from_descriptor(descriptor)
    PUBLISHED_BATCH_ID.set(context.batch_id)
    RUNS_TOTAL.labels(status="published").inc()
    gateway = os.environ.get("MILLRACE_PUSHGATEWAY_URL")
    if gateway:
        push_to_gateway(
            gateway,
            job="millrace",
            registry=REGISTRY,
            grouping_key={"run_id": context.run_id},
        )


def cleanup_temporary_files(descriptor: RunDescriptor) -> None:
    root = Path(orchestration_config().temporary_directory).resolve()
    safe_run_id = "".join(
        character if character.isalnum() or character in {"-", "_"} else "_"
        for character in descriptor["run_id"]
    )
    destination = (root / safe_run_id).resolve()
    if destination.parent != root:
        raise ValueError("temporary run path escapes its configured root")
    shutil.rmtree(destination, ignore_errors=True)

    snowflake_load_root = (root / "snowflake_load").resolve()
    snowflake_load_destination = (snowflake_load_root / safe_run_id).resolve()
    if snowflake_load_destination.parent == snowflake_load_root:
        shutil.rmtree(snowflake_load_destination, ignore_errors=True)


def context_from_descriptor(descriptor: RunDescriptor) -> RunContext:
    return RunContext.from_iso(
        run_id=descriptor["run_id"],
        batch_id=descriptor["batch_id"],
        interval_start=descriptor["interval_start"],
        interval_end=descriptor["interval_end"],
    )


def _execute(
    command: list[str],
    *,
    timeout_seconds: int,
    extra_environment: dict[str, str] | None = None,
) -> str:
    environment = os.environ.copy()
    environment.update(extra_environment or {})
    completed = subprocess.run(  # noqa: S603
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        env=environment,
    )
    if completed.returncode != 0:
        output = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"command exited with status {completed.returncode}: {output[-4000:]}")
    return completed.stdout


def _validation_command(action: str, context: RunContext, *, warehouse: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "millrace.validation",
        action,
        "--warehouse",
        warehouse,
        "--run-id",
        context.run_id,
        "--batch-id",
        str(context.batch_id),
        "--interval-start",
        context.interval_start.isoformat(),
        "--interval-end",
        context.interval_end.isoformat(),
    ]


def _check_postgres(settings: Settings) -> None:
    with psycopg.connect(settings.postgres_dsn, connect_timeout=5) as connection:
        connection.execute("SELECT 1")


def _check_duckdb(settings: Settings) -> None:
    with duckdb.connect(settings.duckdb_path) as connection:
        connection.execute("SELECT 1")


def _check_snowflake(settings: Settings) -> None:
    from millrace.warehouse.snowflake_target import connect

    connection = connect(settings)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
    finally:
        connection.close()


def _check_object_store(settings: Settings) -> None:
    client = cast(
        S3ReadinessClient,
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
            region_name=settings.s3_region,
            config=BotoConfig(connect_timeout=5, read_timeout=5, retries={"max_attempts": 0}),
        ),
    )
    client.head_bucket(Bucket=settings.s3_bucket)
