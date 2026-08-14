from __future__ import annotations

from datetime import UTC, datetime

from millrace.contracts import RunContext
from millrace.orchestration.commands import SparkInputMode, spark_submit_command
from millrace.orchestration.configuration import SparkConfig
from millrace.orchestration.runtime import stable_run_id


def test_backfill_command_reads_bronze_partitions() -> None:
    context = _context()
    command = spark_submit_command(
        SparkConfig(
            master="local[*]",
            application="stream.py",
            bronze_uri="s3a://millrace/bronze",
        ),
        context,
        mode=SparkInputMode.BRONZE_BACKFILL,
    )

    assert command[0] == "spark-submit"
    assert _argument(command, "--input-mode") == "bronze-backfill"
    assert _argument(command, "--bronze-uri") == "s3a://millrace/bronze"
    assert _argument(command, "--partition-end") == "2026-01-02"
    assert "--available-now" not in command


def test_run_id_is_stable_and_interval_specific() -> None:
    context = _context()

    first = stable_run_id(context.interval_start, context.interval_end, context.batch_id)

    assert first == stable_run_id(context.interval_start, context.interval_end, context.batch_id)
    assert first != stable_run_id(
        context.interval_start,
        context.interval_end,
        context.batch_id + 1,
    )


def _argument(command: list[str], name: str) -> str:
    return command[command.index(name) + 1]


def _context() -> RunContext:
    return RunContext(
        run_id="backfill",
        batch_id=42,
        interval_start=datetime(2026, 1, 1, tzinfo=UTC),
        interval_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
