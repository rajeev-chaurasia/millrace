from __future__ import annotations

import json
from datetime import datetime
from enum import StrEnum

from millrace.contracts import RunContext
from millrace.orchestration.configuration import DbtConfig, SparkConfig
from millrace.validation.configuration import candidate_schema


class SparkInputMode(StrEnum):
    KAFKA = "kafka"
    BRONZE_BACKFILL = "bronze-backfill"


def spark_submit_command(
    config: SparkConfig,
    context: RunContext,
    *,
    mode: SparkInputMode,
) -> list[str]:
    command = [config.executable, "--master", config.master]
    if config.packages:
        command.extend(["--packages", ",".join(config.packages)])
    for key, value in sorted(config.conf.items()):
        command.extend(["--conf", f"{key}={value}"])
    command.extend(
        [
            config.application,
            "--input-mode",
            mode.value,
            "--run-id",
            context.run_id,
            "--batch-cutoff",
            str(context.batch_id),
            "--interval-start",
            context.interval_start.isoformat(),
            "--interval-end",
            context.interval_end.isoformat(),
            "--output-run-key",
            context.storage_key,
        ]
    )
    if mode is SparkInputMode.KAFKA:
        command.append("--available-now")
    else:
        command.extend(
            [
                "--bronze-uri",
                config.bronze_uri,
                "--partition-end",
                context.interval_end.date().isoformat(),
            ]
        )
    return command


def dbt_candidate_command(config: DbtConfig, context: RunContext) -> list[str]:
    return [
        config.executable,
        "run",
        *_dbt_options(config),
        "--select",
        config.selector,
        "--vars",
        _dbt_variables(context),
    ]


def dbt_test_command(config: DbtConfig, context: RunContext) -> list[str]:
    return [
        config.executable,
        "test",
        *_dbt_options(config),
        "--select",
        config.selector,
        "--vars",
        _dbt_variables(context),
    ]


def _dbt_options(config: DbtConfig) -> list[str]:
    return [
        "--project-dir",
        config.project_dir,
        "--profiles-dir",
        config.profiles_dir,
        "--target",
        config.target,
    ]


def _dbt_variables(context: RunContext) -> str:
    return json.dumps(
        {
            "batch_id": context.batch_id,
            "candidate_schema": candidate_schema(context),
            "data_interval_end": _iso(context.interval_end),
            "data_interval_start": _iso(context.interval_start),
            "run_id": context.run_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _iso(value: datetime) -> str:
    return value.isoformat()
