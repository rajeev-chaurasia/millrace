from __future__ import annotations

import pytest

from millrace.streaming.config import InputMode, parse_args
from millrace.streaming.models import Entity


def test_orchestration_arguments_and_minio_environment_are_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "MILLRACE_S3_ACCESS_KEY",
        "MILLRACE_S3_SECRET_KEY",
        "MILLRACE_S3_BUCKET",
        "MILLRACE_S3_ENDPOINT_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MILLRACE_MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MILLRACE_MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MILLRACE_MINIO_BUCKET", "bucket")
    monkeypatch.setenv("MILLRACE_MINIO_ENDPOINT", "http://minio:9000")

    config = parse_args(
        [
            "--input-mode",
            "kafka",
            "--run-id",
            "run-1",
            "--batch-cutoff",
            "4",
            "--interval-start",
            "2026-08-14T01:00:00+00:00",
            "--interval-end",
            "2026-08-14T02:00:00+00:00",
            "--output-run-key",
            "2026-08-14/0000000004_run-1",
            "--available-now",
        ]
    )

    assert config.input_mode is InputMode.KAFKA
    assert config.batch_id == 4
    assert config.topic_for(Entity.CUSTOMERS) == "millrace.customers"
    assert config.s3_endpoint_url == "http://minio:9000"
    assert config.s3_bucket == "bucket"
