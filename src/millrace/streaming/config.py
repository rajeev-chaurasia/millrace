from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from millrace.streaming.models import Entity


class InputMode(StrEnum):
    KAFKA = "kafka"
    BRONZE_BACKFILL = "bronze-backfill"


@dataclass(frozen=True, slots=True)
class StreamingConfig:
    run_id: str
    batch_id: int
    kafka_bootstrap_servers: str
    kafka_topic_prefix: str
    s3_endpoint_url: str
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)
    s3_bucket: str
    kafka_topic_schema: str = ""
    s3_region: str = "us-east-1"
    starting_offsets: str = "earliest"
    input_mode: InputMode = InputMode.KAFKA
    bronze_uri: str | None = None
    partition_end: str | None = None

    def __post_init__(self) -> None:
        required = {
            "run_id": self.run_id,
            "kafka_bootstrap_servers": self.kafka_bootstrap_servers,
            "kafka_topic_prefix": self.kafka_topic_prefix,
            "s3_endpoint_url": self.s3_endpoint_url,
            "s3_access_key": self.s3_access_key,
            "s3_secret_key": self.s3_secret_key,
            "s3_bucket": self.s3_bucket,
            "s3_region": self.s3_region,
        }
        missing = [name for name, value in required.items() if not value.strip()]
        if missing:
            raise ValueError(f"configuration values must not be empty: {', '.join(missing)}")
        if self.batch_id < 1:
            raise ValueError("batch_id must be positive")
        if self.starting_offsets not in {"earliest", "latest"}:
            raise ValueError("starting_offsets must be earliest or latest")
        if self.input_mode is InputMode.BRONZE_BACKFILL and self.bronze_uri is None:
            raise ValueError("bronze_uri is required for bronze backfills")

    def topic_for(self, entity: Entity) -> str:
        components = (self.kafka_topic_prefix, self.kafka_topic_schema, entity.value)
        return ".".join(component for component in components if component)


def parse_args(argv: Sequence[str] | None = None) -> StreamingConfig:
    parser = argparse.ArgumentParser(description="Ingest Millrace Debezium CDC into Parquet")
    parser.add_argument(
        "--input-mode",
        choices=tuple(InputMode),
        default=InputMode.KAFKA,
        type=InputMode,
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--batch-id", "--batch-cutoff", dest="batch_id", required=True, type=int)
    parser.add_argument("--interval-start")
    parser.add_argument("--interval-end")
    parser.add_argument("--output-run-key")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--bronze-uri")
    parser.add_argument("--partition-end")
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.getenv("MILLRACE_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--kafka-topic-prefix",
        default=os.getenv("MILLRACE_KAFKA_TOPIC_PREFIX", "millrace"),
    )
    parser.add_argument(
        "--kafka-topic-schema",
        default=os.getenv("MILLRACE_KAFKA_TOPIC_SCHEMA", ""),
    )
    parser.add_argument(
        "--s3-endpoint-url",
        default=_env(
            "MILLRACE_S3_ENDPOINT_URL",
            "MILLRACE_MINIO_ENDPOINT",
            default="http://localhost:9000",
        ),
    )
    parser.add_argument(
        "--s3-access-key",
        default=_env("MILLRACE_S3_ACCESS_KEY", "MILLRACE_MINIO_ACCESS_KEY"),
    )
    parser.add_argument(
        "--s3-secret-key",
        default=_env("MILLRACE_S3_SECRET_KEY", "MILLRACE_MINIO_SECRET_KEY"),
    )
    parser.add_argument(
        "--s3-bucket",
        default=_env("MILLRACE_S3_BUCKET", "MILLRACE_MINIO_BUCKET", default="millrace"),
    )
    parser.add_argument(
        "--s3-region",
        default=os.getenv("MILLRACE_S3_REGION", "us-east-1"),
    )
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default=os.getenv("MILLRACE_KAFKA_STARTING_OFFSETS", "earliest"),
    )
    arguments = parser.parse_args(argv)
    if arguments.s3_access_key is None:
        parser.error("--s3-access-key or MILLRACE_S3_ACCESS_KEY is required")
    if arguments.s3_secret_key is None:
        parser.error("--s3-secret-key or MILLRACE_S3_SECRET_KEY is required")
    return StreamingConfig(
        run_id=arguments.run_id,
        batch_id=arguments.batch_id,
        kafka_bootstrap_servers=arguments.kafka_bootstrap_servers,
        kafka_topic_prefix=arguments.kafka_topic_prefix,
        kafka_topic_schema=arguments.kafka_topic_schema,
        s3_endpoint_url=arguments.s3_endpoint_url,
        s3_access_key=arguments.s3_access_key,
        s3_secret_key=arguments.s3_secret_key,
        s3_bucket=arguments.s3_bucket,
        s3_region=arguments.s3_region,
        starting_offsets=arguments.starting_offsets,
        input_mode=arguments.input_mode,
        bronze_uri=arguments.bronze_uri,
        partition_end=arguments.partition_end,
    )


def _env(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default
