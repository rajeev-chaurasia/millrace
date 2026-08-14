from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from urllib.parse import quote

from millrace.streaming.models import Entity


def stable_component(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("storage component must not be empty")
    return quote(stripped, safe="-_.~")


@dataclass(frozen=True, slots=True)
class StoragePaths:
    bucket: str
    pipeline_name: str = "millrace_cdc"

    def __post_init__(self) -> None:
        if not self.bucket.strip() or "/" in self.bucket:
            raise ValueError("bucket must be a non-empty S3 bucket name")
        stable_component(self.pipeline_name)

    @property
    def root(self) -> str:
        return f"s3a://{self.bucket}"

    def bronze_table(self, entity: Entity) -> str:
        return f"{self.root}/bronze/table={entity.value}"

    def bronze_run(
        self,
        entity: Entity,
        *,
        event_date: date,
        run_id: str,
        stream_batch_id: int,
    ) -> str:
        if stream_batch_id < 0:
            raise ValueError("stream_batch_id must not be negative")
        return (
            f"{self.bronze_table(entity)}/event_date={event_date.isoformat()}"
            f"/run_id={stable_component(run_id)}"
            f"/stream_batch_id={stream_batch_id:020d}"
        )

    def silver_table(self, entity: Entity, *, run_id: str) -> str:
        return f"{self.root}/silver/run_id={stable_component(run_id)}/table={entity.value}"

    def dead_letter_run(
        self,
        entity: Entity,
        *,
        run_id: str,
        stream_batch_id: int,
    ) -> str:
        if stream_batch_id < 0:
            raise ValueError("stream_batch_id must not be negative")
        return (
            f"{self.root}/bronze/dead_letter/table={entity.value}"
            f"/run_id={stable_component(run_id)}"
            f"/stream_batch_id={stream_batch_id:020d}"
        )

    def checkpoint(self, *, topic: str) -> str:
        return (
            f"{self.root}/checkpoints/pipeline={stable_component(self.pipeline_name)}"
            f"/source={stable_component(topic)}"
        )
