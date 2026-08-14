from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Self


class RunState(StrEnum):
    CREATED = "created"
    INGESTING = "ingesting"
    TRANSFORMING = "transforming"
    VALIDATING = "validating"
    PUBLISHED = "published"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RunContext:
    run_id: str
    batch_id: int
    interval_start: datetime
    interval_end: datetime

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if self.batch_id < 1:
            raise ValueError("batch_id must be positive")
        if self.interval_start.tzinfo is None or self.interval_end.tzinfo is None:
            raise ValueError("data interval timestamps must include a timezone")
        if self.interval_start >= self.interval_end:
            raise ValueError("interval_start must be before interval_end")

    @classmethod
    def from_iso(
        cls,
        *,
        run_id: str,
        batch_id: int,
        interval_start: str,
        interval_end: str,
    ) -> Self:
        start = datetime.fromisoformat(interval_start)
        end = datetime.fromisoformat(interval_end)
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("data interval timestamps must include a timezone")
        return cls(
            run_id=run_id,
            batch_id=batch_id,
            interval_start=start.astimezone(UTC),
            interval_end=end.astimezone(UTC),
        )

    @property
    def storage_key(self) -> str:
        run_digest = hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()
        return f"{self.interval_start:%Y-%m-%d}/{self.batch_id:010d}_{run_digest}"
