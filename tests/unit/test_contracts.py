import hashlib
from datetime import UTC, datetime

import pytest

from millrace.contracts import RunContext


def test_run_context_builds_stable_storage_key() -> None:
    context = RunContext(
        run_id="scheduled__2026-08-14T01:00:00+00:00",
        batch_id=42,
        interval_start=datetime(2026, 8, 14, 1, tzinfo=UTC),
        interval_end=datetime(2026, 8, 14, 2, tzinfo=UTC),
    )

    digest = hashlib.sha256(context.run_id.encode("utf-8")).hexdigest()
    assert context.storage_key == f"2026-08-14/0000000042_{digest}"


def test_run_context_rejects_invalid_interval() -> None:
    timestamp = datetime(2026, 8, 14, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="interval_start"):
        RunContext(
            run_id="run",
            batch_id=1,
            interval_start=timestamp,
            interval_end=timestamp,
        )


def test_run_context_rejects_naive_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone"):
        RunContext(
            run_id="run",
            batch_id=1,
            interval_start=datetime(2026, 8, 14, 1),
            interval_end=datetime(2026, 8, 14, 2),
        )
