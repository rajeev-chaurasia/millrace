from datetime import date

from millrace.streaming.models import Entity
from millrace.streaming.storage import StoragePaths, stable_component


def test_storage_keys_are_stable_and_run_scoped() -> None:
    paths = StoragePaths("millrace")
    run_id = "scheduled__2026-08-14T01:00:00+00:00"

    assert paths.bronze_run(
        Entity.CUSTOMERS,
        event_date=date(2026, 8, 14),
        run_id=run_id,
        stream_batch_id=7,
    ) == (
        "s3a://millrace/bronze/table=customers/event_date=2026-08-14/"
        "run_id=scheduled__2026-08-14T01%3A00%3A00%2B00%3A00/"
        "stream_batch_id=00000000000000000007"
    )
    assert paths.silver_table(Entity.CUSTOMERS, run_id=run_id) == (
        "s3a://millrace/silver/run_id=scheduled__2026-08-14T01%3A00%3A00%2B00%3A00/table=customers"
    )
    assert paths.checkpoint(topic="millrace.public.customers") == (
        "s3a://millrace/checkpoints/pipeline=millrace_cdc/source=millrace.public.customers"
    )


def test_stable_component_rejects_empty_values() -> None:
    assert stable_component("run:1") == "run%3A1"
