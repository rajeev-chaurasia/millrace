from __future__ import annotations

from millrace.streaming.models import CdcEvent, Entity, JsonObject, KafkaMetadata, Operation
from millrace.streaming.state import build_current_state, deduplicate_events


def _event(
    operation: Operation,
    *,
    lsn: int,
    offset: int,
    batch_id: int,
    price: float,
) -> CdcEvent:
    row: JsonObject = {"product_id": 3, "price": price, "batch_id": batch_id}
    return CdcEvent(
        entity=Entity.PRODUCTS,
        operation=operation,
        key={"product_id": 3},
        before=row if operation is Operation.DELETE else None,
        after=None if operation is Operation.DELETE else row,
        batch_id=batch_id,
        source_lsn=lsn,
        source_timestamp_ms=None,
        transaction_order=0,
        kafka=KafkaMetadata(topic="millrace.products", partition=0, offset=offset),
        raw_payload=None,
    )


def test_deduplicate_uses_source_lsn_before_kafka_offset() -> None:
    newer_offset = _event(Operation.UPDATE, lsn=100, offset=20, batch_id=2, price=20.0)
    newer_source = _event(Operation.UPDATE, lsn=101, offset=10, batch_id=3, price=30.0)

    result = deduplicate_events([newer_source, newer_offset])

    assert result == [newer_source]


def test_current_state_applies_create_update_and_delete() -> None:
    created = _event(Operation.CREATE, lsn=100, offset=1, batch_id=1, price=10.0)
    updated = _event(Operation.UPDATE, lsn=101, offset=2, batch_id=2, price=12.0)
    deleted = _event(Operation.DELETE, lsn=102, offset=3, batch_id=3, price=12.0)

    assert build_current_state([updated, created, deleted]) == []
    state_before_delete = build_current_state(
        [updated, created, deleted],
        max_batch_id=2,
    )
    assert len(state_before_delete) == 1
    assert state_before_delete[0].row["price"] == 12.0
    assert state_before_delete[0].batch_id == 2
