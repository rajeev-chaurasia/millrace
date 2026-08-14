from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

from millrace.streaming.models import CdcEvent, Entity, JsonObject, Operation


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    entity: Entity
    key: JsonObject
    row: JsonObject
    batch_id: int
    source_lsn: int


def canonical_key(entity: Entity, key: JsonObject) -> str:
    return f"{entity.value}:{json.dumps(key, sort_keys=True, separators=(',', ':'))}"


def deduplicate_events(
    events: Iterable[CdcEvent],
    *,
    max_batch_id: int | None = None,
) -> list[CdcEvent]:
    latest: dict[str, CdcEvent] = {}
    for event in events:
        if max_batch_id is not None and event.batch_id > max_batch_id:
            continue
        storage_key = canonical_key(event.entity, event.key)
        previous = latest.get(storage_key)
        if previous is None or _event_order(event) > _event_order(previous):
            latest[storage_key] = event
    return sorted(latest.values(), key=lambda event: canonical_key(event.entity, event.key))


def build_current_state(
    events: Iterable[CdcEvent],
    *,
    max_batch_id: int | None = None,
) -> list[SnapshotRecord]:
    state: list[SnapshotRecord] = []
    for event in deduplicate_events(events, max_batch_id=max_batch_id):
        if event.operation is Operation.DELETE:
            continue
        row = event.row
        if row is None:
            continue
        state.append(
            SnapshotRecord(
                entity=event.entity,
                key=event.key,
                row=row,
                batch_id=event.batch_id,
                source_lsn=event.source_lsn,
            )
        )
    return state


def _event_order(event: CdcEvent) -> tuple[int, int, int, int, str]:
    payload_tiebreaker = json.dumps(
        {
            "operation": event.operation.value,
            "before": event.before,
            "after": event.after,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return (*event.source_order, payload_tiebreaker)
