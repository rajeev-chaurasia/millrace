from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest

from millrace.streaming.models import (
    CdcEvent,
    Entity,
    KafkaMetadata,
    MalformedEvent,
    Operation,
    ReasonCode,
    Tombstone,
)
from millrace.streaming.parser import parse_debezium_event


def _metadata(topic: str = "millrace.public.customers") -> KafkaMetadata:
    return KafkaMetadata(
        topic=topic,
        partition=2,
        offset=91,
        timestamp=datetime(2026, 8, 14, 2, tzinfo=UTC),
    )


def _payload(
    *,
    operation: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    lsn: int = 9001,
) -> str:
    return json.dumps(
        {
            "schema": {"type": "struct"},
            "payload": {
                "before": before,
                "after": after,
                "op": operation,
                "source": {"lsn": lsn, "ts_ms": 1_776_124_800_000},
                "transaction": {"total_order": "4"},
            },
        }
    )


def test_parse_create_preserves_source_and_kafka_metadata() -> None:
    row = {
        "customer_id": 7,
        "email": "customer@example.com",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "status": "active",
        "updated_at": "2026-08-14T01:00:00Z",
        "batch_id": 12,
    }

    result = parse_debezium_event(
        _payload(operation="c", before=None, after=row),
        key_payload='{"payload":{"customer_id":7}}',
        metadata=_metadata(),
    )

    assert isinstance(result, CdcEvent)
    assert result.entity is Entity.CUSTOMERS
    assert result.operation is Operation.CREATE
    assert result.after == row
    assert result.batch_id == 12
    assert result.source_lsn == 9001
    assert result.transaction_order == 4
    assert result.kafka.partition == 2
    assert result.kafka.offset == 91


def test_parse_update_uses_after_image() -> None:
    result = parse_debezium_event(
        _payload(
            operation="u",
            before={
                "product_id": 5,
                "sku": "SKU-5",
                "name": "Keyboard",
                "category": "input",
                "unit_price": 10.0,
                "active": True,
                "updated_at": "2026-08-14T01:00:00Z",
                "batch_id": 20,
            },
            after={
                "product_id": 5,
                "sku": "SKU-5",
                "name": "Keyboard",
                "category": "input",
                "unit_price": 11.5,
                "active": True,
                "updated_at": "2026-08-14T02:00:00Z",
                "batch_id": 21,
            },
        ),
        key_payload='{"product_id":5}',
        metadata=_metadata("millrace.public.products"),
    )

    assert isinstance(result, CdcEvent)
    assert result.operation is Operation.UPDATE
    assert result.row is not None
    assert result.row["unit_price"] == 11.5
    assert result.batch_id == 21


def test_parse_delete_uses_before_image() -> None:
    result = parse_debezium_event(
        _payload(
            operation="d",
            before={
                "order_id": 44,
                "customer_id": 7,
                "ordered_at": "2026-08-14T01:00:00Z",
                "status": "cancelled",
                "updated_at": "2026-08-14T02:00:00Z",
                "batch_id": 22,
            },
            after=None,
        ),
        key_payload='{"order_id":44}',
        metadata=_metadata("millrace.public.orders"),
    )

    assert isinstance(result, CdcEvent)
    assert result.operation is Operation.DELETE
    assert result.row is not None
    assert result.row["order_id"] == 44


def test_parse_tombstone_is_not_malformed() -> None:
    result = parse_debezium_event(
        None,
        key_payload='{"order_id":44,"line_number":3}',
        metadata=_metadata("millrace.public.order_items"),
    )

    assert isinstance(result, Tombstone)
    assert result.key == {"order_id": 44, "line_number": 3}


def test_parse_precise_debezium_decimal() -> None:
    row = {
        "product_id": 5,
        "sku": "SKU-5",
        "name": "Keyboard",
        "category": "input",
        "unit_price": "B8s=",
        "active": True,
        "updated_at": "2026-08-14T02:00:00Z",
        "batch_id": 21,
    }
    product_schema = {
        "type": "struct",
        "fields": [
            {
                "type": "bytes",
                "name": "org.apache.kafka.connect.data.Decimal",
                "parameters": {"scale": "2"},
                "field": "unit_price",
            }
        ],
    }
    payload = json.dumps(
        {
            "schema": {
                "type": "struct",
                "fields": [
                    {**product_schema, "field": "before"},
                    {**product_schema, "field": "after"},
                ],
            },
            "payload": {
                "before": None,
                "after": row,
                "op": "c",
                "source": {"lsn": 9001},
            },
        }
    )

    result = parse_debezium_event(
        payload,
        key_payload='{"product_id":5}',
        metadata=_metadata("millrace.public.products"),
    )

    assert isinstance(result, CdcEvent)
    assert result.after is not None
    assert result.after["unit_price"] == "19.95"


@pytest.mark.parametrize(
    ("payload", "topic", "reason"),
    [
        ("{", "millrace.public.customers", ReasonCode.INVALID_JSON),
        (
            '{"op":"x","source":{"lsn":1}}',
            "millrace.public.customers",
            ReasonCode.UNSUPPORTED_OPERATION,
        ),
        (
            '{"op":"c","after":{"customer_id":1},"source":{}}',
            "millrace.public.customers",
            ReasonCode.MISSING_SOURCE_LSN,
        ),
        (
            '{"op":"c","after":{"customer_id":"bad","email":"a@b","first_name":"a",'
            '"last_name":"b","status":"active","updated_at":"2026-08-14T00:00:00Z",'
            '"batch_id":1},"source":{"lsn":1}}',
            "millrace.public.customers",
            ReasonCode.INVALID_SCHEMA,
        ),
        (
            '{"op":"c","after":{"customer_id":1,"email":"a@b","first_name":"a",'
            '"last_name":"b","status":"active","updated_at":"2026-08-14T00:00:00Z"},'
            '"source":{"lsn":1}}',
            "millrace.public.customers",
            ReasonCode.MISSING_BATCH_ID,
        ),
        (
            '{"op":"c","after":{"id":1,"batch_id":1},"source":{"lsn":1}}',
            "millrace.public.unknown",
            ReasonCode.UNKNOWN_TOPIC,
        ),
    ],
)
def test_malformed_payloads_have_stable_reason_codes(
    payload: str,
    topic: str,
    reason: ReasonCode,
) -> None:
    result = parse_debezium_event(
        payload,
        key_payload='{"customer_id":1}',
        metadata=_metadata(topic),
    )

    assert isinstance(result, MalformedEvent)
    assert result.reason_code is reason
    assert result.kafka.offset == 91
    assert result.raw_payload == payload
