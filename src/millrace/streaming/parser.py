from __future__ import annotations

import base64
import binascii
import json
from decimal import Decimal
from typing import cast

from millrace.streaming.models import (
    CdcEvent,
    JsonObject,
    JsonValue,
    KafkaMetadata,
    MalformedEvent,
    Operation,
    ParseResult,
    ReasonCode,
    Tombstone,
)
from millrace.streaming.schemas import ENTITY_SPECS, entity_from_topic


def parse_debezium_event(
    payload: str | bytes | None,
    *,
    metadata: KafkaMetadata,
    key_payload: str | bytes | None = None,
) -> ParseResult:
    raw_payload = _decode(payload)
    entity = entity_from_topic(metadata.topic)
    if entity is None:
        return _malformed(
            ReasonCode.UNKNOWN_TOPIC,
            "topic does not map to an entity",
            metadata,
            raw_payload,
        )

    key, key_error = _parse_key(key_payload)
    if payload is None:
        if key_error is not None:
            return _malformed(ReasonCode.MISSING_KEY, key_error, metadata, raw_payload)
        spec_key = ENTITY_SPECS[entity].find_key(key)
        if spec_key is None:
            return _malformed(
                ReasonCode.MISSING_KEY,
                "tombstone key does not match the entity schema",
                metadata,
                raw_payload,
            )
        return Tombstone(entity=entity, key=spec_key, kafka=metadata)

    document, error = _parse_object(raw_payload)
    if error is not None:
        return _malformed(ReasonCode.INVALID_JSON, error, metadata, raw_payload)
    envelope = _unwrap_payload(document)
    if envelope is None:
        return _malformed(
            ReasonCode.INVALID_ENVELOPE,
            "Debezium envelope payload must be an object",
            metadata,
            raw_payload,
        )
    envelope = _normalize_decimal_fields(document, envelope)

    operation = _operation(envelope.get("op"))
    if operation is None:
        return _malformed(
            ReasonCode.UNSUPPORTED_OPERATION,
            "op must be one of c, u, d, or r",
            metadata,
            raw_payload,
        )

    before = _optional_object(envelope.get("before"))
    after = _optional_object(envelope.get("after"))
    row = before if operation is Operation.DELETE else after
    if row is None:
        expected = "before" if operation is Operation.DELETE else "after"
        return _malformed(
            ReasonCode.INVALID_ENVELOPE,
            f"{expected} must be an object for operation {operation.value}",
            metadata,
            raw_payload,
        )

    source = _optional_object(envelope.get("source"))
    if source is None:
        return _malformed(
            ReasonCode.INVALID_ENVELOPE,
            "source must be an object",
            metadata,
            raw_payload,
        )
    source_lsn = _non_negative_int(source.get("lsn"))
    if source_lsn is None:
        return _malformed(
            ReasonCode.MISSING_SOURCE_LSN,
            "source.lsn must be a non-negative integer",
            metadata,
            raw_payload,
        )

    spec = ENTITY_SPECS[entity]
    schema_error = spec.validate_row(row)
    if schema_error is not None:
        return _malformed(ReasonCode.INVALID_SCHEMA, schema_error, metadata, raw_payload)

    row_key = spec.find_key(row)
    if row_key is None:
        return _malformed(
            ReasonCode.INVALID_SCHEMA,
            "row key failed schema validation",
            metadata,
            raw_payload,
        )
    if key_error is not None:
        return _malformed(ReasonCode.MISSING_KEY, key_error, metadata, raw_payload)
    message_key = spec.find_key(key)
    if message_key is None:
        return _malformed(
            ReasonCode.MISSING_KEY,
            "Kafka key does not match the entity schema",
            metadata,
            raw_payload,
        )
    if message_key != row_key:
        return _malformed(
            ReasonCode.INVALID_SCHEMA,
            "Kafka key does not match the row key",
            metadata,
            raw_payload,
        )
    key = message_key

    batch_id = _positive_int(row.get("batch_id"))
    if batch_id is None:
        batch_id = _positive_int(source.get("batch_id"))
    if batch_id is None:
        return _malformed(
            ReasonCode.MISSING_BATCH_ID,
            "batch_id must be a positive integer",
            metadata,
            raw_payload,
        )

    transaction = _optional_object(envelope.get("transaction")) or {}
    transaction_order = (
        _non_negative_int(transaction.get("data_collection_order"))
        or _non_negative_int(transaction.get("total_order"))
        or 0
    )
    source_timestamp_ms = _non_negative_int(source.get("ts_ms"))
    if source_timestamp_ms is None:
        source_timestamp_ms = _non_negative_int(envelope.get("ts_ms"))

    return CdcEvent(
        entity=entity,
        operation=operation,
        key=key,
        before=before,
        after=after,
        batch_id=batch_id,
        source_lsn=source_lsn,
        source_timestamp_ms=source_timestamp_ms,
        transaction_order=transaction_order,
        kafka=metadata,
        raw_payload=raw_payload,
    )


def _decode(payload: str | bytes | None) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, str):
        return payload
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return payload.decode("utf-8", errors="replace")


def _parse_object(payload: str | None) -> tuple[JsonObject, str | None]:
    if payload is None:
        return {}, "payload is null"
    try:
        value = cast(JsonValue, json.loads(payload))
    except json.JSONDecodeError as error:
        return {}, f"payload is not valid JSON: {error.msg}"
    if not isinstance(value, dict):
        return {}, "payload must be a JSON object"
    return value, None


def _parse_key(payload: str | bytes | None) -> tuple[JsonObject, str | None]:
    raw_key = _decode(payload)
    if raw_key is None:
        return {}, "Kafka key is missing"
    key, error = _parse_object(raw_key)
    if error is not None:
        return {}, f"Kafka key is invalid: {error}"
    unwrapped = _unwrap_payload(key)
    if unwrapped is None:
        return {}, "Kafka key payload must be an object"
    return unwrapped, None


def _unwrap_payload(value: JsonObject) -> JsonObject | None:
    if "schema" not in value and "payload" not in value:
        return value
    return _optional_object(value.get("payload"))


def _normalize_decimal_fields(document: JsonObject, envelope: JsonObject) -> JsonObject:
    schema = _optional_object(document.get("schema"))
    if schema is None:
        return envelope
    schema_fields = schema.get("fields")
    if not isinstance(schema_fields, list):
        return envelope

    normalized = dict(envelope)
    for image_name in ("before", "after"):
        image = _optional_object(envelope.get(image_name))
        image_schema = _named_field_schema(schema_fields, image_name)
        if image is None or image_schema is None:
            continue
        image_fields = image_schema.get("fields")
        if not isinstance(image_fields, list):
            continue
        normalized[image_name] = _normalize_decimal_row(image, image_fields)
    return normalized


def _named_field_schema(fields: list[JsonValue], name: str) -> JsonObject | None:
    for field in fields:
        if isinstance(field, dict) and field.get("field") == name:
            return field
    return None


def _normalize_decimal_row(row: JsonObject, fields: list[JsonValue]) -> JsonObject:
    normalized = dict(row)
    for field in fields:
        if not isinstance(field, dict):
            continue
        field_name = field.get("field")
        if not isinstance(field_name, str):
            continue
        value = row.get(field_name)
        scale = _decimal_scale(field)
        if scale is not None and isinstance(value, str):
            decoded = _decode_decimal(value, scale)
            if decoded is not None:
                normalized[field_name] = decoded
    return normalized


def _decimal_scale(field: JsonObject) -> int | None:
    if field.get("name") != "org.apache.kafka.connect.data.Decimal":
        return None
    parameters = _optional_object(field.get("parameters"))
    if parameters is None:
        return None
    return _non_negative_int(parameters.get("scale"))


def _decode_decimal(value: str, scale: int) -> str | None:
    try:
        encoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        return None
    if not encoded:
        return None
    unscaled = int.from_bytes(encoded, byteorder="big", signed=True)
    return format(Decimal(unscaled).scaleb(-scale), "f")


def _optional_object(value: JsonValue | None) -> JsonObject | None:
    return value if isinstance(value, dict) else None


def _operation(value: JsonValue | None) -> Operation | None:
    if not isinstance(value, str):
        return None
    try:
        operation = Operation(value)
    except ValueError:
        return None
    return operation if operation is not Operation.TOMBSTONE else None


def _non_negative_int(value: JsonValue | None) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _positive_int(value: JsonValue | None) -> int | None:
    parsed = _non_negative_int(value)
    return parsed if parsed is not None and parsed > 0 else None


def _malformed(
    reason_code: ReasonCode,
    detail: str,
    metadata: KafkaMetadata,
    raw_payload: str | None,
) -> MalformedEvent:
    return MalformedEvent(
        reason_code=reason_code,
        detail=detail,
        kafka=metadata,
        raw_payload=raw_payload,
    )
