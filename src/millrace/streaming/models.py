from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TypeAlias

JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]


class Entity(StrEnum):
    CUSTOMERS = "customers"
    PRODUCTS = "products"
    ORDERS = "orders"
    ORDER_ITEMS = "order_items"


class Operation(StrEnum):
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"
    TOMBSTONE = "t"


class ReasonCode(StrEnum):
    INVALID_JSON = "invalid_json"
    INVALID_ENVELOPE = "invalid_envelope"
    UNKNOWN_TOPIC = "unknown_topic"
    UNSUPPORTED_OPERATION = "unsupported_operation"
    MISSING_KEY = "missing_key"
    MISSING_SOURCE_LSN = "missing_source_lsn"
    MISSING_BATCH_ID = "missing_batch_id"
    INVALID_SCHEMA = "invalid_schema"


@dataclass(frozen=True, slots=True)
class KafkaMetadata:
    topic: str
    partition: int
    offset: int
    timestamp: datetime | None = None

    def __post_init__(self) -> None:
        if not self.topic.strip():
            raise ValueError("topic must not be empty")
        if self.partition < 0:
            raise ValueError("partition must not be negative")
        if self.offset < 0:
            raise ValueError("offset must not be negative")
        if self.timestamp is not None and self.timestamp.tzinfo is None:
            raise ValueError("Kafka timestamp must include a timezone")


@dataclass(frozen=True, slots=True)
class CdcEvent:
    entity: Entity
    operation: Operation
    key: JsonObject
    before: JsonObject | None
    after: JsonObject | None
    batch_id: int
    source_lsn: int
    source_timestamp_ms: int | None
    transaction_order: int
    kafka: KafkaMetadata
    raw_payload: str | None

    @property
    def row(self) -> JsonObject | None:
        return self.before if self.operation is Operation.DELETE else self.after

    @property
    def source_order(self) -> tuple[int, int, int, int]:
        return (
            self.source_lsn,
            self.transaction_order,
            self.kafka.partition,
            self.kafka.offset,
        )


@dataclass(frozen=True, slots=True)
class Tombstone:
    entity: Entity
    key: JsonObject
    kafka: KafkaMetadata


@dataclass(frozen=True, slots=True)
class MalformedEvent:
    reason_code: ReasonCode
    detail: str
    kafka: KafkaMetadata
    raw_payload: str | None


ParseResult: TypeAlias = CdcEvent | Tombstone | MalformedEvent
