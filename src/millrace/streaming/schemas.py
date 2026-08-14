from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType

from millrace.streaming.models import Entity, JsonObject, JsonValue


class ValueKind(StrEnum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    DECIMAL = "decimal"
    STRING = "string"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True, slots=True)
class EntitySpec:
    entity: Entity
    key_candidates: tuple[tuple[str, ...], ...]
    required_fields: frozenset[str]
    fields: Mapping[str, ValueKind]

    def validate_row(self, row: JsonObject) -> str | None:
        if not row:
            return "row must not be empty"
        missing = sorted(name for name in self.required_fields if row.get(name) is None)
        if missing:
            return f"row is missing required fields: {', '.join(missing)}"
        for name, value in row.items():
            expected = self.fields.get(name)
            if expected is not None and value is not None and not _matches_kind(value, expected):
                return f"field {name!r} must be {expected.value}"
        if self.find_key(row) is None:
            expected = " or ".join("+".join(candidate) for candidate in self.key_candidates)
            return f"row must contain key {expected}"
        return None

    def find_key(self, value: JsonObject) -> JsonObject | None:
        for candidate in self.key_candidates:
            if all(_is_key_value(value.get(name)) for name in candidate):
                return {name: value[name] for name in candidate}
        return None


def _matches_kind(value: JsonValue, expected: ValueKind) -> bool:
    if expected is ValueKind.BOOLEAN:
        return isinstance(value, bool)
    if expected is ValueKind.INTEGER:
        return isinstance(value, int) and not isinstance(value, bool)
    if expected is ValueKind.DECIMAL:
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            return False
        try:
            Decimal(str(value))
        except InvalidOperation:
            return False
        return True
    if expected is ValueKind.TIMESTAMP:
        return (
            isinstance(value, (str, int))
            and not isinstance(value, bool)
            and (not isinstance(value, str) or bool(value.strip()))
        )
    return isinstance(value, str)


def _is_key_value(value: JsonValue | None) -> bool:
    return (
        isinstance(value, (str, int))
        and not isinstance(value, bool)
        and (not isinstance(value, str) or bool(value.strip()))
    )


ENTITY_SPECS: Mapping[Entity, EntitySpec] = MappingProxyType(
    {
        Entity.CUSTOMERS: EntitySpec(
            entity=Entity.CUSTOMERS,
            key_candidates=(("customer_id",),),
            required_fields=frozenset(
                {
                    "customer_id",
                    "email",
                    "first_name",
                    "last_name",
                    "status",
                    "updated_at",
                }
            ),
            fields=MappingProxyType(
                {
                    "customer_id": ValueKind.INTEGER,
                    "email": ValueKind.STRING,
                    "first_name": ValueKind.STRING,
                    "last_name": ValueKind.STRING,
                    "status": ValueKind.STRING,
                    "updated_at": ValueKind.TIMESTAMP,
                    "batch_id": ValueKind.INTEGER,
                }
            ),
        ),
        Entity.PRODUCTS: EntitySpec(
            entity=Entity.PRODUCTS,
            key_candidates=(("product_id",),),
            required_fields=frozenset(
                {
                    "product_id",
                    "sku",
                    "name",
                    "category",
                    "unit_price",
                    "active",
                    "updated_at",
                }
            ),
            fields=MappingProxyType(
                {
                    "product_id": ValueKind.INTEGER,
                    "sku": ValueKind.STRING,
                    "name": ValueKind.STRING,
                    "category": ValueKind.STRING,
                    "unit_price": ValueKind.DECIMAL,
                    "active": ValueKind.BOOLEAN,
                    "updated_at": ValueKind.TIMESTAMP,
                    "batch_id": ValueKind.INTEGER,
                }
            ),
        ),
        Entity.ORDERS: EntitySpec(
            entity=Entity.ORDERS,
            key_candidates=(("order_id",),),
            required_fields=frozenset(
                {
                    "order_id",
                    "customer_id",
                    "ordered_at",
                    "status",
                    "updated_at",
                }
            ),
            fields=MappingProxyType(
                {
                    "order_id": ValueKind.INTEGER,
                    "customer_id": ValueKind.INTEGER,
                    "status": ValueKind.STRING,
                    "ordered_at": ValueKind.TIMESTAMP,
                    "updated_at": ValueKind.TIMESTAMP,
                    "batch_id": ValueKind.INTEGER,
                }
            ),
        ),
        Entity.ORDER_ITEMS: EntitySpec(
            entity=Entity.ORDER_ITEMS,
            key_candidates=(("order_id", "line_number"),),
            required_fields=frozenset(
                {
                    "order_id",
                    "line_number",
                    "product_id",
                    "quantity",
                    "unit_price",
                    "updated_at",
                }
            ),
            fields=MappingProxyType(
                {
                    "order_id": ValueKind.INTEGER,
                    "line_number": ValueKind.INTEGER,
                    "product_id": ValueKind.INTEGER,
                    "quantity": ValueKind.INTEGER,
                    "unit_price": ValueKind.DECIMAL,
                    "updated_at": ValueKind.TIMESTAMP,
                    "batch_id": ValueKind.INTEGER,
                }
            ),
        ),
    }
)


def entity_from_topic(topic: str) -> Entity | None:
    table_name = topic.rsplit(".", maxsplit=1)[-1].lower()
    try:
        return Entity(table_name)
    except ValueError:
        return None
