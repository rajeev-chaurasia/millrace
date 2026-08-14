from __future__ import annotations

import hashlib
import math
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from millrace.validation.models import AggregateOperation, AggregateRule, CanonicalType, ColumnRule

Row = Mapping[str, Any]


def canonicalize(value: Any, canonical_type: CanonicalType) -> str:
    if value is None:
        return "N"
    if canonical_type is CanonicalType.TEXT:
        if not isinstance(value, str):
            raise TypeError(f"expected text, received {type(value).__name__}")
        return f"S:{unicodedata.normalize('NFC', value)}"
    if canonical_type is CanonicalType.DATE:
        parsed = _as_date(value)
        return f"D:{parsed.isoformat()}"
    if canonical_type is CanonicalType.TIMESTAMP:
        parsed = _as_datetime(value)
        if parsed.tzinfo is None:
            raise ValueError("timestamps must include a timezone")
        utc_value = parsed.astimezone(UTC)
        return f"T:{utc_value.isoformat(timespec='microseconds').replace('+00:00', 'Z')}"
    if canonical_type is CanonicalType.DECIMAL:
        return f"M:{_decimal_text(value)}"
    if canonical_type is CanonicalType.BOOLEAN:
        if not isinstance(value, bool):
            raise TypeError(f"expected Boolean, received {type(value).__name__}")
        return "B:1" if value else "B:0"
    if canonical_type is CanonicalType.INTEGER:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"expected integer, received {type(value).__name__}")
        return f"I:{value}"
    if canonical_type is CanonicalType.FLOAT:
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise TypeError(f"expected float, received {type(value).__name__}")
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError("non-finite floats cannot be reconciled")
        return f"F:{format(numeric, '.17g')}"
    raise ValueError(f"unsupported canonical type: {canonical_type}")


def column_checksum(
    rows: Sequence[Row],
    *,
    column: ColumnRule,
    key_columns: Sequence[ColumnRule],
) -> str:
    digest = hashlib.sha256()
    ordered = sorted(rows, key=lambda row: _row_key(row, key_columns))
    for row in ordered:
        for key_column in key_columns:
            _update_digest(digest, canonicalize(row[key_column.name], key_column.canonical_type))
        _update_digest(digest, canonicalize(row[column.name], column.canonical_type))
    return digest.hexdigest()


def grouped_counts(
    rows: Iterable[Row],
    columns: Sequence[ColumnRule],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = _display_key(row, columns)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def aggregate_values(
    rows: Sequence[Row],
    rule: AggregateRule,
    columns: Mapping[str, ColumnRule],
) -> dict[str, str]:
    groups: dict[str, list[Any]] = {}
    group_columns = [columns[name] for name in rule.group_by]
    for row in rows:
        key = _display_key(row, group_columns) if group_columns else "__all__"
        groups.setdefault(key, []).append(row[rule.column])

    value_type = columns[rule.column].canonical_type
    result: dict[str, str] = {}
    for key, values in sorted(groups.items()):
        non_null = [value for value in values if value is not None]
        if not non_null:
            aggregate: Any = None
        elif rule.operation is AggregateOperation.SUM:
            try:
                aggregate = sum((Decimal(str(value)) for value in non_null), start=Decimal(0))
            except InvalidOperation as exc:
                raise ValueError(f"aggregate {rule.name!r} contains non-numeric values") from exc
            value_type = CanonicalType.DECIMAL
        elif rule.operation is AggregateOperation.MIN:
            aggregate = min(non_null)
        elif rule.operation is AggregateOperation.MAX:
            aggregate = max(non_null)
        else:
            raise ValueError(f"unsupported aggregate operation: {rule.operation}")
        result[key] = canonicalize(aggregate, value_type)
    return result


def _row_key(row: Row, key_columns: Sequence[ColumnRule]) -> tuple[str, ...]:
    return tuple(canonicalize(row[column.name], column.canonical_type) for column in key_columns)


def _display_key(row: Row, columns: Sequence[ColumnRule]) -> str:
    return "|".join(canonicalize(row[column.name], column.canonical_type) for column in columns)


def _update_digest(digest: Any, value: str) -> None:
    encoded = value.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, byteorder="big"))
    digest.update(encoded)


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"expected date, received {type(value).__name__}")


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(f"expected timestamp, received {type(value).__name__}")


def _decimal_text(value: Any) -> str:
    if isinstance(value, bool):
        raise TypeError("Boolean values are not decimals")
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not decimal_value.is_finite():
        raise ValueError("non-finite decimals cannot be reconciled")
    normalized = decimal_value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")
