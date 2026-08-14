from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from millrace.validation.canonical import canonicalize, column_checksum
from millrace.validation.models import CanonicalType, ColumnRule


def test_checksum_is_order_independent_and_value_sensitive() -> None:
    key = ColumnRule(name="id", canonical_type=CanonicalType.INTEGER)
    value = ColumnRule(name="amount", canonical_type=CanonicalType.DECIMAL)
    rows = [{"id": 2, "amount": Decimal("1.00")}, {"id": 1, "amount": Decimal("2.0")}]

    checksum = column_checksum(rows, column=value, key_columns=[key])

    assert checksum == column_checksum(list(reversed(rows)), column=value, key_columns=[key])
    assert checksum != column_checksum(
        [{"id": 1, "amount": Decimal("2.1")}, {"id": 2, "amount": Decimal("1")}],
        column=value,
        key_columns=[key],
    )


def test_canonical_types_normalize_equivalent_values() -> None:
    assert canonicalize(Decimal("1.000"), CanonicalType.DECIMAL) == "M:1"
    assert canonicalize("e\u0301", CanonicalType.TEXT) == canonicalize("\u00e9", CanonicalType.TEXT)
    assert (
        canonicalize(
            datetime(2026, 1, 1, tzinfo=UTC),
            CanonicalType.TIMESTAMP,
        )
        == "T:2026-01-01T00:00:00.000000Z"
    )


def test_naive_timestamp_fails_closed() -> None:
    with pytest.raises(ValueError, match="timezone"):
        canonicalize(datetime(2026, 1, 1), CanonicalType.TIMESTAMP)
