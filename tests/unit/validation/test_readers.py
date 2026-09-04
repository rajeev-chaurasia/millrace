from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from millrace.contracts import RunContext
from millrace.validation.models import CanonicalType, ColumnRule, EntityRule, SourceRule, TargetRule
from millrace.validation.readers import PostgresTombstoneReader, SnowflakeCandidateReader
from millrace.warehouse.dialect import SNOWFLAKE, Dialect


class FakeGateway:
    """Minimal `WarehouseGateway` double: only `.query()` is exercised by
    `SnowflakeCandidateReader`, so nothing else needs a real implementation.
    """

    def __init__(self, names: list[str], rows: list[tuple[Any, ...]]) -> None:
        self._names = names
        self._rows = rows
        self.queries: list[tuple[str, Sequence[Any]]] = []

    @property
    def dialect(self) -> Dialect:
        return SNOWFLAKE

    def query(
        self, sql: str, params: Sequence[Any] = ()
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        self.queries.append((sql, params))
        return self._names, self._rows

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        raise NotImplementedError

    def begin(self) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        raise NotImplementedError

    def rollback(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


def test_snowflake_reader_lowercases_uppercase_column_names() -> None:
    gateway = FakeGateway(
        names=["CUSTOMER_ID", "EMAIL"],
        rows=[(1, "a@example.com")],
    )
    reader = SnowflakeCandidateReader(gateway)

    rows = reader.fetch_rows(_customers_entity(), _context())

    assert rows == [{"customer_id": 1, "email": "a@example.com"}]


def test_snowflake_reader_coerces_zero_scale_decimal_to_int_for_integer_columns() -> None:
    gateway = FakeGateway(names=["CUSTOMER_ID", "EMAIL"], rows=[(Decimal(1), "a@example.com")])
    reader = SnowflakeCandidateReader(gateway)

    rows = reader.fetch_rows(_customers_entity(), _context())

    assert rows[0]["customer_id"] == 1
    assert isinstance(rows[0]["customer_id"], int)
    assert not isinstance(rows[0]["customer_id"], Decimal)


def test_snowflake_reader_leaves_non_integral_decimal_alone_to_fail_closed() -> None:
    gateway = FakeGateway(names=["CUSTOMER_ID", "EMAIL"], rows=[(Decimal("1.5"), "a@example.com")])
    reader = SnowflakeCandidateReader(gateway)

    rows = reader.fetch_rows(_customers_entity(), _context())

    assert rows[0]["customer_id"] == Decimal("1.5")


def test_snowflake_reader_coerces_zero_one_int_to_bool_for_boolean_columns() -> None:
    entity = EntityRule(
        name="products",
        source=SourceRule(relation="history.products", batch_column="batch_id"),
        target=TargetRule(relation="{candidate_schema}.products", batch_column="batch_id"),
        key_columns=("product_id",),
        columns=(
            ColumnRule(name="product_id", canonical_type=CanonicalType.INTEGER),
            ColumnRule(name="active", canonical_type=CanonicalType.BOOLEAN),
        ),
    )
    gateway = FakeGateway(names=["PRODUCT_ID", "ACTIVE"], rows=[(1, 1), (2, 0)])
    reader = SnowflakeCandidateReader(gateway)

    rows = reader.fetch_rows(entity, _context())

    assert rows[0]["active"] is True
    assert rows[1]["active"] is False


def test_tombstone_reader_returns_empty_without_a_configured_deleted_column() -> None:
    entity = EntityRule(
        name="customers",
        source=SourceRule(relation="history.customers", batch_column="batch_id"),
        target=TargetRule(relation="{candidate_schema}.customers", batch_column="batch_id"),
        key_columns=("customer_id",),
        columns=(ColumnRule(name="customer_id", canonical_type=CanonicalType.INTEGER),),
    )
    reader = PostgresTombstoneReader("postgresql://unused/unused")

    assert reader.fetch_deleted_keys(entity, _context()) == []


def _customers_entity() -> EntityRule:
    return EntityRule(
        name="customers",
        source=SourceRule(relation="history.customers", batch_column="batch_id"),
        target=TargetRule(relation="{candidate_schema}.customers", batch_column="batch_id"),
        key_columns=("customer_id",),
        columns=(
            ColumnRule(name="customer_id", canonical_type=CanonicalType.INTEGER),
            ColumnRule(name="email", canonical_type=CanonicalType.TEXT),
        ),
    )


def _context() -> RunContext:
    return RunContext(
        run_id="run-1",
        batch_id=7,
        interval_start=datetime(2026, 1, 1, tzinfo=UTC),
        interval_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
