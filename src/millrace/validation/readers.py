from __future__ import annotations

# Dynamic identifiers are validated and quoted before use.
# ruff: noqa: S608
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, LiteralString, Protocol, cast

import duckdb
import psycopg

from millrace.contracts import RunContext
from millrace.validation.canonical import Row
from millrace.validation.configuration import (
    IdentifierCase,
    quote_identifier,
    quote_relation,
    render_relation,
)
from millrace.validation.models import CanonicalType, EntityRule
from millrace.warehouse.gateway import WarehouseGateway


class SnapshotReader(Protocol):
    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]: ...


class TombstoneReader(Protocol):
    def fetch_deleted_keys(self, entity: EntityRule, context: RunContext) -> Sequence[Row]: ...


class PostgresHistoryReader:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        selected = ", ".join(quote_identifier(column.name) for column in entity.columns)
        keys = ", ".join(quote_identifier(column) for column in entity.key_columns)
        batch = quote_identifier(entity.source.batch_column)
        ordering = batch
        if entity.source.sequence_column is not None:
            sequence = quote_identifier(entity.source.sequence_column)
            ordering = f"{batch} DESC, {sequence} DESC"
        else:
            ordering = f"{batch} DESC"
        relation = quote_relation(entity.source.relation)
        delete_filter = ""
        if entity.source.deleted_column is not None:
            deleted = quote_identifier(entity.source.deleted_column)
            delete_filter = f" AND COALESCE({deleted}, FALSE) = FALSE"
        query = (
            f"SELECT {selected} FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {ordering}) AS _mr_rank "
            f"FROM {relation} WHERE {batch} <= %s"
            f") AS history WHERE _mr_rank = 1{delete_filter}"
        )
        with psycopg.connect(self._dsn) as connection, connection.cursor() as cursor:
            cursor.execute(cast(LiteralString, query), (context.batch_id,))
            records = cursor.fetchall()
            names = [description.name for description in cursor.description or ()]
        rows = [dict(zip(names, record, strict=True)) for record in records]
        _ensure_unique_keys(rows, entity)
        return rows


class PostgresTombstoneReader:
    """Deletes are only implicit in `PostgresHistoryReader`: a tombstoned row simply
    disappears from `fetch_rows`, so a target that wrongly keeps it surfaces as a
    row_count/checksum mismatch rather than a labeled delete failure. This class
    exists to make that check explicit and diagnosable. Its query intentionally
    duplicates a small amount of `PostgresHistoryReader`'s ranking logic rather
    than sharing it, so a refactor here can never risk the source-history query
    that reader has already proven correct.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def fetch_deleted_keys(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        if entity.source.deleted_column is None:
            return []
        selected = ", ".join(quote_identifier(column) for column in entity.key_columns)
        keys = ", ".join(quote_identifier(column) for column in entity.key_columns)
        batch = quote_identifier(entity.source.batch_column)
        if entity.source.sequence_column is not None:
            sequence = quote_identifier(entity.source.sequence_column)
            ordering = f"{batch} DESC, {sequence} DESC"
        else:
            ordering = f"{batch} DESC"
        relation = quote_relation(entity.source.relation)
        deleted = quote_identifier(entity.source.deleted_column)
        query = (
            f"SELECT {selected} FROM ("
            f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {ordering}) AS _mr_rank "
            f"FROM {relation} WHERE {batch} <= %s"
            f") AS history WHERE _mr_rank = 1 AND COALESCE({deleted}, FALSE) = TRUE"
        )
        with psycopg.connect(self._dsn) as connection, connection.cursor() as cursor:
            cursor.execute(cast(LiteralString, query), (context.batch_id,))
            records = cursor.fetchall()
            names = [description.name for description in cursor.description or ()]
        return [dict(zip(names, record, strict=True)) for record in records]


class DuckDbCandidateReader:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self._connection = connection

    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        selected = ", ".join(quote_identifier(column.name) for column in entity.columns)
        relation = render_relation(entity.target.relation, context)
        batch = quote_identifier(entity.target.batch_column)
        cursor = self._connection.execute(
            f"SELECT {selected} FROM {relation} WHERE {batch} <= ?",
            [context.batch_id],
        )
        records = cursor.fetchall()
        names = [str(description[0]) for description in cursor.description or ()]
        rows = [dict(zip(names, record, strict=True)) for record in records]
        _ensure_unique_keys(rows, entity)
        return rows


class SnowflakeCandidateReader:
    """Mirrors `DuckDbCandidateReader`. Two real differences: Snowflake returns
    uppercase column names and `%s` placeholders, both handled here; and
    Snowflake's connector returns `Decimal` for some NUMBER-typed columns where
    DuckDB returns a native `int`/`bool`. `_coerce_for_snowflake` normalizes
    only those two lossless, unambiguous cases at this boundary. Timestamps are
    never coerced: a naive datetime must keep failing `canonicalize` closed,
    exactly as it does today for DuckDB.
    """

    def __init__(self, gateway: WarehouseGateway) -> None:
        self._gateway = gateway

    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        selected = ", ".join(
            quote_identifier(column.name, case=IdentifierCase.UPPER) for column in entity.columns
        )
        relation = render_relation(entity.target.relation, context, case=IdentifierCase.UPPER)
        batch = quote_identifier(entity.target.batch_column, case=IdentifierCase.UPPER)
        names, records = self._gateway.query(
            f"SELECT {selected} FROM {relation} WHERE {batch} <= %s",
            [context.batch_id],
        )
        lowered_names = [name.lower() for name in names]
        columns_by_name = {column.name: column for column in entity.columns}
        rows = [
            {
                name: _coerce_for_snowflake(value, columns_by_name[name].canonical_type)
                for name, value in zip(lowered_names, record, strict=True)
            }
            for record in records
        ]
        _ensure_unique_keys(rows, entity)
        return rows


def _coerce_for_snowflake(value: Any, canonical_type: CanonicalType) -> Any:
    if isinstance(value, Decimal):
        if canonical_type is CanonicalType.INTEGER and value == value.to_integral_value():
            return int(value)
        if canonical_type is CanonicalType.BOOLEAN and value in (0, 1):
            return bool(value)
    elif canonical_type is CanonicalType.BOOLEAN and isinstance(value, int) and value in (0, 1):
        return bool(value)
    return value


def _ensure_unique_keys(rows: Sequence[Mapping[str, Any]], entity: EntityRule) -> None:
    keys = [tuple(row[column] for column in entity.key_columns) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError(f"entity {entity.name!r} contains duplicate business keys")
