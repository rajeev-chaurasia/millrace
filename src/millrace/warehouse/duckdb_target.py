from __future__ import annotations

# Dynamic identifiers are validated and quoted before use.
from collections.abc import Sequence
from typing import Any

import duckdb

from millrace.settings import Settings
from millrace.warehouse.dialect import DUCKDB, Dialect


def configure_object_store(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> None:
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(
        """
        CREATE OR REPLACE SECRET millrace_s3 (
            TYPE s3,
            KEY_ID ?,
            SECRET ?,
            REGION ?,
            ENDPOINT ?,
            URL_STYLE 'path',
            USE_SSL ?
        )
        """,
        [
            settings.s3_access_key,
            settings.s3_secret_key.get_secret_value(),
            settings.s3_region,
            settings.s3_endpoint_url.removeprefix("http://").removeprefix("https://"),
            settings.s3_endpoint_url.startswith("https://"),
        ],
    )


class DuckDbGateway:
    """Thin `WarehouseGateway` wrapper, kept only so `open_warehouse` returns a
    uniform type across targets. Existing DuckDB validation classes take the
    raw connection via the `.connection` escape hatch instead of this gateway,
    so their behavior is unchanged.
    """

    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.connection = connection

    @property
    def dialect(self) -> Dialect:
        return DUCKDB

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self.connection.execute(sql, list(params))

    def query(
        self, sql: str, params: Sequence[Any] = ()
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        cursor = self.connection.execute(sql, list(params))
        names = [str(description[0]) for description in cursor.description or ()]
        return names, cursor.fetchall()

    def begin(self) -> None:
        self.connection.begin()

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def close(self) -> None:
        self.connection.close()
