from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from millrace.warehouse.dialect import Dialect


class WarehouseGateway(Protocol):
    """Uniform SQL surface a Snowflake-side validation class depends on.

    DuckDB code is untouched by this Protocol: `DuckDbCandidateReader`,
    `DuckDbAuditWriter`, and the DuckDB publication path keep taking a raw
    `duckdb.DuckDBPyConnection` directly, exactly as before this uplift, so
    their existing behavior and tests are unaffected. This Protocol exists
    only so the new Snowflake reader, audit writer, and publication strategy
    share one execution surface instead of each re-wrapping the Snowflake
    connector.
    """

    @property
    def dialect(self) -> Dialect: ...

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None: ...

    def query(
        self, sql: str, params: Sequence[Any] = ()
    ) -> tuple[list[str], list[tuple[Any, ...]]]: ...

    def begin(self) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...
