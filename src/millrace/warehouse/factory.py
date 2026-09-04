from __future__ import annotations

import duckdb

from millrace.settings import Settings
from millrace.warehouse.duckdb_target import DuckDbGateway, configure_object_store
from millrace.warehouse.gateway import WarehouseGateway


def open_warehouse(target: str, settings: Settings) -> WarehouseGateway:
    """Composition root for warehouse connections. `target` must be one of
    `settings.enabled_warehouse_targets`.

    The Snowflake import is deliberately deferred to inside this function
    rather than a module-level import. `millrace.validation.publication`,
    `.audit`, and `.readers` import `WarehouseGateway` from this package, and
    `millrace.warehouse.snowflake_target` imports back from
    `millrace.validation.configuration` (for `quote_identifier`). A
    module-level import here would make `millrace.warehouse.__init__`
    transitively depend on `millrace.validation` finishing first in one
    direction and `millrace.validation.__init__` depend on
    `millrace.warehouse` finishing first in the other, which is only livable
    by accident of which package a given process happens to import first.
    Deferring this one import lets `millrace.warehouse` finish initializing
    on its own before anything reaches into `millrace.validation`.
    """
    if target == "duckdb":
        connection = duckdb.connect(settings.duckdb_path)
        configure_object_store(connection, settings)
        return DuckDbGateway(connection)
    if target == "snowflake":
        from millrace.warehouse.snowflake_target import SnowflakeGateway
        from millrace.warehouse.snowflake_target import connect as connect_snowflake

        return SnowflakeGateway(connect_snowflake(settings))
    raise ValueError(f"unknown warehouse target: {target!r}")
