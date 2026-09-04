from __future__ import annotations

from millrace.warehouse.dialect import DUCKDB, SNOWFLAKE, Dialect
from millrace.warehouse.duckdb_target import DuckDbGateway, configure_object_store
from millrace.warehouse.factory import open_warehouse
from millrace.warehouse.gateway import WarehouseGateway

__all__ = [
    "DUCKDB",
    "SNOWFLAKE",
    "Dialect",
    "DuckDbGateway",
    "WarehouseGateway",
    "configure_object_store",
    "open_warehouse",
]
