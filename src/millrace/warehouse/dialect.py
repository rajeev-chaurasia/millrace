from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class IdentifierCase(StrEnum):
    """How a warehouse resolves an unquoted identifier.

    DuckDB and Postgres preserve the case a name was written in. Snowflake folds
    unquoted identifiers to uppercase, so a quoted lowercase name created there
    will not match a table dbt-snowflake built unquoted. Callers targeting
    Snowflake pass UPPER; every other target keeps the PRESERVE default so
    existing behavior is unchanged.

    Defined here rather than in millrace.validation.configuration (its
    original home) because that direction creates a real import cycle:
    validation.publication/audit/readers already depend on
    warehouse.gateway.WarehouseGateway, so warehouse depending back on
    validation.configuration for this one enum made the two packages
    circular, breaking depending on which one a process happened to import
    first.
    """

    PRESERVE = "preserve"
    UPPER = "upper"


@dataclass(frozen=True, slots=True)
class Dialect:
    """Warehouse-specific SQL surface that the rest of the codebase must not hardcode.

    Adding a target means adding one Dialect instance, not scattering another
    round of if-engine checks through readers, audit writers, and publication
    strategies.
    """

    name: str
    identifier_case: IdentifierCase
    parameter_placeholder: str
    timestamp_type: str
    json_type: str
    supports_transactional_ddl: bool


DUCKDB = Dialect(
    name="duckdb",
    identifier_case=IdentifierCase.PRESERVE,
    parameter_placeholder="?",
    timestamp_type="TIMESTAMPTZ",
    json_type="JSON",
    supports_transactional_ddl=True,
)

SNOWFLAKE = Dialect(
    name="snowflake",
    identifier_case=IdentifierCase.UPPER,
    parameter_placeholder="%s",
    timestamp_type="TIMESTAMP_TZ",
    json_type="VARIANT",
    supports_transactional_ddl=False,
)
