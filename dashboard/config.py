from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class DashboardConfigurationError(ValueError):
    """Raised when dashboard configuration is invalid."""


@dataclass(frozen=True, slots=True)
class DashboardConfig:
    warehouse_path: Path

    @classmethod
    def from_environment(cls) -> DashboardConfig:
        warehouse_path = Path(os.getenv("MILLRACE_DUCKDB_PATH", "data/millrace.duckdb"))
        if not warehouse_path.name:
            raise DashboardConfigurationError("MILLRACE_DUCKDB_PATH must identify a database file")
        return cls(warehouse_path=warehouse_path)
