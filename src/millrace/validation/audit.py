from __future__ import annotations

# Dynamic identifiers are validated and quoted before use.
# ruff: noqa: S608
import json
from pathlib import Path
from typing import Protocol

import duckdb

from millrace.validation.configuration import quote_identifier
from millrace.validation.models import ValidationReport


class ReportSink(Protocol):
    def write(self, report: ValidationReport) -> Path: ...


class AuditWriter(Protocol):
    def record_validation(self, report: ValidationReport, report_path: Path) -> None: ...


class JsonReportSink:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def write(self, report: ValidationReport) -> Path:
        safe_run_id = "".join(
            character if character.isalnum() or character in {"-", "_"} else "_"
            for character in report.run_id
        )
        destination = self._root / safe_run_id / "reconciliation.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(".json.tmp")
        payload = report.model_dump_json(indent=2)
        temporary.write_text(f"{payload}\n", encoding="utf-8")
        temporary.replace(destination)
        return destination


class DuckDbAuditWriter:
    def __init__(self, connection: duckdb.DuckDBPyConnection, control_schema: str) -> None:
        self._connection = connection
        self._schema = quote_identifier(control_schema)

    def ensure_schema(self) -> None:
        self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.validation_runs (
                run_id VARCHAR PRIMARY KEY,
                batch_id BIGINT NOT NULL,
                interval_start TIMESTAMPTZ NOT NULL,
                interval_end TIMESTAMPTZ NOT NULL,
                checked_at TIMESTAMPTZ NOT NULL,
                status VARCHAR NOT NULL,
                checks_passed INTEGER NOT NULL,
                checks_failed INTEGER NOT NULL,
                report_path VARCHAR NOT NULL,
                report_json JSON NOT NULL
            )
            """
        )

    def record_validation(self, report: ValidationReport, report_path: Path) -> None:
        self.ensure_schema()
        payload = json.dumps(report.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        checks_passed = sum(check.passed for check in report.checks)
        checks_failed = len(report.checks) - checks_passed
        self._connection.execute(
            f"""
            INSERT OR REPLACE INTO {self._schema}.validation_runs
            (run_id, batch_id, interval_start, interval_end, checked_at, status, checks_passed,
             checks_failed, report_path, report_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                report.run_id,
                report.batch_id,
                report.interval_start,
                report.interval_end,
                report.checked_at,
                report.status.value,
                checks_passed,
                checks_failed,
                str(report_path),
                payload,
            ],
        )
