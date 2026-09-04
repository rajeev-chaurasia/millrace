from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from pathlib import Path

import duckdb

from millrace.contracts import RunContext
from millrace.settings import Settings, get_settings
from millrace.validation.audit import DuckDbAuditWriter, JsonReportSink, SnowflakeAuditWriter
from millrace.validation.comparison import (
    CachingSnapshotReader,
    CachingTombstoneReader,
    compare_reports,
)
from millrace.validation.configuration import load_reconciliation_config
from millrace.validation.models import ReconciliationConfig, ValidationReport
from millrace.validation.publication import (
    Promoter,
    PromotionService,
    SnowflakePromotionService,
)
from millrace.validation.readers import (
    DuckDbCandidateReader,
    PostgresHistoryReader,
    PostgresTombstoneReader,
    SnowflakeCandidateReader,
)
from millrace.validation.service import ValidationService
from millrace.warehouse import configure_object_store, open_warehouse


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m millrace.validation")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "promote"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--warehouse", default="duckdb", choices=("duckdb", "snowflake"))
        subparser.add_argument("--run-id", required=True)
        subparser.add_argument("--batch-id", required=True, type=int)
        subparser.add_argument("--interval-start", required=True)
        subparser.add_argument("--interval-end", required=True)
        subparser.add_argument("--config")
    subparsers.choices["promote"].add_argument("--report", required=True)

    compare = subparsers.add_parser("compare")
    compare.add_argument("--targets", default="duckdb,snowflake")
    compare.add_argument("--run-id", required=True)
    compare.add_argument("--batch-id", required=True, type=int)
    compare.add_argument("--interval-start", required=True)
    compare.add_argument("--interval-end", required=True)
    compare.add_argument("--config")
    return parser


def _context(arguments: argparse.Namespace) -> RunContext:
    return RunContext.from_iso(
        run_id=str(arguments.run_id),
        batch_id=int(arguments.batch_id),
        interval_start=str(arguments.interval_start),
        interval_end=str(arguments.interval_end),
    )


class _DiscardingAuditWriter:
    """Audit writer for diagnostic runs, which must not touch the shared
    `control.validation_runs` row.

    That row is what promotion's Gate 2 reads to decide a candidate was
    validated. A `compare` run validates every target in turn against the
    same run_id, so a recording writer would leave the row reflecting
    whichever target happened to run last, and would overwrite the row a
    real gated run wrote.
    """

    def record_validation(self, report: ValidationReport, report_path: Path) -> None:
        del report, report_path


def _validation_service(
    warehouse: str,
    *,
    settings: Settings,
    config: ReconciliationConfig,
    source: PostgresHistoryReader | CachingSnapshotReader,
    tombstones: PostgresTombstoneReader | CachingTombstoneReader,
    diagnostic: bool = False,
) -> tuple[ValidationService, Promoter, Callable[[], None]]:
    """Composition root for one warehouse target: wires the matching reader,
    audit writer, and promotion gate, and returns a close callback the caller
    must invoke when done with the connection.

    `diagnostic` wires a discarding audit writer and a per-target report
    directory, so a `compare` run leaves the gated run's audit row and
    reconciliation.json untouched.
    """
    audit_root = (
        Path(settings.reports_directory) / "cross_engine" / warehouse
        if diagnostic
        else Path(settings.reports_directory)
    )
    if warehouse == "duckdb":
        connection = duckdb.connect(settings.duckdb_path)
        configure_object_store(connection, settings)
        service = ValidationService(
            config=config,
            source=source,
            candidate=DuckDbCandidateReader(connection),
            report_sink=JsonReportSink(audit_root),
            audit_writer=(
                _DiscardingAuditWriter()
                if diagnostic
                else DuckDbAuditWriter(connection, config.control_schema)
            ),
            tombstones=tombstones,
        )
        return service, PromotionService(connection, config), connection.close
    if warehouse == "snowflake":
        gateway = open_warehouse("snowflake", settings)
        service = ValidationService(
            config=config,
            source=source,
            candidate=SnowflakeCandidateReader(gateway),
            report_sink=JsonReportSink(audit_root),
            audit_writer=(
                _DiscardingAuditWriter()
                if diagnostic
                else SnowflakeAuditWriter(gateway, config.control_schema)
            ),
            tombstones=tombstones,
        )
        return service, SnowflakePromotionService(gateway, config), gateway.close
    raise ValueError(f"unknown warehouse target: {warehouse!r}")


def _run_validate(arguments: argparse.Namespace, settings: Settings, context: RunContext) -> int:
    config = load_reconciliation_config(str(arguments.config or settings.reconciliation_config))
    warehouse = str(arguments.warehouse)
    source = PostgresHistoryReader(settings.postgres_dsn)
    tombstones = PostgresTombstoneReader(settings.postgres_dsn)
    service, _, close = _validation_service(
        warehouse, settings=settings, config=config, source=source, tombstones=tombstones
    )
    try:
        _, report_path = service.validate_or_raise(context)
        print(report_path)
    finally:
        close()
    return 0


def _run_promote(arguments: argparse.Namespace, settings: Settings, context: RunContext) -> int:
    config = load_reconciliation_config(str(arguments.config or settings.reconciliation_config))
    warehouse = str(arguments.warehouse)
    report_path = Path(str(arguments.report))
    report = ValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    source = PostgresHistoryReader(settings.postgres_dsn)
    tombstones = PostgresTombstoneReader(settings.postgres_dsn)
    _, promoter, close = _validation_service(
        warehouse, settings=settings, config=config, source=source, tombstones=tombstones
    )
    try:
        promoter.promote(context, report)
    finally:
        close()
    return 0


def _run_compare(arguments: argparse.Namespace, settings: Settings, context: RunContext) -> int:
    config = load_reconciliation_config(str(arguments.config or settings.reconciliation_config))
    targets = [target.strip() for target in str(arguments.targets).split(",") if target.strip()]
    source = CachingSnapshotReader(PostgresHistoryReader(settings.postgres_dsn))
    tombstones = CachingTombstoneReader(PostgresTombstoneReader(settings.postgres_dsn))

    reports: dict[str, ValidationReport] = {}
    closers: list[Callable[[], None]] = []
    try:
        for target in targets:
            service, _, close = _validation_service(
                target,
                settings=settings,
                config=config,
                source=source,
                tombstones=tombstones,
                diagnostic=True,
            )
            closers.append(close)
            report, _ = service.validate(context)
            reports[target] = report
    finally:
        for close in closers:
            close()

    comparison = compare_reports(reports)
    destination = Path(settings.reports_directory) / context.run_id / "cross_engine.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(comparison, indent=2, default=str) + "\n", encoding="utf-8")
    print(destination)
    return 0 if comparison["passed"] else 1


def main() -> int:
    arguments = _parser().parse_args()
    settings = get_settings()
    context = _context(arguments)
    if arguments.command == "validate":
        return _run_validate(arguments, settings, context)
    if arguments.command == "promote":
        return _run_promote(arguments, settings, context)
    return _run_compare(arguments, settings, context)


if __name__ == "__main__":
    raise SystemExit(main())
