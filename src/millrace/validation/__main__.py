from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from millrace.contracts import RunContext
from millrace.settings import get_settings
from millrace.validation.audit import DuckDbAuditWriter, JsonReportSink
from millrace.validation.configuration import load_reconciliation_config
from millrace.validation.models import ValidationReport
from millrace.validation.publication import PromotionService
from millrace.validation.readers import DuckDbCandidateReader, PostgresHistoryReader
from millrace.validation.service import ValidationService
from millrace.warehouse import configure_object_store


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m millrace.validation")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("validate", "promote"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--run-id", required=True)
        subparser.add_argument("--batch-id", required=True, type=int)
        subparser.add_argument("--interval-start", required=True)
        subparser.add_argument("--interval-end", required=True)
        subparser.add_argument("--config")
    subparsers.choices["promote"].add_argument("--report", required=True)
    return parser


def _context(arguments: argparse.Namespace) -> RunContext:
    return RunContext.from_iso(
        run_id=str(arguments.run_id),
        batch_id=int(arguments.batch_id),
        interval_start=str(arguments.interval_start),
        interval_end=str(arguments.interval_end),
    )


def main() -> int:
    arguments = _parser().parse_args()
    settings = get_settings()
    context = _context(arguments)
    config_path = str(arguments.config or settings.reconciliation_config)
    config = load_reconciliation_config(config_path)
    connection = duckdb.connect(settings.duckdb_path)
    try:
        configure_object_store(connection, settings)
        if arguments.command == "validate":
            service = ValidationService(
                config=config,
                source=PostgresHistoryReader(settings.postgres_dsn),
                candidate=DuckDbCandidateReader(connection),
                report_sink=JsonReportSink(settings.reports_directory),
                audit_writer=DuckDbAuditWriter(connection, config.control_schema),
            )
            _, report_path = service.validate_or_raise(context)
            print(report_path)
        else:
            report_path = Path(str(arguments.report))
            report = ValidationReport.model_validate_json(report_path.read_text(encoding="utf-8"))
            PromotionService(connection, config).promote(context, report)
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
