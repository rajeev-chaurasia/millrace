from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from millrace.contracts import RunContext
from millrace.validation.canonical import Row
from millrace.validation.models import (
    AggregateOperation,
    AggregateRule,
    CanonicalType,
    ColumnRule,
    EntityRule,
    PublicationRule,
    ReconciliationConfig,
    SourceRule,
    TargetRule,
    ValidationReport,
    ValidationStatus,
)
from millrace.validation.service import ValidationService


class StaticReader:
    def __init__(self, rows: Sequence[Row]) -> None:
        self._rows = rows

    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        del entity, context
        return self._rows


class FailingReader:
    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        del entity, context
        raise RuntimeError("injected query failure")


class CapturingSink:
    def __init__(self, destination: Path) -> None:
        self.destination = destination

    def write(self, report: ValidationReport) -> Path:
        del report
        return self.destination


class CapturingAudit:
    def __init__(self) -> None:
        self.report: ValidationReport | None = None

    def record_validation(self, report: ValidationReport, report_path: Path) -> None:
        del report_path
        self.report = report


def test_matching_rows_pass_all_configured_checks(tmp_path: Path) -> None:
    rows = [{"id": 1, "group": "east", "amount": 10}, {"id": 2, "group": "west", "amount": 5}]
    audit = CapturingAudit()
    service = _service(tmp_path, StaticReader(rows), StaticReader(list(reversed(rows))), audit)

    report, _ = service.validate(_context())

    assert report.status is ValidationStatus.PASSED
    assert audit.report == report
    assert all(check.passed for check in report.checks)


def test_mismatch_and_query_error_fail_closed(tmp_path: Path) -> None:
    rows = [{"id": 1, "group": "east", "amount": 10}]
    mismatch, _ = _service(
        tmp_path,
        StaticReader(rows),
        StaticReader([{"id": 1, "group": "east", "amount": 11}]),
        CapturingAudit(),
    ).validate(_context())
    query_error, _ = _service(
        tmp_path,
        StaticReader(rows),
        FailingReader(),
        CapturingAudit(),
    ).validate(_context())

    assert mismatch.status is ValidationStatus.FAILED
    assert query_error.status is ValidationStatus.FAILED
    assert "injected query failure" in (query_error.checks[0].error or "")


def _service(
    tmp_path: Path,
    source: StaticReader,
    candidate: StaticReader | FailingReader,
    audit: CapturingAudit,
) -> ValidationService:
    return ValidationService(
        config=_config(),
        source=source,
        candidate=candidate,
        report_sink=CapturingSink(tmp_path / "report.json"),
        audit_writer=audit,
    )


def _config() -> ReconciliationConfig:
    return ReconciliationConfig(
        entities=(
            EntityRule(
                name="sales",
                source=SourceRule(relation="history.sales", batch_column="batch_id"),
                target=TargetRule(
                    relation="{candidate_schema}.sales",
                    batch_column="batch_id",
                ),
                key_columns=("id",),
                columns=(
                    ColumnRule(name="id", canonical_type=CanonicalType.INTEGER),
                    ColumnRule(name="group", canonical_type=CanonicalType.TEXT),
                    ColumnRule(name="amount", canonical_type=CanonicalType.INTEGER),
                ),
                aggregates=(
                    AggregateRule(
                        name="amount",
                        operation=AggregateOperation.SUM,
                        column="amount",
                        group_by=("group",),
                    ),
                ),
            ),
        ),
        publications=(
            PublicationRule(
                view="analytics.sales",
                relation="{candidate_schema}.sales",
                batch_column="batch_id",
            ),
        ),
    )


def _context() -> RunContext:
    return RunContext(
        run_id="test-run",
        batch_id=7,
        interval_start=datetime(2026, 1, 1, tzinfo=UTC),
        interval_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
