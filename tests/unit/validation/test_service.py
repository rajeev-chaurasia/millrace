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


class StaticTombstoneReader:
    def __init__(self, deleted_keys: Sequence[Row]) -> None:
        self._deleted_keys = deleted_keys

    def fetch_deleted_keys(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        del entity, context
        return self._deleted_keys


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


def test_deleted_key_absent_from_target_passes(tmp_path: Path) -> None:
    rows = [{"id": 1, "group": "east", "amount": 10}]
    service = _service(
        tmp_path,
        StaticReader(rows),
        StaticReader(rows),
        CapturingAudit(),
        tombstones=StaticTombstoneReader([{"id": 2}]),
    )

    report, _ = service.validate(_context())

    delete_check = next(check for check in report.checks if check.name == "deleted_absent")
    assert report.status is ValidationStatus.PASSED
    assert delete_check.passed
    assert delete_check.actual == []


def test_deleted_key_still_present_in_target_fails_closed(tmp_path: Path) -> None:
    rows = [{"id": 1, "group": "east", "amount": 10}]
    candidate_rows = [*rows, {"id": 2, "group": "east", "amount": 4}]
    service = _service(
        tmp_path,
        StaticReader(rows),
        StaticReader(candidate_rows),
        CapturingAudit(),
        tombstones=StaticTombstoneReader([{"id": 2}]),
    )

    report, _ = service.validate(_context())

    delete_check = next(check for check in report.checks if check.name == "deleted_absent")
    assert report.status is ValidationStatus.FAILED
    assert not delete_check.passed
    assert delete_check.actual == ["I:2"]


def test_configured_deletes_without_a_tombstone_reader_fail_closed(tmp_path: Path) -> None:
    # An entity that declares deleted_column but has no reader wired cannot
    # verify deletes. Emitting `deleted_absent` as passed there would report
    # a check that examined nothing, so it must surface as a failure instead.
    rows = [{"id": 1, "group": "east", "amount": 10}]
    service = ValidationService(
        config=_config(with_deletes=True),
        source=StaticReader(rows),
        candidate=StaticReader(rows),
        report_sink=CapturingSink(tmp_path / "report.json"),
        audit_writer=CapturingAudit(),
        tombstones=None,
    )

    report, _ = service.validate(_context())

    assert report.status is ValidationStatus.FAILED
    assert not any(check.name == "deleted_absent" for check in report.checks)
    assert "no tombstone reader is available" in (report.checks[0].error or "")


def _service(
    tmp_path: Path,
    source: StaticReader,
    candidate: StaticReader | FailingReader,
    audit: CapturingAudit,
    *,
    tombstones: StaticTombstoneReader | None = None,
) -> ValidationService:
    return ValidationService(
        config=_config(with_deletes=tombstones is not None),
        source=source,
        candidate=candidate,
        report_sink=CapturingSink(tmp_path / "report.json"),
        audit_writer=audit,
        tombstones=tombstones,
    )


def _config(*, with_deletes: bool = False) -> ReconciliationConfig:
    return ReconciliationConfig(
        entities=(
            EntityRule(
                name="sales",
                source=SourceRule(
                    relation="history.sales",
                    batch_column="batch_id",
                    deleted_column="is_deleted" if with_deletes else None,
                ),
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
