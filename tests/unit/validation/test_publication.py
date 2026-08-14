from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from millrace.contracts import RunContext
from millrace.validation.audit import DuckDbAuditWriter
from millrace.validation.configuration import candidate_schema
from millrace.validation.models import (
    CanonicalType,
    CheckResult,
    CheckType,
    ColumnRule,
    EntityRule,
    PublicationRule,
    ReconciliationConfig,
    SourceRule,
    TargetRule,
    ValidationReport,
    ValidationStatus,
)
from millrace.validation.publication import PromotionService


def test_failed_transaction_keeps_old_publication(tmp_path: Path) -> None:
    connection = duckdb.connect(str(tmp_path / "publication.duckdb"))
    context = _context()
    candidate = candidate_schema(context)
    connection.execute("CREATE SCHEMA analytics")
    connection.execute("CREATE SCHEMA old_candidate")
    connection.execute("CREATE TABLE old_candidate.sales (value INTEGER)")
    connection.execute("INSERT INTO old_candidate.sales VALUES (1)")
    connection.execute("CREATE VIEW analytics.sales AS SELECT * FROM old_candidate.sales")
    connection.execute(f'CREATE SCHEMA "{candidate}"')
    connection.execute(f'CREATE TABLE "{candidate}".sales (value INTEGER)')
    connection.execute(f'INSERT INTO "{candidate}".sales VALUES (2)')  # noqa: S608
    report = _report(context)
    DuckDbAuditWriter(connection, "control").record_validation(report, tmp_path / "report.json")

    with pytest.raises(duckdb.Error):
        PromotionService(connection, _config()).promote(context, report)

    assert connection.execute("SELECT value FROM analytics.sales").fetchone() == (1,)
    assert connection.execute("SELECT COUNT(*) FROM control.publication_runs").fetchone() == (0,)
    connection.close()


def _config() -> ReconciliationConfig:
    entity = EntityRule(
        name="sales",
        source=SourceRule(relation="history.sales", batch_column="batch_id"),
        target=TargetRule(relation="{candidate_schema}.sales", batch_column="batch_id"),
        key_columns=("value",),
        columns=(ColumnRule(name="value", canonical_type=CanonicalType.INTEGER),),
    )
    return ReconciliationConfig(
        entities=(entity,),
        publications=(
            PublicationRule(
                view="analytics.sales",
                relation="{candidate_schema}.sales",
                batch_column="value",
            ),
            PublicationRule(
                view="analytics.missing",
                relation="{candidate_schema}.missing",
                batch_column="value",
            ),
        ),
    )


def _context() -> RunContext:
    return RunContext(
        run_id="failure-injection",
        batch_id=9,
        interval_start=datetime(2026, 1, 1, tzinfo=UTC),
        interval_end=datetime(2026, 1, 2, tzinfo=UTC),
    )


def _report(context: RunContext) -> ValidationReport:
    return ValidationReport(
        run_id=context.run_id,
        batch_id=context.batch_id,
        interval_start=context.interval_start,
        interval_end=context.interval_end,
        checked_at=datetime.now(UTC),
        status=ValidationStatus.PASSED,
        checks=(
            CheckResult(
                entity="sales",
                name="row_count",
                check_type=CheckType.COUNT,
                passed=True,
                expected=1,
                actual=1,
            ),
        ),
    )
