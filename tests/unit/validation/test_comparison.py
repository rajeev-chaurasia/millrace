from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from millrace.contracts import RunContext
from millrace.validation.canonical import Row
from millrace.validation.comparison import (
    CachingSnapshotReader,
    CachingTombstoneReader,
    compare_reports,
)
from millrace.validation.models import (
    CanonicalType,
    CheckResult,
    CheckType,
    ColumnRule,
    EntityRule,
    SourceRule,
    TargetRule,
    ValidationReport,
    ValidationStatus,
)


def test_identical_reports_across_targets_agree() -> None:
    report = _report(
        checks=(_check("row_count", passed=True, expected=2, actual=2),),
    )

    result = compare_reports({"duckdb": report, "snowflake": report})

    assert result["passed"] is True
    assert result["disagreements"] == []


def test_differing_check_results_are_reported_as_a_disagreement() -> None:
    duckdb_report = _report(
        checks=(_check("checksum:email", passed=True, expected="abc", actual="abc"),),
    )
    snowflake_report = _report(
        checks=(_check("checksum:email", passed=False, expected="abc", actual="xyz"),),
    )

    result = compare_reports({"duckdb": duckdb_report, "snowflake": snowflake_report})

    assert result["passed"] is False
    assert len(result["disagreements"]) == 1
    assert result["disagreements"][0]["check"] == "checksum:email"


def test_identical_dict_valued_checks_agree() -> None:
    # partition_count and aggregate checks carry a dict for expected/actual
    # (see canonical.py's grouped_counts/aggregate_values), which is
    # unhashable and previously broke the set()-based comparison.
    report = _report(
        checks=(
            _check(
                "aggregate:price_sum_by_product",
                passed=True,
                expected={"I:2001": "M:39.9", "I:2002": "M:46.5"},
                actual={"I:2001": "M:39.9", "I:2002": "M:46.5"},
            ),
        ),
    )

    result = compare_reports({"duckdb": report, "snowflake": report})

    assert result["passed"] is True
    assert result["disagreements"] == []


def test_differing_dict_valued_checks_are_reported_as_a_disagreement() -> None:
    duckdb_report = _report(
        checks=(
            _check(
                "partition_count:category",
                passed=True,
                expected={"S:tools": 2},
                actual={"S:tools": 2},
            ),
        ),
    )
    snowflake_report = _report(
        checks=(
            _check(
                "partition_count:category",
                passed=False,
                expected={"S:tools": 2},
                actual={"S:tools": 1},
            ),
        ),
    )

    result = compare_reports({"duckdb": duckdb_report, "snowflake": snowflake_report})

    assert result["passed"] is False
    assert result["disagreements"][0]["check"] == "partition_count:category"


def test_a_check_present_on_only_one_target_is_a_disagreement() -> None:
    duckdb_report = _report(
        checks=(_check("row_count", passed=True, expected=1, actual=1),),
    )
    snowflake_report = _report(checks=())

    result = compare_reports({"duckdb": duckdb_report, "snowflake": snowflake_report})

    assert result["passed"] is False
    assert result["disagreements"][0]["reason"].startswith("missing on")


def test_a_target_that_failed_its_own_validation_fails_the_comparison_even_if_checks_agree() -> (
    None
):
    failed_report = _report(
        checks=(_check("row_count", passed=False, expected=2, actual=1),),
        status=ValidationStatus.FAILED,
    )

    result = compare_reports({"duckdb": failed_report, "snowflake": failed_report})

    assert result["passed"] is False


def test_caching_snapshot_reader_fetches_each_entity_batch_once() -> None:
    calls: list[int] = []

    class CountingReader:
        def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
            del entity
            calls.append(context.batch_id)
            return [{"id": 1}]

    reader = CachingSnapshotReader(CountingReader())
    entity = _entity()
    context = _context()

    first = reader.fetch_rows(entity, context)
    second = reader.fetch_rows(entity, context)

    assert first is second
    assert calls == [7]


def test_caching_tombstone_reader_fetches_each_entity_batch_once() -> None:
    calls: list[int] = []

    class CountingTombstoneReader:
        def fetch_deleted_keys(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
            del entity
            calls.append(context.batch_id)
            return [{"id": 9}]

    reader = CachingTombstoneReader(CountingTombstoneReader())
    entity = _entity()
    context = _context()

    first = reader.fetch_deleted_keys(entity, context)
    second = reader.fetch_deleted_keys(entity, context)

    assert first is second
    assert calls == [7]


def _entity() -> EntityRule:
    return EntityRule(
        name="customers",
        source=SourceRule(relation="history.customers", batch_column="batch_id"),
        target=TargetRule(relation="{candidate_schema}.customers", batch_column="batch_id"),
        key_columns=("id",),
        columns=(ColumnRule(name="id", canonical_type=CanonicalType.INTEGER),),
    )


def _check(name: str, *, passed: bool, expected: object, actual: object) -> CheckResult:
    return CheckResult(
        entity="customers",
        name=name,
        check_type=CheckType.CHECKSUM,
        passed=passed,
        expected=expected,
        actual=actual,
    )


def _report(
    *,
    checks: tuple[CheckResult, ...],
    status: ValidationStatus = ValidationStatus.PASSED,
) -> ValidationReport:
    return ValidationReport(
        run_id="run-1",
        batch_id=7,
        interval_start=datetime(2026, 1, 1, tzinfo=UTC),
        interval_end=datetime(2026, 1, 2, tzinfo=UTC),
        checked_at=datetime(2026, 1, 2, tzinfo=UTC),
        status=status,
        checks=checks,
    )


def _context() -> RunContext:
    return RunContext(
        run_id="run-1",
        batch_id=7,
        interval_start=datetime(2026, 1, 1, tzinfo=UTC),
        interval_end=datetime(2026, 1, 2, tzinfo=UTC),
    )
