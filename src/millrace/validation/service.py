from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from millrace.contracts import RunContext
from millrace.metrics import SOURCE_TARGET_ROW_DELTA, VALIDATION_CHECKS_TOTAL
from millrace.validation.audit import AuditWriter, ReportSink
from millrace.validation.canonical import (
    Row,
    aggregate_values,
    column_checksum,
    grouped_counts,
)
from millrace.validation.models import (
    CheckResult,
    CheckType,
    ColumnRule,
    EntityRule,
    ReconciliationConfig,
    ValidationReport,
    ValidationStatus,
)
from millrace.validation.readers import SnapshotReader


class ValidationFailedError(RuntimeError):
    def __init__(self, report: ValidationReport, report_path: Path) -> None:
        super().__init__(f"reconciliation failed; report written to {report_path}")
        self.report = report
        self.report_path = report_path


class ValidationService:
    def __init__(
        self,
        *,
        config: ReconciliationConfig,
        source: SnapshotReader,
        candidate: SnapshotReader,
        report_sink: ReportSink,
        audit_writer: AuditWriter,
    ) -> None:
        self._config = config
        self._source = source
        self._candidate = candidate
        self._report_sink = report_sink
        self._audit_writer = audit_writer

    def validate(self, context: RunContext) -> tuple[ValidationReport, Path]:
        checks: list[CheckResult] = []
        for entity in self._config.entities:
            try:
                source_rows = self._source.fetch_rows(entity, context)
                candidate_rows = self._candidate.fetch_rows(entity, context)
                checks.extend(self._compare_entity(entity, source_rows, candidate_rows))
            except Exception as exc:
                checks.append(
                    CheckResult(
                        entity=entity.name,
                        name="query",
                        check_type=CheckType.ERROR,
                        passed=False,
                        error=f"{type(exc).__name__}: {exc}",
                    )
                )

        passed = bool(checks) and all(check.passed for check in checks)
        for check in checks:
            VALIDATION_CHECKS_TOTAL.labels(
                check_type=check.check_type.value,
                status="passed" if check.passed else "failed",
            ).inc()
            if (
                check.check_type is CheckType.COUNT
                and isinstance(check.expected, int)
                and isinstance(check.actual, int)
            ):
                SOURCE_TARGET_ROW_DELTA.labels(entity=check.entity).set(
                    check.expected - check.actual
                )
        report = ValidationReport(
            run_id=context.run_id,
            batch_id=context.batch_id,
            interval_start=context.interval_start,
            interval_end=context.interval_end,
            checked_at=datetime.now(UTC),
            status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
            checks=tuple(checks),
        )
        report_path = self._report_sink.write(report)
        self._audit_writer.record_validation(report, report_path)
        return report, report_path

    def validate_or_raise(self, context: RunContext) -> tuple[ValidationReport, Path]:
        report, report_path = self.validate(context)
        if not report.passed:
            raise ValidationFailedError(report, report_path)
        return report, report_path

    def _compare_entity(
        self,
        entity: EntityRule,
        source_rows: Sequence[Row],
        candidate_rows: Sequence[Row],
    ) -> list[CheckResult]:
        checks = [
            CheckResult(
                entity=entity.name,
                name="non_empty",
                check_type=CheckType.PRESENCE,
                passed=entity.allow_empty or (bool(source_rows) and bool(candidate_rows)),
                expected="rows present" if not entity.allow_empty else "empty allowed",
                actual={
                    "source_rows": len(source_rows),
                    "candidate_rows": len(candidate_rows),
                },
            ),
            self._result(
                entity=entity.name,
                name="row_count",
                check_type=CheckType.COUNT,
                expected=len(source_rows),
                actual=len(candidate_rows),
            ),
        ]
        columns = {column.name: column for column in entity.columns}
        key_columns = [columns[name] for name in entity.key_columns]
        checks.extend(self._partition_checks(entity, source_rows, candidate_rows, columns))
        for column in entity.columns:
            checks.append(
                self._result(
                    entity=entity.name,
                    name=f"checksum:{column.name}",
                    check_type=CheckType.CHECKSUM,
                    expected=column_checksum(
                        source_rows,
                        column=column,
                        key_columns=key_columns,
                    ),
                    actual=column_checksum(
                        candidate_rows,
                        column=column,
                        key_columns=key_columns,
                    ),
                )
            )
        for aggregate in entity.aggregates:
            checks.append(
                self._result(
                    entity=entity.name,
                    name=f"aggregate:{aggregate.name}",
                    check_type=CheckType.AGGREGATE,
                    expected=aggregate_values(source_rows, aggregate, columns),
                    actual=aggregate_values(candidate_rows, aggregate, columns),
                )
            )
        return checks

    @staticmethod
    def _partition_checks(
        entity: EntityRule,
        source_rows: Sequence[Row],
        candidate_rows: Sequence[Row],
        columns: Mapping[str, ColumnRule],
    ) -> list[CheckResult]:
        return [
            ValidationService._result(
                entity=entity.name,
                name=f"partition_count:{partition.name}",
                check_type=CheckType.PARTITION_COUNT,
                expected=grouped_counts(source_rows, [columns[name] for name in partition.columns]),
                actual=grouped_counts(
                    candidate_rows,
                    [columns[name] for name in partition.columns],
                ),
            )
            for partition in entity.partitions
        ]

    @staticmethod
    def _result(
        *,
        entity: str,
        name: str,
        check_type: CheckType,
        expected: object,
        actual: object,
    ) -> CheckResult:
        return CheckResult(
            entity=entity,
            name=name,
            check_type=check_type,
            passed=expected == actual,
            expected=expected,
            actual=actual,
        )
