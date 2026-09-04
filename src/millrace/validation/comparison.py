from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from millrace.contracts import RunContext
from millrace.validation.canonical import Row
from millrace.validation.models import CheckResult, EntityRule, ValidationReport
from millrace.validation.readers import SnapshotReader, TombstoneReader


class CachingSnapshotReader:
    """Wraps a `SnapshotReader` so every warehouse target validated in one
    `compare` run reads the exact same source rows for a given entity and
    batch. Without this, a cross-engine disagreement could not be trusted to
    be warehouse-side: it could equally be two separate Postgres reads racing
    a concurrent write.
    """

    def __init__(self, reader: SnapshotReader) -> None:
        self._reader = reader
        self._cache: dict[tuple[str, int], Sequence[Row]] = {}

    def fetch_rows(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        key = (entity.name, context.batch_id)
        if key not in self._cache:
            self._cache[key] = self._reader.fetch_rows(entity, context)
        return self._cache[key]


class CachingTombstoneReader:
    """Same guarantee as `CachingSnapshotReader`, for the deletes check."""

    def __init__(self, reader: TombstoneReader) -> None:
        self._reader = reader
        self._cache: dict[tuple[str, int], Sequence[Row]] = {}

    def fetch_deleted_keys(self, entity: EntityRule, context: RunContext) -> Sequence[Row]:
        key = (entity.name, context.batch_id)
        if key not in self._cache:
            self._cache[key] = self._reader.fetch_deleted_keys(entity, context)
        return self._cache[key]


def compare_reports(reports: Mapping[str, ValidationReport]) -> dict[str, Any]:
    """Builds the cross-engine agreement payload written to cross_engine.json.

    Every `expected`/`actual` value on a `CheckResult` is already a canonical
    string, an int, or a dict of canonical strings (see canonical.py), so
    comparing them across targets is exact equality, not a fuzzy diff.
    """
    targets = tuple(sorted(reports))
    by_check: dict[tuple[str, str], dict[str, CheckResult]] = {}
    for target, report in reports.items():
        for check in report.checks:
            by_check.setdefault((check.entity, check.name), {})[target] = check

    disagreements: list[dict[str, Any]] = []
    for (entity, name), per_target in sorted(by_check.items()):
        if set(per_target) != set(targets):
            disagreements.append(
                {
                    "entity": entity,
                    "check": name,
                    "reason": "missing on: " + ", ".join(sorted(set(targets) - set(per_target))),
                }
            )
            continue
        signatures = [(check.passed, check.expected, check.actual) for check in per_target.values()]
        # Not a set: aggregate and partition_count checks carry a dict for
        # expected/actual (see canonical.py's grouped_counts/aggregate_values),
        # which is unhashable. Direct comparison against the first signature
        # is equivalent for equality purposes and works for every check type.
        if any(signature != signatures[0] for signature in signatures[1:]):
            disagreements.append(
                {
                    "entity": entity,
                    "check": name,
                    "reason": "results differ across targets",
                    "results": {
                        target: {
                            "passed": check.passed,
                            "expected": check.expected,
                            "actual": check.actual,
                        }
                        for target, check in per_target.items()
                    },
                }
            )

    all_targets_passed = all(report.passed for report in reports.values())
    passed = all_targets_passed and not disagreements
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "targets": list(targets),
        "passed": passed,
        "target_reports": {
            target: {
                "status": report.status.value,
                "checks_passed": sum(check.passed for check in report.checks),
                "checks_failed": sum(not check.passed for check in report.checks),
            }
            for target, report in reports.items()
        },
        "disagreements": disagreements,
    }
