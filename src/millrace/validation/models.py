from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CanonicalType(StrEnum):
    TEXT = "text"
    DATE = "date"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    INTEGER = "integer"
    FLOAT = "float"


class AggregateOperation(StrEnum):
    SUM = "sum"
    MIN = "min"
    MAX = "max"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ColumnRule(StrictModel):
    name: str = Field(min_length=1)
    canonical_type: CanonicalType


class SourceRule(StrictModel):
    relation: str = Field(min_length=1)
    batch_column: str = Field(min_length=1)
    sequence_column: str | None = None
    deleted_column: str | None = None


class TargetRule(StrictModel):
    relation: str = Field(min_length=1)
    batch_column: str = Field(min_length=1)


class PartitionRule(StrictModel):
    name: str = Field(min_length=1)
    columns: tuple[str, ...] = Field(min_length=1)


class AggregateRule(StrictModel):
    name: str = Field(min_length=1)
    operation: AggregateOperation
    column: str = Field(min_length=1)
    group_by: tuple[str, ...] = ()


class EntityRule(StrictModel):
    name: str = Field(min_length=1)
    source: SourceRule
    target: TargetRule
    key_columns: tuple[str, ...] = Field(min_length=1)
    columns: tuple[ColumnRule, ...] = Field(min_length=1)
    partitions: tuple[PartitionRule, ...] = ()
    aggregates: tuple[AggregateRule, ...] = ()
    allow_empty: bool = False

    @model_validator(mode="after")
    def validate_references(self) -> Self:
        column_names = [column.name for column in self.columns]
        if len(column_names) != len(set(column_names)):
            raise ValueError(f"entity {self.name!r} has duplicate columns")
        available = set(column_names)
        missing_keys = set(self.key_columns) - available
        if missing_keys:
            raise ValueError(f"entity {self.name!r} has unconfigured keys: {sorted(missing_keys)}")
        for partition in self.partitions:
            missing = set(partition.columns) - available
            if missing:
                raise ValueError(
                    f"partition {partition.name!r} has unconfigured columns: {sorted(missing)}"
                )
        for aggregate in self.aggregates:
            missing = ({aggregate.column} | set(aggregate.group_by)) - available
            if missing:
                raise ValueError(
                    f"aggregate {aggregate.name!r} has unconfigured columns: {sorted(missing)}"
                )
        return self


class PublicationRule(StrictModel):
    view: str = Field(min_length=1)
    relation: str = Field(min_length=1)
    batch_column: str = Field(min_length=1)


class ReconciliationConfig(StrictModel):
    control_schema: str = "control"
    entities: tuple[EntityRule, ...] = Field(min_length=1)
    publications: tuple[PublicationRule, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_names(self) -> Self:
        for label, names in (
            ("entity", [entity.name for entity in self.entities]),
            ("publication", [publication.view for publication in self.publications]),
        ):
            if len(names) != len(set(names)):
                raise ValueError(f"duplicate {label} names are not allowed")
        return self


class CheckType(StrEnum):
    PRESENCE = "presence"
    COUNT = "count"
    PARTITION_COUNT = "partition_count"
    CHECKSUM = "checksum"
    AGGREGATE = "aggregate"
    ERROR = "error"


class CheckResult(StrictModel):
    entity: str
    name: str
    check_type: CheckType
    passed: bool
    expected: Any = None
    actual: Any = None
    error: str | None = None


class ValidationStatus(StrEnum):
    PASSED = "passed"
    FAILED = "failed"


class ValidationReport(StrictModel):
    run_id: str
    batch_id: int
    interval_start: datetime
    interval_end: datetime
    checked_at: datetime
    status: ValidationStatus
    checks: tuple[CheckResult, ...]
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.status is ValidationStatus.PASSED
