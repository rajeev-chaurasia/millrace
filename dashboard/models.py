from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import pandas as pd


@dataclass(frozen=True, slots=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("start date must not be after end date")


@dataclass(frozen=True, slots=True)
class Kpis:
    revenue: float
    orders: int
    customers: int
    average_order_value: float


@dataclass(frozen=True, slots=True)
class Freshness:
    source_updated_at: datetime | None
    pipeline_run_id: str | None
    pipeline_batch_id: int | None


@dataclass(frozen=True, slots=True)
class ValidationStatus:
    run_id: str
    status: str
    validated_at: datetime | None
    published_at: datetime | None
    checks_passed: int
    checks_failed: int


@dataclass(frozen=True, slots=True)
class DashboardData:
    kpis: Kpis
    daily_revenue: pd.DataFrame
    product_performance: pd.DataFrame
    freshness: Freshness
