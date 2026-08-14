from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any, Protocol, cast

import duckdb
import pandas as pd

from dashboard.models import DateRange, Freshness, Kpis, ValidationStatus

_DATE_RANGE_SQL = """
select min(cast(ordered_at as date)), max(cast(ordered_at as date))
from analytics.fact_order
"""

_KPI_SQL = """
select
    coalesce(sum(order_total), 0) as revenue,
    count(*) as orders,
    count(distinct customer_id) as customers,
    coalesce(avg(order_total), 0) as average_order_value
from analytics.fact_order
where ordered_at >= ?
  and ordered_at < ?
  and order_status not in ('cancelled', 'refunded')
"""

_DAILY_REVENUE_SQL = """
select
    cast(ordered_at as date) as order_date,
    sum(order_total) as revenue,
    count(*) as orders
from analytics.fact_order
where ordered_at >= ?
  and ordered_at < ?
  and order_status not in ('cancelled', 'refunded')
group by cast(ordered_at as date)
order by order_date
"""

_PRODUCT_PERFORMANCE_SQL = """
select
    products.product_id,
    products.sku,
    products.product_name,
    products.category,
    sum(items.quantity) as units_sold,
    sum(items.line_amount) as revenue,
    count(distinct items.order_id) as orders
from analytics.fact_order_item as items
inner join analytics.fact_order as orders
    on items.order_id = orders.order_id
inner join analytics.dim_product as products
    on items.product_id = products.product_id
where orders.ordered_at >= ?
  and orders.ordered_at < ?
  and orders.order_status not in ('cancelled', 'refunded')
group by
    products.product_id,
    products.sku,
    products.product_name,
    products.category
order by revenue desc, products.product_id
limit ?
"""

_FRESHNESS_SQL = """
select
    max(source_updated_at) as source_updated_at,
    max(pipeline_run_id) as pipeline_run_id,
    max(pipeline_batch_id) as pipeline_batch_id
from (
    select source_updated_at, pipeline_run_id, pipeline_batch_id
    from analytics.fact_order
    union all
    select source_updated_at, pipeline_run_id, pipeline_batch_id
    from analytics.fact_order_item
) as published_facts
"""

_VALIDATION_STATUS_SQL = """
select
    run_id,
    status,
    validated_at,
    published_at,
    checks_passed,
    checks_failed
from analytics.current_validation_status
limit 1
"""


class QueryResult(Protocol):
    def fetchone(self) -> tuple[Any, ...] | None: ...

    def df(self) -> pd.DataFrame: ...


class QueryConnection(Protocol):
    def execute(
        self,
        query: str,
        parameters: object | None = None,
    ) -> QueryResult: ...

    def close(self) -> None: ...


class ValidationStatusUnavailable(RuntimeError):
    """Raised when publication has not exposed validation status."""


def connect_read_only(warehouse_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(warehouse_path), read_only=True)


class AnalyticsRepository:
    def __init__(self, connection: QueryConnection) -> None:
        self._connection = connection

    def close(self) -> None:
        self._connection.close()

    def available_date_range(self) -> DateRange | None:
        row = self._connection.execute(_DATE_RANGE_SQL).fetchone()
        if row is None or row[0] is None or row[1] is None:
            return None
        return DateRange(start=cast(date, row[0]), end=cast(date, row[1]))

    def kpis(self, date_range: DateRange) -> Kpis:
        row = self._connection.execute(
            _KPI_SQL,
            self._date_parameters(date_range),
        ).fetchone()
        if row is None:
            return Kpis(revenue=0.0, orders=0, customers=0, average_order_value=0.0)
        return Kpis(
            revenue=float(row[0]),
            orders=int(row[1]),
            customers=int(row[2]),
            average_order_value=float(row[3]),
        )

    def daily_revenue(self, date_range: DateRange) -> pd.DataFrame:
        return self._connection.execute(
            _DAILY_REVENUE_SQL,
            self._date_parameters(date_range),
        ).df()

    def product_performance(
        self,
        date_range: DateRange,
        *,
        limit: int = 20,
    ) -> pd.DataFrame:
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")
        parameters: tuple[object, ...] = (*self._date_parameters(date_range), limit)
        return self._connection.execute(_PRODUCT_PERFORMANCE_SQL, parameters).df()

    def freshness(self) -> Freshness:
        row = self._connection.execute(_FRESHNESS_SQL).fetchone()
        if row is None:
            return Freshness(
                source_updated_at=None,
                pipeline_run_id=None,
                pipeline_batch_id=None,
            )
        return Freshness(
            source_updated_at=row[0],
            pipeline_run_id=None if row[1] is None else str(row[1]),
            pipeline_batch_id=None if row[2] is None else int(row[2]),
        )

    def current_validation_status(self) -> ValidationStatus | None:
        try:
            row = self._connection.execute(_VALIDATION_STATUS_SQL).fetchone()
        except duckdb.CatalogException as error:
            raise ValidationStatusUnavailable(
                "analytics.current_validation_status is not published"
            ) from error
        if row is None:
            return None
        return ValidationStatus(
            run_id=str(row[0]),
            status=str(row[1]),
            validated_at=row[2],
            published_at=row[3],
            checks_passed=int(row[4]),
            checks_failed=int(row[5]),
        )

    @staticmethod
    def _date_parameters(date_range: DateRange) -> tuple[date, date]:
        return date_range.start, date_range.end + timedelta(days=1)
