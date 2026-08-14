from __future__ import annotations

from pathlib import Path

import duckdb
import streamlit as st

from dashboard.config import DashboardConfig, DashboardConfigurationError
from dashboard.models import DashboardData, DateRange, ValidationStatus
from dashboard.repository import (
    AnalyticsRepository,
    ValidationStatusUnavailable,
    connect_read_only,
)


def _repository(warehouse_path: str) -> AnalyticsRepository:
    path = Path(warehouse_path)
    if not path.is_file():
        raise DashboardConfigurationError(f"DuckDB warehouse does not exist: {path}")
    return AnalyticsRepository(connect_read_only(path))


@st.cache_data(ttl=60)
def load_available_date_range(warehouse_path: str) -> DateRange | None:
    repository = _repository(warehouse_path)
    try:
        return repository.available_date_range()
    finally:
        repository.close()


@st.cache_data(ttl=60)
def load_dashboard_data(warehouse_path: str, selected_range: DateRange) -> DashboardData:
    repository = _repository(warehouse_path)
    try:
        return DashboardData(
            kpis=repository.kpis(selected_range),
            daily_revenue=repository.daily_revenue(selected_range),
            product_performance=repository.product_performance(selected_range),
            freshness=repository.freshness(),
        )
    finally:
        repository.close()


@st.cache_data(ttl=30)
def load_validation_status(warehouse_path: str) -> ValidationStatus | None:
    repository = _repository(warehouse_path)
    try:
        return repository.current_validation_status()
    finally:
        repository.close()


def render_validation_status(status: ValidationStatus | None) -> None:
    st.subheader("Validation")
    if status is None:
        st.info("No published validation result is available yet.")
        return

    normalized_status = status.status.lower()
    message = (
        f"Run `{status.run_id}` is **{status.status}**. "
        f"{status.checks_passed} checks passed and {status.checks_failed} failed."
    )
    if normalized_status in {"passed", "published"} and status.checks_failed == 0:
        st.success(message)
    elif normalized_status == "failed" or status.checks_failed > 0:
        st.error(message)
    else:
        st.warning(message)

    detail_columns = st.columns(2)
    detail_columns[0].caption(f"Validated: {status.validated_at or 'Unavailable'}")
    detail_columns[1].caption(f"Published: {status.published_at or 'Unavailable'}")


def render_dashboard(data: DashboardData) -> None:
    metric_columns = st.columns(4)
    metric_columns[0].metric("Revenue", f"${data.kpis.revenue:,.2f}")
    metric_columns[1].metric("Orders", f"{data.kpis.orders:,}")
    metric_columns[2].metric("Customers", f"{data.kpis.customers:,}")
    metric_columns[3].metric("Average order", f"${data.kpis.average_order_value:,.2f}")

    st.subheader("Revenue and orders")
    if data.daily_revenue.empty:
        st.info("No revenue was recorded for the selected dates.")
    else:
        st.line_chart(data.daily_revenue, x="order_date", y="revenue")

    st.subheader("Product performance")
    if data.product_performance.empty:
        st.info("No product sales were recorded for the selected dates.")
    else:
        st.bar_chart(
            data.product_performance,
            x="product_name",
            y="revenue",
            horizontal=True,
        )
        st.dataframe(
            data.product_performance,
            hide_index=True,
            use_container_width=True,
            column_config={
                "revenue": st.column_config.NumberColumn("Revenue", format="$%.2f"),
                "units_sold": st.column_config.NumberColumn("Units", format="%d"),
                "orders": st.column_config.NumberColumn("Orders", format="%d"),
            },
        )

    st.subheader("Data freshness")
    if data.freshness.source_updated_at is None:
        st.warning("Published facts contain no source freshness timestamp.")
        return
    freshness_columns = st.columns(3)
    freshness_columns[0].metric("Latest source update", str(data.freshness.source_updated_at))
    freshness_columns[1].metric("Published run", data.freshness.pipeline_run_id or "Unavailable")
    freshness_columns[2].metric(
        "Batch cutoff",
        str(data.freshness.pipeline_batch_id or "Unavailable"),
    )


def main() -> None:
    st.set_page_config(page_title="Millrace Analytics", page_icon="🏁", layout="wide")
    st.title("Millrace retail analytics")
    st.caption("Validated, published warehouse data")

    try:
        config = DashboardConfig.from_environment()
        warehouse_path = str(config.warehouse_path)
        available_range = load_available_date_range(warehouse_path)
    except (DashboardConfigurationError, duckdb.Error, OSError) as error:
        st.error("The analytics warehouse is unavailable.")
        st.caption(str(error))
        return

    try:
        validation_status = load_validation_status(warehouse_path)
    except ValidationStatusUnavailable as error:
        st.warning(str(error))
    except duckdb.Error as error:
        st.warning("Validation status could not be read.")
        st.caption(str(error))
    else:
        render_validation_status(validation_status)

    if available_range is None:
        st.info("The published order view is available but contains no orders.")
        return

    selected_dates = st.sidebar.date_input(
        "Order dates",
        value=(available_range.start, available_range.end),
        min_value=available_range.start,
        max_value=available_range.end,
    )
    if len(selected_dates) != 2:
        st.info("Select both a start date and an end date.")
        return

    selected_range = DateRange(
        start=selected_dates[0],
        end=selected_dates[1],
    )
    try:
        dashboard_data = load_dashboard_data(warehouse_path, selected_range)
    except (duckdb.Error, OSError, ValueError) as error:
        st.error("Published analytics could not be loaded.")
        st.caption(str(error))
        return
    render_dashboard(dashboard_data)


if __name__ == "__main__":
    main()
