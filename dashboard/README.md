# Millrace dashboard

Run from the repository root after a validated candidate has been published:

```shell
streamlit run dashboard/app.py
```

`MILLRACE_DUCKDB_PATH` selects the DuckDB warehouse. The dashboard opens it read-only and issues
only fixed, parameterized queries against these stable views:

- `analytics.dim_product`
- `analytics.fact_order`
- `analytics.fact_order_item`
- `analytics.current_validation_status`

The publication layer must expose `analytics.current_validation_status` with one current row:
`run_id`, `status`, `validated_at`, `published_at`, `checks_passed`, and `checks_failed`. If that
optional operational view is absent, business metrics remain available and the dashboard displays
a warning.
