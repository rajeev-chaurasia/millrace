from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

RUNS_TOTAL = Counter(
    "millrace_runs_total",
    "Pipeline runs by final status",
    labelnames=("status",),
)
VALIDATION_CHECKS_TOTAL = Counter(
    "millrace_validation_checks_total",
    "Reconciliation checks by type and status",
    labelnames=("check_type", "status"),
)
RUN_DURATION_SECONDS = Histogram(
    "millrace_run_duration_seconds",
    "Pipeline run duration",
    labelnames=("stage",),
)
SOURCE_TARGET_ROW_DELTA = Gauge(
    "millrace_source_target_row_delta",
    "Source row count minus target row count",
    labelnames=("entity",),
)
PUBLISHED_BATCH_ID = Gauge(
    "millrace_published_batch_id",
    "Most recently published source batch",
)
