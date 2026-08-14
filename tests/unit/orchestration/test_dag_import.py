from __future__ import annotations

import runpy
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

pytest.importorskip("airflow.sdk")


def test_dag_imports_without_side_effect_failures() -> None:
    dag_path = Path(__file__).parents[3] / "airflow" / "dags" / "millrace_pipeline.py"
    module = runpy.run_path(str(dag_path))
    dag_factory = cast(Callable[[], Any], module["millrace_pipeline"])
    dag = dag_factory()

    assert dag.max_active_runs == 1
    assert {task.task_id for task in dag.tasks} == {
        "readiness",
        "capture_cutoff",
        "bounded_spark_submit",
        "dbt_candidate_build",
        "reconciliation",
        "dbt_tests",
        "transactional_promotion",
        "publish_metrics",
        "cleanup",
    }
