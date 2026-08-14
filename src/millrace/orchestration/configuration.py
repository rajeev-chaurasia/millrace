from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from millrace.validation.configuration import quote_identifier, quote_relation


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CutoffConfig(StrictModel):
    relation: str
    batch_column: str
    event_timestamp_column: str


class SparkConfig(StrictModel):
    executable: str = "spark-submit"
    master: str
    application: str
    packages: tuple[str, ...] = ()
    conf: dict[str, str] = {}
    timeout_seconds: int = Field(default=1800, ge=1, le=14400)
    bronze_uri: str


class DbtConfig(StrictModel):
    executable: str = "dbt"
    project_dir: str
    profiles_dir: str
    target: str
    selector: str
    timeout_seconds: int = Field(default=1200, ge=1, le=7200)


class OrchestrationConfig(StrictModel):
    cutoff: CutoffConfig
    spark: SparkConfig
    dbt: DbtConfig
    readiness_timeout_seconds: int = Field(default=120, ge=1, le=1800)
    temporary_directory: str = "artifacts/tmp"


def load_orchestration_config(path: str | Path) -> OrchestrationConfig:
    config_path = Path(path)
    try:
        raw: Any = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot load orchestration config {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("orchestration config must be a mapping")
    config = OrchestrationConfig.model_validate(raw)
    quote_relation(config.cutoff.relation)
    quote_identifier(config.cutoff.batch_column)
    quote_identifier(config.cutoff.event_timestamp_column)
    return config
