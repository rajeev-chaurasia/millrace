from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from millrace.contracts import RunContext
from millrace.validation.models import ReconciliationConfig

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CANDIDATE_PLACEHOLDER = "{candidate_schema}"


def quote_identifier(identifier: str) -> str:
    if not _IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"unsafe SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def quote_relation(relation: str) -> str:
    parts = relation.split(".")
    if not parts or any(not part for part in parts):
        raise ValueError(f"invalid SQL relation: {relation!r}")
    return ".".join(quote_identifier(part) for part in parts)


def candidate_schema(context: RunContext) -> str:
    return f"candidate_{context.storage_key.split('/', maxsplit=1)[1]}"


def render_relation(template: str, context: RunContext) -> str:
    rendered = template.replace(_CANDIDATE_PLACEHOLDER, candidate_schema(context))
    return quote_relation(rendered)


def load_reconciliation_config(path: str | Path) -> ReconciliationConfig:
    config_path = Path(path)
    try:
        raw: Any = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot load reconciliation config {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("reconciliation config must be a mapping")
    config = ReconciliationConfig.model_validate(raw)
    _validate_identifiers(config)
    return config


def _validate_identifiers(config: ReconciliationConfig) -> None:
    quote_identifier(config.control_schema)
    for entity in config.entities:
        quote_relation(entity.source.relation)
        _validate_relation_template(entity.target.relation)
        quote_identifier(entity.source.batch_column)
        quote_identifier(entity.target.batch_column)
        if entity.source.sequence_column is not None:
            quote_identifier(entity.source.sequence_column)
        if entity.source.deleted_column is not None:
            quote_identifier(entity.source.deleted_column)
        for identifier in (*entity.key_columns, *(column.name for column in entity.columns)):
            quote_identifier(identifier)
    for publication in config.publications:
        quote_relation(publication.view)
        _validate_relation_template(publication.relation)
        quote_identifier(publication.batch_column)


def _validate_relation_template(template: str) -> None:
    quote_relation(template.replace(_CANDIDATE_PLACEHOLDER, "candidate_schema"))
