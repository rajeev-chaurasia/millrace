from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from millrace.settings import Settings


def test_postgres_dsn_uses_configured_values() -> None:
    settings = Settings(
        postgres_host="database",
        postgres_port=5433,
        postgres_database="warehouse",
        postgres_user="pipeline",
        postgres_password=SecretStr("secret"),
    )

    assert settings.postgres_dsn == "postgresql://pipeline:secret@database:5433/warehouse"


def test_duckdb_only_by_default_and_needs_no_snowflake_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Settings resolves its relative env_file against the working directory,
    # so these default-behaviour assertions run somewhere without a .env:
    # otherwise they assert about whatever the developer running the suite
    # happens to have configured locally.
    monkeypatch.chdir(tmp_path)
    settings = Settings()

    assert settings.enabled_warehouse_targets == ("duckdb",)


def test_warehouse_targets_are_normalized_for_whitespace_and_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    settings = Settings(warehouse_targets=" DuckDB , duckdb ")

    assert settings.enabled_warehouse_targets == ("duckdb", "duckdb")


def test_snowflake_target_without_credentials_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValidationError, match="snowflake warehouse target requires"):
        Settings(warehouse_targets="snowflake")


def test_snowflake_target_with_credentials_is_accepted() -> None:
    settings = Settings(
        warehouse_targets="duckdb,snowflake",
        snowflake_account="org-account",
        snowflake_user="pipeline",
        snowflake_password=SecretStr("secret"),
        snowflake_warehouse="compute_wh",
        snowflake_database="millrace",
    )

    assert settings.enabled_warehouse_targets == ("duckdb", "snowflake")


def test_unknown_warehouse_target_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValidationError, match="unknown warehouse targets"):
        Settings(warehouse_targets="redshift")
