from __future__ import annotations

from functools import lru_cache
from typing import Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_KNOWN_WAREHOUSE_TARGETS = frozenset({"duckdb", "snowflake"})


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="MILLRACE_",
        case_sensitive=False,
        extra="ignore",
    )

    environment: str = "local"
    log_level: str = "INFO"

    postgres_host: str = "localhost"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_database: str = "millrace"
    postgres_user: str = "millrace"
    postgres_password: SecretStr = SecretStr("millrace")

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_prefix: str = "millrace"

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "millrace"
    s3_secret_key: SecretStr = SecretStr("millrace-local")
    s3_bucket: str = "millrace"
    s3_region: str = "us-east-1"

    duckdb_path: str = "data/millrace.duckdb"
    reconciliation_config: str = "config/reconciliation.yml"
    reports_directory: str = "artifacts/reconciliation"
    metrics_port: int = Field(default=9108, ge=1, le=65535)

    # Comma-separated warehouse targets to operate against, e.g. "duckdb" or
    # "duckdb,snowflake". DuckDB stays the default so every existing deployment,
    # test, and CI job is unaffected.
    warehouse_targets: str = "duckdb"

    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: SecretStr = SecretStr("")
    snowflake_role: str = ""
    snowflake_warehouse: str = ""
    snowflake_database: str = ""
    # Empty means plain password auth. Set to PROGRAMMATIC_ACCESS_TOKEN (with
    # snowflake_password holding the token) when the account enforces MFA,
    # since plain password auth is rejected for programmatic connections
    # there. Any other value is passed through as-is to the connector's
    # `authenticator` argument, alongside the password. Ignored when
    # snowflake_private_key_path is set: key-pair auth takes priority and
    # needs no authenticator value.
    snowflake_authenticator: str = ""
    # RSA key-pair auth, Snowflake's recommended method for service accounts
    # and the only one this codebase can use uniformly for both the direct
    # Python connector and dbt-snowflake: dbt-snowflake (as pinned here) has
    # no PAT support, but does support a key file unconditionally. When set,
    # this takes priority over snowflake_password entirely.
    snowflake_private_key_path: str = ""
    snowflake_private_key_passphrase: SecretStr = SecretStr("")

    @property
    def postgres_dsn(self) -> str:
        password = self.postgres_password.get_secret_value()
        return (
            f"postgresql://{self.postgres_user}:{password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )

    @property
    def enabled_warehouse_targets(self) -> tuple[str, ...]:
        return tuple(
            target
            for target in (item.strip().lower() for item in self.warehouse_targets.split(","))
            if target
        )

    @model_validator(mode="after")
    def _validate_warehouse_targets(self) -> Self:
        unknown = set(self.enabled_warehouse_targets) - _KNOWN_WAREHOUSE_TARGETS
        if unknown:
            raise ValueError(f"unknown warehouse targets: {sorted(unknown)}")
        if "snowflake" in self.enabled_warehouse_targets:
            missing = [
                name
                for name, value in (
                    ("snowflake_account", self.snowflake_account),
                    ("snowflake_user", self.snowflake_user),
                    ("snowflake_warehouse", self.snowflake_warehouse),
                    ("snowflake_database", self.snowflake_database),
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    "snowflake warehouse target requires: " + ", ".join(sorted(missing))
                )
            if (
                not self.snowflake_private_key_path
                and not self.snowflake_password.get_secret_value()
            ):
                raise ValueError(
                    "snowflake warehouse target requires either snowflake_password "
                    "or snowflake_private_key_path"
                )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
