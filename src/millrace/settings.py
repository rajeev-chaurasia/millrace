from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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

    @property
    def postgres_dsn(self) -> str:
        password = self.postgres_password.get_secret_value()
        return (
            f"postgresql://{self.postgres_user}:{password}@"
            f"{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
