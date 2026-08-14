from pydantic import SecretStr

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
