from __future__ import annotations

# Dynamic identifiers are validated and quoted before use.
import logging
import shutil
import time
from collections.abc import Sequence
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Protocol, cast

import boto3
import duckdb
from botocore.config import Config as BotoConfig

if TYPE_CHECKING:
    import snowflake.connector

# `snowflake-connector-python` is an optional extra (`pip install millrace[snowflake]`).
# Importing it lazily, only where a connection is actually opened, keeps every
# DuckDB-only deployment and test free of the dependency: importing
# `millrace.warehouse` must not require a package that a DuckDB-only install
# never needs.

from millrace.contracts import RunContext
from millrace.settings import Settings
from millrace.streaming.models import Entity
from millrace.streaming.schemas import ENTITY_SPECS, ValueKind
from millrace.streaming.storage import StoragePaths
from millrace.validation.configuration import IdentifierCase, quote_identifier
from millrace.warehouse.dialect import SNOWFLAKE, Dialect
from millrace.warehouse.gateway import WarehouseGateway

logger = logging.getLogger(__name__)

# The silver Parquet payload columns are string-typed for timestamps (Spark
# writes the Debezium ISO string through unchanged; see
# streaming/spark_job.py:_entity_row_schema). The raw Snowflake table mirrors
# that physical layout exactly, the same way DuckDB's implicit read_parquet
# does, so the dbt staging cast (`millrace_timestamp_tz`) is the single place
# either engine converts to a real timestamp.
_RAW_COLUMN_TYPE: MappingProxyType[ValueKind, str] = MappingProxyType(
    {
        ValueKind.BOOLEAN: "BOOLEAN",
        ValueKind.INTEGER: "NUMBER(38,0)",
        ValueKind.DECIMAL: "NUMBER(18,2)",
        ValueKind.STRING: "VARCHAR",
        ValueKind.TIMESTAMP: "VARCHAR",
    }
)

_METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("batch_id", "NUMBER(38,0) NOT NULL"),
    ("source_lsn", "NUMBER(38,0) NOT NULL"),
    ("topic", "VARCHAR NOT NULL"),
    ("partition", "NUMBER(38,0) NOT NULL"),
    ("offset", "NUMBER(38,0) NOT NULL"),
    ("operation", "VARCHAR NOT NULL"),
)


def raw_schema(context: RunContext) -> str:
    """Per-run raw schema name, parallel to `validation.configuration.candidate_schema`."""
    return f"raw_{context.storage_key.split('/', maxsplit=1)[1]}"


def connect(settings: Settings) -> snowflake.connector.SnowflakeConnection:
    import snowflake.connector

    kwargs = credential_kwargs(settings)
    return snowflake.connector.connect(  # pyright: ignore[reportUnknownMemberType]
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        role=settings.snowflake_role or None,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        session_parameters={"TIMEZONE": "UTC", "WEEK_START": 1},
        autocommit=False,
        **kwargs,
    )


def credential_kwargs(settings: Settings) -> dict[str, Any]:
    if settings.snowflake_private_key_path:
        return {"private_key": load_private_key(settings)}
    secret = settings.snowflake_password.get_secret_value()
    authenticator = settings.snowflake_authenticator.strip()
    if authenticator.upper() == "PROGRAMMATIC_ACCESS_TOKEN":
        return {"token": secret, "authenticator": authenticator}
    if authenticator:
        return {"password": secret, "authenticator": authenticator}
    return {"password": secret}


def load_private_key(settings: Settings) -> bytes:
    from cryptography.hazmat.primitives import serialization

    passphrase = settings.snowflake_private_key_passphrase.get_secret_value()
    key_bytes = Path(settings.snowflake_private_key_path).expanduser().read_bytes()
    private_key = serialization.load_pem_private_key(
        key_bytes,
        password=passphrase.encode("utf-8") if passphrase else None,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class SnowflakeGateway:
    def __init__(self, connection: snowflake.connector.SnowflakeConnection) -> None:
        self._connection = connection

    @property
    def dialect(self) -> Dialect:
        return SNOWFLAKE

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, tuple(params) or None)

    def query(
        self, sql: str, params: Sequence[Any] = ()
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        with self._connection.cursor() as cursor:
            cursor.execute(sql, tuple(params) or None)
            names = [description[0] for description in cursor.description or ()]
            rows = [tuple(row) for row in cursor.fetchall()]
        return names, rows

    def begin(self) -> None:
        self.execute("BEGIN")

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()


def _raw_table_ddl(schema: str, entity: Entity) -> str:
    table = quote_relation_name(schema, entity.value)
    payload = [
        f"{quote_identifier(name, case=IdentifierCase.UPPER)} {_RAW_COLUMN_TYPE[kind]}"
        for name, kind in ENTITY_SPECS[entity].fields.items()
        if name != "batch_id"
    ]
    metadata = [
        f"{quote_identifier(name, case=IdentifierCase.UPPER)} {column_type}"
        for name, column_type in _METADATA_COLUMNS
    ]
    columns = ",\n    ".join([*payload, *metadata])
    return f"CREATE OR REPLACE TABLE {table} (\n    {columns}\n)"


def quote_relation_name(schema: str, table: str) -> str:
    return ".".join(quote_identifier(part, case=IdentifierCase.UPPER) for part in (schema, table))


class S3DownloadClient(Protocol):
    def get_paginator(self, operation_name: str) -> Any: ...

    def download_file(self, bucket: str, key: str, filename: str) -> None: ...


class SilverLoadError(RuntimeError):
    pass


def load_silver(
    gateway: WarehouseGateway,
    settings: Settings,
    context: RunContext,
    *,
    temporary_directory: str,
) -> dict[str, int]:
    """Loads the silver Parquet snapshot for every entity into a per-run Snowflake
    raw schema via an internal stage, and asserts the loaded row count against
    the Parquet row count for each entity before returning.

    The Parquet bytes loaded here are the exact same objects DuckDB reads
    through httpfs, so a downstream cross-engine mismatch can only be a
    warehouse-side difference, never a snapshot difference.
    """
    schema = raw_schema(context)
    local_root = Path(temporary_directory) / "snowflake_load" / context.run_id
    # A run_id is normally used exactly once, but a retried or manually
    # re-invoked run must not silently inflate its own row counts with
    # whatever a prior attempt already downloaded here: local downloads are
    # never cleaned up mid-run (only orchestration.runtime.
    # cleanup_temporary_files does, and only on the run's overall exit), so
    # stale files under the same run_id would otherwise sit next to freshly
    # downloaded ones and get counted twice by _parquet_row_count.
    shutil.rmtree(local_root, ignore_errors=True)
    s3_client = cast(
        S3DownloadClient,
        boto3.client(  # pyright: ignore[reportUnknownMemberType]
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key.get_secret_value(),
            region_name=settings.s3_region,
            config=BotoConfig(connect_timeout=10, read_timeout=30),
        ),
    )

    stage = quote_relation_name(schema, "silver_stage")
    gateway.execute(
        f"CREATE OR REPLACE SCHEMA {quote_identifier(schema, case=IdentifierCase.UPPER)}"
    )
    gateway.execute(f"CREATE OR REPLACE STAGE {stage} FILE_FORMAT = (TYPE = PARQUET)")

    loaded_rows: dict[str, int] = {}
    paths = StoragePaths(settings.s3_bucket)
    for entity in Entity:
        prefix = paths.silver_table(entity, run_id=context.run_id).removeprefix(
            f"s3a://{settings.s3_bucket}/"
        )
        local_dir = local_root / entity.value
        local_dir.mkdir(parents=True, exist_ok=True)
        downloaded, expected_rows = _download_parquet_with_retry(
            s3_client, bucket=settings.s3_bucket, prefix=prefix, destination=local_dir
        )
        if not downloaded:
            loaded_rows[entity.value] = 0
            continue

        gateway.execute(_raw_table_ddl(schema, entity))
        for file_path in downloaded:
            gateway.execute(f"PUT file://{file_path} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        table = quote_relation_name(schema, entity.value)
        names, rows = gateway.query(
            f"COPY INTO {table} FROM @{stage} "
            "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE "
            "FILE_FORMAT=(TYPE=PARQUET) ON_ERROR=ABORT_STATEMENT PURGE=TRUE"
        )
        rows_loaded_index = names.index("rows_loaded") if "rows_loaded" in names else None
        actual_rows = (
            sum(int(row[rows_loaded_index]) for row in rows) if rows_loaded_index is not None else 0
        )
        if actual_rows != expected_rows:
            raise SilverLoadError(
                f"entity {entity.value!r}: loaded {actual_rows} rows, "
                f"expected {expected_rows} from the silver Parquet snapshot"
            )
        loaded_rows[entity.value] = actual_rows

    # The gateway's connection uses autocommit=False. DDL (CREATE OR REPLACE
    # TABLE) implicitly commits whatever came before it in Snowflake, which
    # is why every entity but the last silently appeared committed: closing
    # the connection with no explicit commit rolls back the final COPY INTO,
    # since nothing after it triggers an implicit commit. Every entity is
    # committed uniformly here rather than relying on that DDL side effect.
    gateway.commit()
    return loaded_rows


class _ObjectMismatchError(RuntimeError):
    pass


def _download_parquet(
    s3_client: S3DownloadClient,
    *,
    bucket: str,
    prefix: str,
    destination: Path,
) -> list[Path]:
    """Lists and downloads every `.parquet` object under `prefix`, verifying
    each download's byte size against what the listing reported.

    A size mismatch (observed in practice for the entity processed last in a
    run, moments after Spark's job exits) raises `_ObjectMismatchError`
    rather than silently accepting a truncated file: the object being
    listable with its final size does not guarantee every storage backend
    serves the identical byte count on the very next GET, and a row count
    computed from a truncated-but-still-parseable file would otherwise look
    like a legitimately empty entity instead of an incomplete read.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    downloaded: list[Path] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            if not key.endswith(".parquet"):
                continue
            expected_size = int(entry["Size"])
            local_path = destination / Path(key).name
            s3_client.download_file(bucket, key, str(local_path))
            actual_size = local_path.stat().st_size
            if actual_size != expected_size:
                raise _ObjectMismatchError(
                    f"{key}: downloaded {actual_size} bytes, listing reported {expected_size}"
                )
            downloaded.append(local_path)
    return downloaded


def _download_parquet_with_retry(
    s3_client: S3DownloadClient,
    *,
    bucket: str,
    prefix: str,
    destination: Path,
    attempts: int = 5,
    delay_seconds: float = 2.0,
) -> tuple[list[Path], int]:
    last_error: _ObjectMismatchError | None = None
    for attempt in range(1, attempts + 1):
        try:
            downloaded = _download_parquet(
                s3_client, bucket=bucket, prefix=prefix, destination=destination
            )
        except _ObjectMismatchError as exc:
            last_error = exc
            if attempt == attempts:
                raise SilverLoadError(str(exc)) from exc
            time.sleep(delay_seconds)
            continue
        if not downloaded:
            return [], 0
        return downloaded, _parquet_row_count(destination)
    raise SilverLoadError(str(last_error))  # pragma: no cover - loop always returns or raises


def _parquet_row_count(directory: Path) -> int:
    glob_pattern = str(directory / "*.parquet")
    with duckdb.connect(":memory:") as connection:
        result = connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [glob_pattern]
        ).fetchone()
    return int(result[0]) if result is not None else 0
