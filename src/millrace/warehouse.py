from __future__ import annotations

import duckdb

from millrace.settings import Settings


def configure_object_store(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> None:
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(
        """
        CREATE OR REPLACE SECRET millrace_s3 (
            TYPE s3,
            KEY_ID ?,
            SECRET ?,
            REGION ?,
            ENDPOINT ?,
            URL_STYLE 'path',
            USE_SSL ?
        )
        """,
        [
            settings.s3_access_key,
            settings.s3_secret_key.get_secret_value(),
            settings.s3_region,
            settings.s3_endpoint_url.removeprefix("http://").removeprefix("https://"),
            settings.s3_endpoint_url.startswith("https://"),
        ],
    )
