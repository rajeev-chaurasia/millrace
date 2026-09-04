from __future__ import annotations

# Dynamic identifiers are validated and quoted before use.
# ruff: noqa: S608
from datetime import UTC, datetime
from typing import Protocol

import duckdb

from millrace.contracts import RunContext
from millrace.validation.configuration import (
    IdentifierCase,
    quote_identifier,
    quote_relation,
    render_relation,
)
from millrace.validation.models import ReconciliationConfig, ValidationReport
from millrace.warehouse.gateway import WarehouseGateway


class PromotionError(RuntimeError):
    pass


class Promoter(Protocol):
    def promote(self, context: RunContext, report: ValidationReport) -> None: ...


def _validate_report(context: RunContext, report: ValidationReport) -> None:
    if not report.passed:
        raise PromotionError("failed validation cannot authorize publication")
    if report.run_id != context.run_id or report.batch_id != context.batch_id:
        raise PromotionError("validation report does not match the promotion context")


class PromotionService:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        config: ReconciliationConfig,
    ) -> None:
        self._connection = connection
        self._config = config
        self._schema = quote_identifier(config.control_schema)

    def promote(self, context: RunContext, report: ValidationReport) -> None:
        _validate_report(context, report)
        self._ensure_control_tables()
        self._connection.begin()
        try:
            validation_row = self._connection.execute(
                f"""
                SELECT batch_id, status
                FROM {self._schema}.validation_runs
                WHERE run_id = ?
                """,
                [context.run_id],
            ).fetchone()
            if validation_row != (context.batch_id, "passed"):
                raise PromotionError("a matching passed validation audit row is required")

            latest_batch = self._connection.execute(
                f"SELECT MAX(batch_id) FROM {self._schema}.publication_runs"
            ).fetchone()
            if (
                latest_batch is not None
                and latest_batch[0] is not None
                and int(latest_batch[0]) > context.batch_id
            ):
                raise PromotionError("publication cannot replace a newer batch")

            existing = self._connection.execute(
                f"""
                SELECT batch_id FROM {self._schema}.publication_runs
                WHERE run_id = ?
                """,
                [context.run_id],
            ).fetchone()
            if existing is not None:
                if existing[0] != context.batch_id:
                    raise PromotionError("published run_id has a different batch_id")
                self._connection.commit()
                return

            for publication in self._config.publications:
                view = quote_relation(publication.view)
                if "." in publication.view:
                    schema = publication.view.rsplit(".", maxsplit=1)[0]
                    self._connection.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {quote_relation(schema)}"
                    )
                relation = render_relation(publication.relation, context)
                batch = quote_identifier(publication.batch_column)
                self._connection.execute(
                    f"CREATE OR REPLACE VIEW {view} AS "
                    f"SELECT * FROM {relation} WHERE {batch} = {context.batch_id}"
                )

            self._connection.execute(
                f"""
                INSERT INTO {self._schema}.publication_runs
                (run_id, batch_id, published_at)
                VALUES (?, ?, ?)
                """,
                [context.run_id, context.batch_id, datetime.now(UTC)],
            )
            self._connection.execute(
                f"""
                CREATE OR REPLACE VIEW {quote_identifier(self._config.analytics_schema)}
                    .current_validation_status AS
                SELECT
                    validation.run_id,
                    'published' AS status,
                    validation.checked_at AS validated_at,
                    publication.published_at,
                    validation.checks_passed,
                    validation.checks_failed
                FROM {self._schema}.validation_runs AS validation
                INNER JOIN {self._schema}.publication_runs AS publication
                    ON validation.run_id = publication.run_id
                ORDER BY publication.published_at DESC
                """
            )
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            raise

    def _ensure_control_tables(self) -> None:
        self._connection.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.publication_runs (
                run_id VARCHAR PRIMARY KEY,
                batch_id BIGINT NOT NULL,
                published_at TIMESTAMPTZ NOT NULL
            )
            """
        )


class SnowflakePromotionService:
    """Snowflake equivalent of `PromotionService`. The same four gates in the same
    order, but the view swap cannot share DuckDB's transaction-wrapped
    `CREATE OR REPLACE VIEW` loop: Snowflake DDL auto-commits, so that loop is
    not atomic there. Instead every publication view is built in a staging
    schema, then `ALTER SCHEMA ... SWAP WITH ...` promotes them all in one
    atomic statement, and publication_runs becomes two-phase (`pending` before
    the swap, `published` after) since the row insert can no longer share a
    transaction with the swap. A crash between those two steps leaves the
    recorded published batch lagging the swapped-in views, never ahead of
    them, and the next promotion attempt for the same run resumes rather than
    re-inserting.
    """

    def __init__(self, gateway: WarehouseGateway, config: ReconciliationConfig) -> None:
        self._gateway = gateway
        self._config = config
        self._schema = quote_identifier(config.control_schema, case=IdentifierCase.UPPER)
        self._analytics_schema = quote_identifier(
            config.analytics_schema, case=IdentifierCase.UPPER
        )
        self._staging_schema = quote_identifier(
            f"{config.analytics_schema}_staging", case=IdentifierCase.UPPER
        )

    def promote(self, context: RunContext, report: ValidationReport) -> None:
        _validate_report(context, report)
        self._ensure_control_tables()

        _, validation_rows = self._gateway.query(
            f"SELECT batch_id, status FROM {self._schema}.validation_runs WHERE run_id = %s",
            [context.run_id],
        )
        validation_row = validation_rows[0] if validation_rows else None
        if validation_row != (context.batch_id, "passed"):
            raise PromotionError("a matching passed validation audit row is required")

        # Only `published` rows count as a newer batch. A promotion that
        # crashed between its `pending` insert and the schema swap never
        # published anything, so letting its batch_id raise the monotonicity
        # floor here would permanently block every lower batch from
        # publishing, with no way to recover short of editing the table.
        _, latest_rows = self._gateway.query(
            f"SELECT MAX(batch_id) FROM {self._schema}.publication_runs WHERE status = 'published'"
        )
        latest_batch = latest_rows[0][0] if latest_rows else None
        if latest_batch is not None and int(latest_batch) > context.batch_id:
            raise PromotionError("publication cannot replace a newer batch")

        _, existing_rows = self._gateway.query(
            f"SELECT batch_id, status FROM {self._schema}.publication_runs WHERE run_id = %s",
            [context.run_id],
        )
        existing = existing_rows[0] if existing_rows else None
        if existing is not None:
            existing_batch, existing_status = existing
            if existing_batch != context.batch_id:
                raise PromotionError("published run_id has a different batch_id")
            if existing_status == "published":
                return

        if existing is None:
            self._gateway.execute(
                f"""
                INSERT INTO {self._schema}.publication_runs (run_id, batch_id, status, published_at)
                VALUES (%s, %s, 'pending', %s)
                """,
                [context.run_id, context.batch_id, datetime.now(UTC)],
            )

        self._build_staged_views(context)
        self._gateway.execute(f"CREATE SCHEMA IF NOT EXISTS {self._analytics_schema}")
        self._gateway.execute(
            f"ALTER SCHEMA {self._analytics_schema} SWAP WITH {self._staging_schema}"
        )
        self._gateway.execute(
            f"UPDATE {self._schema}.publication_runs SET status = 'published' WHERE run_id = %s",
            [context.run_id],
        )
        self._gateway.execute(f"DROP SCHEMA IF EXISTS {self._staging_schema}")
        # The connection uses autocommit=False; DDL auto-commits on Snowflake
        # regardless, so every statement above ends up durably committed by
        # the DDL statement that follows it. That happens to hold today only
        # because DROP SCHEMA is the last statement, which is incidental, not
        # a guarantee, so a fresh connection reading `published` status right
        # after this returns (Gate 4's idempotency check on a retry, or any
        # external reader) still needs an explicit commit, not a reordering
        # accident, to be certain of seeing it.
        self._gateway.commit()

    def _build_staged_views(self, context: RunContext) -> None:
        self._gateway.execute(f"CREATE OR REPLACE SCHEMA {self._staging_schema}")
        for publication in self._config.publications:
            view_name = quote_identifier(
                publication.view.rsplit(".", maxsplit=1)[-1], case=IdentifierCase.UPPER
            )
            relation = render_relation(publication.relation, context, case=IdentifierCase.UPPER)
            batch = quote_identifier(publication.batch_column, case=IdentifierCase.UPPER)
            self._gateway.execute(
                f"CREATE OR REPLACE VIEW {self._staging_schema}.{view_name} AS "
                f"SELECT * FROM {relation} WHERE {batch} = {context.batch_id}"
            )
        self._gateway.execute(
            f"""
            CREATE OR REPLACE VIEW {self._staging_schema}.CURRENT_VALIDATION_STATUS AS
            SELECT
                validation.run_id,
                'published' AS status,
                validation.checked_at AS validated_at,
                publication.published_at,
                validation.checks_passed,
                validation.checks_failed
            FROM {self._schema}.validation_runs AS validation
            INNER JOIN {self._schema}.publication_runs AS publication
                ON validation.run_id = publication.run_id
            WHERE publication.status = 'published'
            ORDER BY publication.published_at DESC
            """
        )

    def _ensure_control_tables(self) -> None:
        self._gateway.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self._gateway.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.publication_runs (
                run_id VARCHAR NOT NULL,
                batch_id BIGINT NOT NULL,
                status VARCHAR NOT NULL,
                published_at TIMESTAMP_TZ NOT NULL
            )
            """
        )
