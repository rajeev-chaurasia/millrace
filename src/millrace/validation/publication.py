from __future__ import annotations

# Dynamic identifiers are validated and quoted before use.
# ruff: noqa: S608
from datetime import UTC, datetime

import duckdb

from millrace.contracts import RunContext
from millrace.validation.configuration import (
    quote_identifier,
    quote_relation,
    render_relation,
)
from millrace.validation.models import ReconciliationConfig, ValidationReport


class PromotionError(RuntimeError):
    pass


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
        self._validate_report(context, report)
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
                CREATE OR REPLACE VIEW analytics.current_validation_status AS
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

    @staticmethod
    def _validate_report(context: RunContext, report: ValidationReport) -> None:
        if not report.passed:
            raise PromotionError("failed validation cannot authorize publication")
        if report.run_id != context.run_id or report.batch_id != context.batch_id:
            raise PromotionError("validation report does not match the promotion context")
