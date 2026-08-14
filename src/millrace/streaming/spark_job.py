from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from millrace.streaming.config import InputMode, StreamingConfig, parse_args
from millrace.streaming.models import (
    CdcEvent,
    Entity,
    KafkaMetadata,
    MalformedEvent,
    Operation,
    Tombstone,
)
from millrace.streaming.parser import parse_debezium_event
from millrace.streaming.schemas import ENTITY_SPECS, ValueKind
from millrace.streaming.storage import StoragePaths

_NORMALIZED_SCHEMA = StructType(
    [
        StructField("is_valid", BooleanType(), nullable=False),
        StructField("is_tombstone", BooleanType(), nullable=False),
        StructField("entity", StringType(), nullable=True),
        StructField("operation", StringType(), nullable=True),
        StructField("key_json", StringType(), nullable=True),
        StructField("before_json", StringType(), nullable=True),
        StructField("after_json", StringType(), nullable=True),
        StructField("row_json", StringType(), nullable=True),
        StructField("batch_id", LongType(), nullable=True),
        StructField("source_lsn", LongType(), nullable=True),
        StructField("source_timestamp_ms", LongType(), nullable=True),
        StructField("transaction_order", LongType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
        StructField("reason_code", StringType(), nullable=True),
        StructField("reason_detail", StringType(), nullable=True),
        StructField("raw_payload", StringType(), nullable=True),
    ]
)


def create_spark_session(config: StreamingConfig) -> SparkSession:
    ssl_enabled = str(config.s3_endpoint_url.lower().startswith("https://")).lower()
    return (
        SparkSession.builder.appName("millrace-cdc-ingestion")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", config.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.s3_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", config.s3_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", ssl_enabled)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def run_ingestion(config: StreamingConfig, spark: SparkSession | None = None) -> None:
    active_spark = spark or create_spark_session(config)
    paths = StoragePaths(config.s3_bucket)
    try:
        if config.input_mode is InputMode.BRONZE_BACKFILL:
            if config.bronze_uri is None:
                raise ValueError("bronze_uri is required for bronze backfills")
            for entity in Entity:
                _write_snapshot(
                    active_spark,
                    config=config,
                    paths=paths,
                    entity=entity,
                    history_path=f"{config.bronze_uri.rstrip('/')}/table={entity.value}",
                    partition_end=config.partition_end,
                )
            return
        for entity in Entity:
            query = _start_entity_query(
                active_spark,
                config=config,
                paths=paths,
                entity=entity,
            )
            try:
                query.awaitTermination()
            finally:
                if query.isActive:
                    query.stop()
        for entity in Entity:
            _write_snapshot(active_spark, config=config, paths=paths, entity=entity)
    finally:
        if spark is None:
            active_spark.stop()


def _start_entity_query(
    spark: SparkSession,
    *,
    config: StreamingConfig,
    paths: StoragePaths,
    entity: Entity,
) -> StreamingQuery:
    topic = config.topic_for(entity)
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
        .select("key", "value", "topic", "partition", "offset", "timestamp")
    )
    processor = _micro_batch_processor(config=config, paths=paths, entity=entity)
    return (
        source.writeStream.foreachBatch(processor)
        .option("checkpointLocation", paths.checkpoint(topic=topic))
        .trigger(availableNow=True)
        .start()
    )


def _micro_batch_processor(
    *,
    config: StreamingConfig,
    paths: StoragePaths,
    entity: Entity,
) -> Callable[[DataFrame, int], None]:
    normalize = F.udf(_normalize_kafka_record, _NORMALIZED_SCHEMA)

    def process(batch: DataFrame, stream_batch_id: int) -> None:
        normalized = (
            batch.withColumn(
                "parsed",
                normalize("key", "value", "topic", "partition", "offset", "timestamp"),
            )
            .select("*", "parsed.*")
            .drop("parsed", "key", "value")
            .persist()
        )
        try:
            valid = normalized.filter(F.col("is_valid")).drop(
                "is_valid", "reason_code", "reason_detail"
            )
            event_dates = [
                row.event_date
                for row in valid.select("event_date").distinct().collect()
                if row.event_date is not None
            ]
            for event_date in event_dates:
                target = paths.bronze_run(
                    entity,
                    event_date=event_date,
                    run_id=config.run_id,
                    stream_batch_id=stream_batch_id,
                )
                valid.filter(F.col("event_date") == F.lit(event_date)).write.mode(
                    "overwrite"
                ).parquet(target)

            malformed = normalized.filter(~F.col("is_valid")).select(
                "topic",
                "partition",
                "offset",
                "timestamp",
                "raw_payload",
                "reason_code",
                "reason_detail",
            )
            if not malformed.isEmpty():
                malformed.write.mode("overwrite").parquet(
                    paths.dead_letter_run(
                        entity,
                        run_id=config.run_id,
                        stream_batch_id=stream_batch_id,
                    )
                )
        finally:
            normalized.unpersist()

    return process


def _normalize_kafka_record(
    key: bytes | bytearray | None,
    value: bytes | bytearray | None,
    topic: str,
    partition: int,
    offset: int,
    timestamp: datetime | None,
) -> tuple[object, ...]:
    aware_timestamp = timestamp
    if aware_timestamp is not None and aware_timestamp.tzinfo is None:
        aware_timestamp = aware_timestamp.replace(tzinfo=UTC)
    metadata = KafkaMetadata(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=aware_timestamp,
    )
    result = parse_debezium_event(
        _bytes_or_none(value),
        key_payload=_bytes_or_none(key),
        metadata=metadata,
    )
    if isinstance(result, MalformedEvent):
        return (
            False,
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            _event_date(None, aware_timestamp),
            result.reason_code.value,
            result.detail,
            result.raw_payload,
        )
    if isinstance(result, Tombstone):
        return (
            True,
            True,
            result.entity.value,
            Operation.TOMBSTONE.value,
            _json(result.key),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            _event_date(None, aware_timestamp),
            None,
            None,
            None,
        )
    return _normalized_event(result, aware_timestamp)


def _normalized_event(
    event: CdcEvent,
    kafka_timestamp: datetime | None,
) -> tuple[object, ...]:
    return (
        True,
        False,
        event.entity.value,
        event.operation.value,
        _json(event.key),
        _json(event.before),
        _json(event.after),
        _json(event.row),
        event.batch_id,
        event.source_lsn,
        event.source_timestamp_ms,
        event.transaction_order,
        _event_date(event.source_timestamp_ms, kafka_timestamp),
        None,
        None,
        event.raw_payload,
    )


def _event_date(source_timestamp_ms: int | None, kafka_timestamp: datetime | None) -> date:
    if source_timestamp_ms is not None:
        return datetime.fromtimestamp(source_timestamp_ms / 1000, tz=UTC).date()
    if kafka_timestamp is not None:
        return kafka_timestamp.astimezone(UTC).date()
    return datetime.now(tz=UTC).date()


def _write_snapshot(
    spark: SparkSession,
    *,
    config: StreamingConfig,
    paths: StoragePaths,
    entity: Entity,
    history_path: str | None = None,
    partition_end: str | None = None,
) -> None:
    output_schema = _snapshot_schema(entity)
    try:
        history = spark.read.option("recursiveFileLookup", "true").parquet(
            history_path or paths.bronze_table(entity)
        )
        history = history.filter(
            (F.col("batch_id") <= F.lit(config.batch_id)) & (~F.col("is_tombstone"))
        )
        if partition_end is not None:
            history = history.filter(F.col("event_date") <= F.to_date(F.lit(partition_end)))
    except AnalysisException as exc:
        if exc.getCondition() != "PATH_NOT_FOUND":
            raise
        empty_rows: list[tuple[object, ...]] = []
        spark.createDataFrame(empty_rows, output_schema).write.mode("overwrite").parquet(
            paths.silver_table(entity, run_id=config.run_id)
        )
        return

    from pyspark.sql.window import Window

    ordering = Window.partitionBy("key_json").orderBy(
        F.col("source_lsn").desc(),
        F.col("transaction_order").desc(),
        F.col("partition").desc(),
        F.col("offset").desc(),
    )
    current = (
        history.withColumn("_rank", F.row_number().over(ordering))
        .filter((F.col("_rank") == 1) & (F.col("operation") != Operation.DELETE.value))
        .withColumn("_row", F.from_json("row_json", _entity_row_schema(entity)))
    )
    payload_columns = [
        F.col(f"_row.{field.name}")
        for field in _entity_row_schema(entity).fields
        if field.name != "batch_id"
    ]
    snapshot = current.select(
        *payload_columns,
        F.col("batch_id"),
        F.col("source_lsn"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("operation"),
    )
    snapshot.write.mode("overwrite").parquet(paths.silver_table(entity, run_id=config.run_id))


def _snapshot_schema(entity: Entity) -> StructType:
    payload_fields = [
        field for field in _entity_row_schema(entity).fields if field.name != "batch_id"
    ]
    return StructType(
        [
            *payload_fields,
            StructField("batch_id", LongType(), nullable=False),
            StructField("source_lsn", LongType(), nullable=False),
            StructField("topic", StringType(), nullable=False),
            StructField("partition", IntegerType(), nullable=False),
            StructField("offset", LongType(), nullable=False),
            StructField("operation", StringType(), nullable=False),
        ]
    )


def _entity_row_schema(entity: Entity) -> StructType:
    fields: list[StructField] = []
    for name, kind in ENTITY_SPECS[entity].fields.items():
        data_type: DataType = {
            ValueKind.BOOLEAN: BooleanType(),
            ValueKind.INTEGER: LongType(),
            ValueKind.DECIMAL: DecimalType(18, 2),
            ValueKind.STRING: StringType(),
            ValueKind.TIMESTAMP: StringType(),
        }[kind]
        fields.append(StructField(name, data_type, nullable=True))
    return StructType(fields)


def _bytes_or_none(value: bytes | bytearray | None) -> bytes | None:
    return bytes(value) if value is not None else None


def _json(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def main(argv: Sequence[str] | None = None) -> None:
    run_ingestion(parse_args(argv))


if __name__ == "__main__":
    main()
