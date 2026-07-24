"""ClickHouse writer for Behavioral Gold rows."""

from __future__ import annotations

from datetime import date
import logging
from typing import Iterable, Sequence

import clickhouse_connect
from pyspark.sql import DataFrame

from common.behavioral_gold_config import GoldBehavioralConfig


logger = logging.getLogger(__name__)


CREATE_BEHAVIORAL_GOLD_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {database}.{table} (
    event_key String,
    event_id Nullable(String),
    event_identity_source Nullable(String),
    event_timestamp DateTime64(3, 'Asia/Tehran'),
    date_key Int32,
    processing_date Date,
    user_key Nullable(String),
    user_id Nullable(String),
    session_key Nullable(String),
    session_id Nullable(String),
    session_start_at Nullable(DateTime64(3, 'Asia/Tehran')),
    session_end_at Nullable(DateTime64(3, 'Asia/Tehran')),
    session_duration_sec Nullable(Int64),
    session_event_count Nullable(Int64),
    device_key Nullable(String),
    device_name Nullable(String),
    primary_device_key Nullable(String),
    event_type_key Nullable(String),
    event_type Nullable(String),
    event_category Nullable(String),
    utm_source Nullable(String),
    ip_address_hash Nullable(String),
    product_id Nullable(String),
    order_id Nullable(String),
    url_path Nullable(String),
    query Nullable(String),
    wishlist_name Nullable(String),
    payment_type Nullable(String),
    shipping_method Nullable(String),
    fulfillment_speed Nullable(String),
    error_code Nullable(String),
    success Nullable(Bool),
    http_status Nullable(Int32),
    quantity Nullable(Int32),
    cart_total_items Nullable(Int32),
    cart_value Nullable(Float64),
    duration_sec Nullable(Int32),
    results_count Nullable(Int32),
    clicked_position Nullable(Int32),
    rating Nullable(Int32),
    text_length Nullable(Int32),
    cart_items_json Nullable(String),
    dq_flags_json Nullable(String),
    kafka_topic Nullable(String),
    kafka_partition Nullable(Int32),
    kafka_offset Nullable(Int64),
    kafka_timestamp Nullable(DateTime64(3, 'Asia/Tehran')),
    bronze_ingestion_timestamp Nullable(DateTime64(3, 'Asia/Tehran')),
    source_file Nullable(String),
    pipeline_run_id Nullable(String),
    silver_ingestion_timestamp DateTime64(3, 'Asia/Tehran'),
    gold_loaded_at DateTime64(3, 'Asia/Tehran')
)
ENGINE = ReplacingMergeTree(gold_loaded_at)
PARTITION BY toYYYYMM(event_timestamp)
ORDER BY (
    processing_date,
    ifNull(event_category, ''),
    ifNull(event_type, ''),
    ifNull(user_key, ''),
    ifNull(session_key, ''),
    event_key
)
SETTINGS index_granularity = 8192
"""


def create_clickhouse_client(config: GoldBehavioralConfig):
    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
        database=config.clickhouse_database,
    )


def ensure_behavioral_gold_table(config: GoldBehavioralConfig) -> None:
    client = create_clickhouse_client(config)
    client.command(f"CREATE DATABASE IF NOT EXISTS {config.clickhouse_database}")
    client.command(
        CREATE_BEHAVIORAL_GOLD_TABLE_SQL.format(
            database=config.clickhouse_database,
            table=config.clickhouse_table,
        )
    )


def _rows_in_batches(df: DataFrame, columns: Sequence[str], batch_size: int):
    batch = []
    for row in df.select(*columns).toLocalIterator():
        batch.append(tuple(row[column] for column in columns))
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _assert_target_columns(df: DataFrame, columns: Iterable[str]) -> None:
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise ValueError(f"Gold DataFrame is missing target columns: {missing}")


def replace_behavioral_gold_partition(
    df: DataFrame,
    config: GoldBehavioralConfig,
    execution_date: date,
) -> int:
    """Replace one processing-date slice in ClickHouse and return inserted rows."""

    columns = config.target_columns
    _assert_target_columns(df, columns)
    source_count = df.count()
    if source_count == 0:
        logger.warning("No Behavioral Gold rows found for %s", execution_date)
        return 0

    client = create_clickhouse_client(config)
    client.command(f"CREATE DATABASE IF NOT EXISTS {config.clickhouse_database}")
    client.command(
        CREATE_BEHAVIORAL_GOLD_TABLE_SQL.format(
            database=config.clickhouse_database,
            table=config.clickhouse_table,
        )
    )
    client.command(
        f"ALTER TABLE {config.qualified_clickhouse_table} "
        f"DELETE WHERE processing_date = toDate('{execution_date.isoformat()}')",
        settings={"mutations_sync": 2},
    )

    inserted = 0
    for batch in _rows_in_batches(df, columns, config.insert_batch_size):
        client.insert(
            config.qualified_clickhouse_table,
            batch,
            column_names=list(columns),
        )
        inserted += len(batch)

    if inserted != source_count:
        raise RuntimeError(
            f"ClickHouse insert count mismatch: source={source_count}, inserted={inserted}."
        )
    logger.info("Inserted %s Behavioral Gold rows for %s", inserted, execution_date)
    return inserted
