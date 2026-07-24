"""Build the Behavioral Gold One Big Table from Silver Iceberg tables."""

from __future__ import annotations

from datetime import date
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.behavioral_gold_config import GoldBehavioralConfig


REQUIRED_FACT_COLUMNS = {
    "event_key",
    "event_timestamp",
    "processing_date",
    "device_key",
    "event_type_key",
    "session_key",
}

REQUIRED_DEVICE_COLUMNS = {"device_key", "device_name"}

REQUIRED_EVENT_TYPE_COLUMNS = {"event_type_key", "event_category"}

REQUIRED_SESSION_COLUMNS = {
    "session_key",
    "session_start_at",
    "session_end_at",
    "session_duration_sec",
    "primary_device_key",
    "event_count",
}


def _require_columns(df: DataFrame, required: Iterable[str], name: str) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def build_behavioral_gold_obt(
    spark: SparkSession,
    config: GoldBehavioralConfig,
    execution_date: date,
) -> DataFrame:
    """Return one denormalized row per Behavioral event for ClickHouse."""

    fact = spark.table(config.fact_table).filter(
        F.col("processing_date") == F.lit(execution_date).cast("date")
    )
    _require_columns(fact, REQUIRED_FACT_COLUMNS, config.fact_table)

    device_src = spark.table(config.device_table)
    _require_columns(device_src, REQUIRED_DEVICE_COLUMNS, config.device_table)
    device = device_src.select(
        "device_key",
        F.col("device_name").alias("dim_device_name"),
    )

    event_type_src = spark.table(config.event_type_table)
    _require_columns(
        event_type_src, REQUIRED_EVENT_TYPE_COLUMNS, config.event_type_table
    )
    event_type = event_type_src.select(
        "event_type_key",
        F.col("event_category").alias("dim_event_category"),
    )

    session_src = spark.table(config.session_table)
    _require_columns(session_src, REQUIRED_SESSION_COLUMNS, config.session_table)
    session = session_src.select(
        "session_key",
        F.col("session_start_at").alias("dim_session_start_at"),
        F.col("session_end_at").alias("dim_session_end_at"),
        F.col("session_duration_sec").alias("dim_session_duration_sec"),
        F.col("primary_device_key").alias("dim_primary_device_key"),
        F.col("event_count").alias("dim_session_event_count"),
    )

    joined = (
        fact.alias("f")
        .join(device.alias("d"), "device_key", "left")
        .join(event_type.alias("et"), "event_type_key", "left")
        .join(session.alias("s"), "session_key", "left")
    )

    return joined.select(
        F.col("f.event_key"),
        F.col("f.event_id"),
        F.col("f.event_identity_source"),
        F.col("f.event_timestamp"),
        F.col("f.date_key"),
        F.col("f.processing_date"),
        F.col("f.user_key"),
        F.col("f.user_id"),
        F.col("f.session_key"),
        F.col("f.session_id"),
        F.col("dim_session_start_at").alias("session_start_at"),
        F.col("dim_session_end_at").alias("session_end_at"),
        F.col("dim_session_duration_sec").alias("session_duration_sec"),
        F.col("dim_session_event_count").alias("session_event_count"),
        F.col("f.device_key"),
        F.col("dim_device_name").alias("device_name"),
        F.col("dim_primary_device_key").alias("primary_device_key"),
        F.col("f.event_type_key"),
        F.col("f.event_type"),
        F.col("dim_event_category").alias("event_category"),
        F.col("f.utm_source"),
        F.col("f.ip_address_hash"),
        F.col("f.product_id"),
        F.col("f.order_id"),
        F.col("f.url_path"),
        F.col("f.query"),
        F.col("f.wishlist_name"),
        F.col("f.payment_type"),
        F.col("f.shipping_method"),
        F.col("f.fulfillment_speed"),
        F.col("f.error_code"),
        F.col("f.success"),
        F.col("f.http_status"),
        F.col("f.quantity"),
        F.col("f.cart_total_items"),
        F.col("f.cart_value"),
        F.col("f.duration_sec"),
        F.col("f.results_count"),
        F.col("f.clicked_position"),
        F.col("f.rating"),
        F.col("f.text_length"),
        F.to_json(F.col("f.cart_items")).alias("cart_items_json"),
        F.to_json(F.col("f.dq_flags")).alias("dq_flags_json"),
        F.col("f.kafka_topic"),
        F.col("f.kafka_partition"),
        F.col("f.kafka_offset"),
        F.col("f.kafka_timestamp"),
        F.col("f.bronze_ingestion_timestamp"),
        F.col("f.source_file"),
        F.col("f.pipeline_run_id"),
        F.col("f.silver_ingestion_timestamp"),
        F.current_timestamp().alias("gold_loaded_at"),
    )
