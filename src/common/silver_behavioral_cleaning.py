"""Normalization and deterministic defensive deduplication for Behavioral rows."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)

NULL_STRING_VALUES = ("", "null", "none", "n/a", "na", "unknown", "-")

DESCRIPTIVE_STRING_FIELDS = (
    "product_id",
    "order_id",
    "url_path",
    "query",
    "wishlist_name",
    "payment_type",
    "shipping_method",
    "fulfillment_speed",
    "error_code",
    "ip_address",
    "utm_source",
)

LOWERCASE_FIELDS = (
    "event_type",
    "payment_type",
    "shipping_method",
    "fulfillment_speed",
    "utm_source",
)

TARGET_TYPES = {
    "quantity": "int",
    "cart_total_items": "int",
    "cart_value": "double",
    "duration_sec": "int",
    "http_status": "int",
    "results_count": "int",
    "clicked_position": "int",
    "rating": "int",
    "text_length": "int",
    "success": "boolean",
    "event_timestamp": "timestamp",
    "kafka_partition": "int",
    "kafka_offset": "bigint",
    "kafka_timestamp": "timestamp",
    "bronze_ingestion_timestamp": "timestamp",
    "processing_date": "date",
}

DEVICE_ALIASES = {
    "android": "mobile",
    "ios": "mobile",
    "iphone": "mobile",
    "phone": "mobile",
    "web": "desktop",
    "pc": "desktop",
}


def _normalize_device(df: DataFrame) -> DataFrame:
    source = "device" if "device" in df.columns else "device_type"
    raw = F.lower(F.trim(F.col(source).cast("string")))
    device = F.when(
        raw.isNull() | raw.isin(*NULL_STRING_VALUES),
        F.lit("unknown"),
    ).otherwise(raw)
    for old, new in DEVICE_ALIASES.items():
        device = F.when(device == old, F.lit(new)).otherwise(device)
    result = df.withColumn("device", device)
    if source == "device_type":
        result = result.drop("device_type")
    return result


def _trim_identity_fields(df: DataFrame) -> DataFrame:
    for field in ("event_id", "user_id", "session_id", "event_type"):
        if field in df.columns:
            df = df.withColumn(field, F.trim(F.col(field).cast("string")))
    return df


def _clean_descriptive_strings(df: DataFrame) -> DataFrame:
    for field in DESCRIPTIVE_STRING_FIELDS:
        if field not in df.columns:
            continue
        value = F.trim(F.col(field).cast("string"))
        df = df.withColumn(
            field,
            F.when(
                value.isNull() | F.lower(value).isin(*NULL_STRING_VALUES),
                F.lit(None).cast("string"),
            ).otherwise(value),
        )
    return df


def _normalize_case(df: DataFrame) -> DataFrame:
    for field in LOWERCASE_FIELDS:
        if field in df.columns:
            df = df.withColumn(
                field,
                F.regexp_replace(F.lower(F.trim(F.col(field))), r"[\s-]+", "_"),
            )
    return df


def _normalize_types(df: DataFrame) -> DataFrame:
    for field, target_type in TARGET_TYPES.items():
        if field in df.columns:
            df = df.withColumn(field, F.expr(f"try_cast(`{field}` as {target_type})"))
    return df


def _deterministic_deduplicate(df: DataFrame) -> DataFrame:
    """Keep a reproducible survivor if a caller bypasses validation.

    Validation already quarantines duplicate event keys. This remains a final
    safety net, but unlike ``dropDuplicates`` its survivor is deterministic.
    """

    ordering = [
        F.col("bronze_ingestion_timestamp").asc_nulls_last(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_topic").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
        F.col("pipeline_run_id").asc_nulls_last(),
    ]
    window = Window.partitionBy("event_key").orderBy(*ordering)
    return (
        df.withColumn("_cleaning_row_number", F.row_number().over(window))
        .filter(F.col("_cleaning_row_number") == 1)
        .drop("_cleaning_row_number")
    )


def clean_behavioral_data(df: DataFrame) -> DataFrame:
    cleaned = _normalize_device(df)
    cleaned = _trim_identity_fields(cleaned)
    cleaned = _clean_descriptive_strings(cleaned)
    cleaned = _normalize_case(cleaned)
    cleaned = _normalize_types(cleaned)
    cleaned = cleaned.withColumn("event_date", F.to_date("event_timestamp"))
    cleaned = _deterministic_deduplicate(cleaned)
    cleaned = cleaned.withColumn("silver_cleaned_at", F.current_timestamp())
    logger.info("Behavioral cleaning complete.")
    return cleaned
