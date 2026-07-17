"""Kimball transformations for the independent Behavioral star schema."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_iceberg_writer import table_exists
from common.silver_behavioral_keys import natural_key_hash


logger = logging.getLogger(__name__)

EVENT_CATEGORY_MAPPING = {
    "browse": ("page_view", "product_view", "home_view", "category_view", "click"),
    "search": ("search_product", "search"),
    "cart": ("add_to_cart", "remove_from_cart", "view_cart", "update_cart"),
    "checkout": (
        "checkout_start",
        "checkout_attempt",
        "payment_attempt",
        "purchase",
        "order_placed",
    ),
    "engagement": ("rating", "review", "product_rating"),
    "wishlist": ("wishlist_add", "wishlist_remove"),
    "error": ("error", "page_error"),
}


def categorize_event_type() -> F.Column:
    result = F.lit("other")
    for category, event_types in reversed(tuple(EVENT_CATEGORY_MAPPING.items())):
        result = F.when(
            F.col("event_type").isin(*event_types), F.lit(category)
        ).otherwise(result)
    return result


def add_dimension_keys(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("session_key", natural_key_hash("session", "session_id"))
        .withColumn(
            "device_key",
            natural_key_hash("device", "device", case_sensitive=False),
        )
        .withColumn(
            "event_type_key",
            natural_key_hash("event_type", "event_type", case_sensitive=False),
        )
    )


def resolve_shared_user_keys(
    spark: SparkSession,
    df: DataFrame,
    config: BehavioralRuntimeConfig,
) -> DataFrame:
    """Optionally read a shared user dimension; never create or update it."""

    if not config.enable_shared_user_lookup:
        return df.withColumn("user_key", F.lit(None).cast("string"))

    table = config.shared_user_table
    if not table_exists(spark, table):
        logger.warning(
            "Shared user lookup requested but table %s does not exist; user_key remains null.",
            table,
        )
        return df.withColumn("user_key", F.lit(None).cast("string"))

    users = spark.table(table)
    required = {"user_id", "user_key"}
    if not required.issubset(set(users.columns)):
        logger.warning(
            "Shared user table %s lacks %s; user_key remains null.",
            table,
            sorted(required - set(users.columns)),
        )
        return df.withColumn("user_key", F.lit(None).cast("string"))

    if "is_current" in users.columns:
        users = users.filter(F.col("is_current") == F.lit(True))
    elif "valid_to" in users.columns:
        users = users.filter(F.col("valid_to").isNull())

    # Avoid multiplying events if the shared dimension has accidental duplicate
    # current rows. The deterministic ordering prefers non-null keys.
    user_window = Window.partitionBy("_lookup_user_id").orderBy(
        F.col("_lookup_user_key").asc_nulls_last()
    )
    lookup = (
        users.select(
            F.col("user_id").cast("string").alias("_lookup_user_id"),
            F.col("user_key").cast("string").alias("_lookup_user_key"),
        )
        .withColumn("_rn", F.row_number().over(user_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return (
        df.join(
            lookup,
            F.col("user_id") == F.col("_lookup_user_id"),
            "left",
        )
        .drop("_lookup_user_id")
        .withColumnRenamed("_lookup_user_key", "user_key")
    )


def build_dim_device_updates(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("device_key").isNotNull())
        .groupBy("device_key", F.col("device").alias("device_name"))
        .agg(
            F.min("event_timestamp").alias("first_seen_at"),
            F.max("event_timestamp").alias("last_seen_at"),
        )
        .withColumn("silver_updated_at", F.current_timestamp())
        .select(
            "device_key",
            "device_name",
            "first_seen_at",
            "last_seen_at",
            "silver_updated_at",
        )
    )


def build_dim_event_type_updates(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("event_type_key").isNotNull())
        .withColumn("event_category", categorize_event_type())
        .groupBy("event_type_key", "event_type", "event_category")
        .agg(
            F.min("event_timestamp").alias("first_seen_at"),
            F.max("event_timestamp").alias("last_seen_at"),
        )
        .withColumn("silver_updated_at", F.current_timestamp())
        .select(
            "event_type_key",
            "event_type",
            "event_category",
            "first_seen_at",
            "last_seen_at",
            "silver_updated_at",
        )
    )


def _hashed_ip_address() -> F.Column:
    """Retain analytical linkage without storing raw IP addresses in the fact."""

    normalized = F.lower(F.trim(F.col("ip_address").cast("string")))
    return F.when(
        normalized.isNotNull() & (normalized != ""),
        F.sha2(F.concat(F.lit("ip|"), normalized), 256),
    ).otherwise(F.lit(None).cast("string"))


def build_fact_behavioral_events(df: DataFrame) -> DataFrame:
    empty_flags = F.expr("cast(array() as array<string>)")
    return (
        df.withColumn(
            "date_key", F.date_format("event_timestamp", "yyyyMMdd").cast("int")
        )
        .withColumn(
            "dq_flags",
            F.coalesce(
                F.col("validation_warnings").cast("array<string>"), empty_flags
            ),
        )
        .withColumn("ip_address_hash", _hashed_ip_address())
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
        .select(
            "event_key",
            F.col("event_id").cast("string").alias("event_id"),
            "event_identity_source",
            "date_key",
            "user_key",
            "user_id",
            "session_key",
            "session_id",
            "device_key",
            "event_type_key",
            "event_type",
            "event_timestamp",
            "utm_source",
            "ip_address_hash",
            "product_id",
            "order_id",
            "url_path",
            "query",
            "wishlist_name",
            "payment_type",
            "shipping_method",
            "fulfillment_speed",
            "error_code",
            "success",
            "http_status",
            "quantity",
            "cart_total_items",
            "cart_value",
            "duration_sec",
            "results_count",
            "clicked_position",
            "rating",
            "text_length",
            "cart_items",
            "dq_flags",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "bronze_ingestion_timestamp",
            F.col("_source_file").alias("source_file"),
            "processing_date",
            "pipeline_run_id",
            "silver_ingestion_timestamp",
        )
    )


def recompute_dim_session(
    spark: SparkSession,
    fact_table: str,
    touched_session_keys: DataFrame,
) -> DataFrame:
    """Recompute touched sessions with deterministic first-event attributes."""

    fact = spark.table(fact_table)
    touched = touched_session_keys.select("session_key").distinct()
    session_facts = fact.join(touched, "session_key", "inner").filter(
        F.col("session_key").isNotNull()
    )

    full_session_window = (
        Window.partitionBy("session_key")
        .orderBy(
            F.col("event_timestamp").asc_nulls_last(),
            F.col("event_key").asc_nulls_last(),
        )
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    ordered = (
        session_facts.withColumn(
            "_first_user_key",
            F.first("user_key", ignorenulls=True).over(full_session_window),
        )
        .withColumn(
            "_first_user_id",
            F.first("user_id", ignorenulls=True).over(full_session_window),
        )
        .withColumn(
            "_first_device_key",
            F.first("device_key", ignorenulls=True).over(full_session_window),
        )
    )

    return (
        ordered.groupBy("session_key", "session_id")
        .agg(
            F.max("_first_user_key").alias("user_key"),
            F.max("_first_user_id").alias("user_id"),
            F.min("event_timestamp").alias("session_start_at"),
            F.max("event_timestamp").alias("session_end_at"),
            F.max("_first_device_key").alias("primary_device_key"),
            F.count(F.lit(1)).cast("bigint").alias("event_count"),
        )
        .withColumn(
            "session_duration_sec",
            (
                F.col("session_end_at").cast("long")
                - F.col("session_start_at").cast("long")
            ).cast("bigint"),
        )
        .withColumn("silver_updated_at", F.current_timestamp())
        .select(
            "session_key",
            "session_id",
            "user_key",
            "user_id",
            "session_start_at",
            "session_end_at",
            "session_duration_sec",
            "primary_device_key",
            "event_count",
            "silver_updated_at",
        )
    )
