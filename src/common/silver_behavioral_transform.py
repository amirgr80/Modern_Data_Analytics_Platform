from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StructField, StructType


# NOTE: This module grows across the Silver steps. Step 2 adds the
# Bronze-read / cleansing / quarantine functions below. Later steps will
# add dimension- and fact-building functions to this same file.


# ---------------------------------------------------------------------
# dim_date seeding (cheap, run once)
# ---------------------------------------------------------------------
def build_dim_date_seed(spark: SparkSession, start: date, end: date) -> DataFrame:
    """One row per calendar day in [start, end]."""

    schema = StructType([StructField("full_date", DateType(), False)])
    days = [(start + timedelta(days=i),) for i in range((end - start).days + 1)]
    dates_df = spark.createDataFrame(days, schema)

    return (
        dates_df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .select(
            "date_key", "full_date", "year", "quarter", "month", "month_name",
            "day", "day_of_week", "day_name", "week_of_year", "is_weekend",
        )
    )


# ---------------------------------------------------------------------
# Bronze read
# ---------------------------------------------------------------------
def read_bronze_behavioral_partition(
    spark: SparkSession,
    bronze_path: str,
    execution_date: date,
) -> DataFrame:
    """
    Read the Bronze behavioral Parquet partition for a single day.

    Bronze is partitioned by year/month/day (see bronze_writer.py), so
    filtering on those columns lets Spark prune to just this partition
    instead of scanning the whole dataset. The partition values are
    literal directory names, so this is independent of session timezone.
    """

    df = spark.read.parquet(bronze_path)

    return df.where(
        (F.col("year") == execution_date.year)
        & (F.col("month") == execution_date.month)
        & (F.col("day") == execution_date.day)
    )


def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Bronze may be reprocessed (retries/backfills). kafka_partition +
    kafka_offset uniquely identify a message, so dedupe on that pair.
    """
    return df.dropDuplicates(["kafka_partition", "kafka_offset"])


# ---------------------------------------------------------------------
# valid / invalid split (uses Bronze's is_valid flag)
# ---------------------------------------------------------------------
def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split Bronze rows using the is_valid flag Bronze already computed.
    Invalid rows go to quarantine and never reach the star schema.
    """
    valid_df = df.where(F.col("is_valid") == True)      # noqa: E712
    invalid_df = df.where(F.col("is_valid") == False)   # noqa: E712
    return valid_df, invalid_df


# ---------------------------------------------------------------------
# soft data-quality flags (rows KEPT, just annotated)
# ---------------------------------------------------------------------
# Unlike Bronze's is_valid split, rows tripping these rules are kept in
# the fact table (annotated via dq_flags), since the anomalies are often
# analytically interesting -- see the project's anomaly-detection notes
# (negative order amount, etc.) -- rather than structurally broken.
SOFT_VALIDATION_RULES = {
    "negative_cart_value": "cart_value is not null and cart_value < 0",
    "non_positive_quantity": "quantity is not null and quantity <= 0",
    "negative_duration": "duration_sec is not null and duration_sec < 0",
    "rating_out_of_range": "rating is not null and (rating < 1 or rating > 5)",
    "negative_results_count": "results_count is not null and results_count < 0",
    "negative_clicked_position": "clicked_position is not null and clicked_position < 0",
    "negative_text_length": "text_length is not null and text_length < 0",
    "unreasonable_http_status": "http_status is not null and (http_status < 100 or http_status > 599)",
}


def apply_silver_data_quality(df: DataFrame) -> DataFrame:
    """Add a dq_flags array column. Rows are never dropped here."""

    flag_expressions = [
        f"CASE WHEN {condition} THEN '{flag_name}' END"
        for flag_name, condition in SOFT_VALIDATION_RULES.items()
    ]
    dq_flags_expr = (
        "filter(array(" + ", ".join(flag_expressions) + "), x -> x is not null)"
    )
    return df.withColumn("dq_flags", F.expr(dq_flags_expr))


# ---------------------------------------------------------------------
# quarantine row shaping
# ---------------------------------------------------------------------
def build_quarantine_rows(invalid_df: DataFrame) -> DataFrame:
    """
    Shape Bronze-invalid rows into the quarantine table schema. Keeps the
    raw field values + Bronze's validation_errors for later debugging.
    """
    return (
        invalid_df
        .withColumn("silver_quarantine_timestamp", F.current_timestamp())
        .select(
            "kafka_partition",
            "kafka_offset",
            "validation_errors",
            F.col("user_id").alias("raw_user_id"),
            F.col("session_id").alias("raw_session_id"),
            F.col("event_type").alias("raw_event_type"),
            F.col("timestamp").alias("raw_timestamp"),
            F.col("device").alias("raw_device"),
            "bronze_ingestion_timestamp",
            "silver_quarantine_timestamp",
        )
    )


# =====================================================================
# Step 3: dimension + fact building
# =====================================================================

# Best-effort event_type -> event_category mapping, inferred from which
# schema fields each kind of event populates (cart_items/cart_value ->
# cart, payment_type/order_id -> checkout, etc). The exact event_type
# string values produced by the mentor's generator are NOT yet confirmed
# against real Kafka messages -- check Kafka-UI and extend this if real
# values fall into 'other'.
EVENT_CATEGORY_MAPPING = {
    "browse": ["page_view", "product_view", "home_view", "category_view"],
    "search": ["search_product", "search"],
    "cart": ["add_to_cart", "remove_from_cart", "view_cart", "update_cart"],
    "checkout": ["checkout_start", "checkout_attempt", "purchase", "order_placed"],
    "engagement": ["rating", "review", "product_rating"],
    "wishlist": ["wishlist_add", "wishlist_remove"],
    "error": ["error", "page_error"],
}


def categorize_event_type(event_type_col: str = "event_type"):
    branches = []
    for category, event_types in EVENT_CATEGORY_MAPPING.items():
        in_list = ", ".join(f"'{et}'" for et in event_types)
        branches.append(f"WHEN {event_type_col} IN ({in_list}) THEN '{category}'")
    case_expr = "CASE " + " ".join(branches) + " ELSE 'other' END"
    return F.expr(case_expr)


# ---------------------------------------------------------------------
# lookup dimensions (device, event_type) -- small, insert-if-new
# ---------------------------------------------------------------------
def build_dim_device_updates(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df
        .where(F.col("device").isNotNull())
        .groupBy(F.col("device").alias("device_name"))
        .agg(F.min("event_timestamp").alias("first_seen_at"))
        .withColumn("device_key", F.col("device_name"))
        .select("device_key", "device_name", "first_seen_at")
    )


def build_dim_event_type_updates(valid_df: DataFrame) -> DataFrame:
    return (
        valid_df
        .where(F.col("event_type").isNotNull())
        .withColumn("event_category", categorize_event_type())
        .groupBy("event_type", "event_category")
        .agg(F.min("event_timestamp").alias("first_seen_at"))
        .withColumn("event_type_key", F.col("event_type"))
        .select("event_type_key", "event_type", "event_category", "first_seen_at")
    )


# ---------------------------------------------------------------------
# fact table
# ---------------------------------------------------------------------
def build_fact_behavioral_events(valid_flagged_df: DataFrame) -> DataFrame:
    """
    Shape valid (already dq-flagged) rows into the wide fact schema.

    Dimension keys (user_id, session_id, device, event_type) are stored
    as their natural-key values directly -- no surrogate-key lookup joins
    needed, since the dimensions currently use the natural key as their
    surrogate key too (see the DDL comments). event_key is built from
    kafka_partition + kafka_offset since the real Avro schema has no
    event_id field.
    """
    return (
        valid_flagged_df
        .withColumn(
            "event_key",
            F.concat_ws("_", F.col("kafka_partition"), F.col("kafka_offset")),
        )
        .withColumn(
            "date_key",
            F.date_format("event_date", "yyyyMMdd").cast(IntegerType()),
        )
        .withColumn("silver_ingestion_timestamp", F.current_timestamp())
        .select(
            "event_key", "date_key", "user_id", "session_id", "device",
            "event_type", "event_timestamp", "product_id", "order_id",
            "url_path", "query", "wishlist_name", "payment_type",
            "shipping_method", "fulfillment_speed", "error_code", "success",
            "http_status", "quantity", "cart_total_items", "cart_value",
            "duration_sec", "results_count", "clicked_position", "rating",
            "text_length", "cart_items", "dq_flags", "kafka_partition",
            "kafka_offset", "bronze_ingestion_timestamp",
            "silver_ingestion_timestamp",
        )
    )


# ---------------------------------------------------------------------
# dim_user / dim_session -- recomputed from the FACT table for only the
# entities touched this run, so reruns/backfills stay idempotent
# (values are derived fresh, never incremented).
# ---------------------------------------------------------------------
def recompute_dim_user(spark: SparkSession, fact_table: str, touched_user_ids: DataFrame) -> DataFrame:
    fact_df = spark.table(fact_table)
    return (
        fact_df
        .join(touched_user_ids, on="user_id", how="inner")
        .groupBy("user_id")
        .agg(
            F.min("event_timestamp").alias("first_seen_at"),
            F.max("event_timestamp").alias("last_seen_at"),
            F.count(F.lit(1)).alias("total_events_seen"),
        )
        .withColumn("user_key", F.col("user_id"))
        .withColumn("is_placeholder", F.lit(True))
        .withColumn("silver_updated_at", F.current_timestamp())
        .select(
            "user_key", "user_id", "first_seen_at", "last_seen_at",
            "total_events_seen", "is_placeholder", "silver_updated_at",
        )
    )


def recompute_dim_session(spark: SparkSession, fact_table: str, touched_session_ids: DataFrame) -> DataFrame:
    fact_df = spark.table(fact_table)
    return (
        fact_df
        .join(touched_session_ids, on="session_id", how="inner")
        .groupBy("session_id")
        .agg(
            F.first("user_id", ignorenulls=True).alias("user_id"),
            F.min("event_timestamp").alias("session_start_at"),
            F.max("event_timestamp").alias("session_end_at"),
            F.count(F.lit(1)).alias("event_count"),
            F.first("device", ignorenulls=True).alias("primary_device"),
        )
        .withColumn(
            "session_duration_sec",
            F.col("session_end_at").cast("long") - F.col("session_start_at").cast("long"),
        )
        .withColumn("session_key", F.col("session_id"))
        .withColumn("silver_updated_at", F.current_timestamp())
        .select(
            "session_key", "session_id", "user_id", "session_start_at",
            "session_end_at", "session_duration_sec", "primary_device",
            "event_count", "silver_updated_at",
        )
    )
