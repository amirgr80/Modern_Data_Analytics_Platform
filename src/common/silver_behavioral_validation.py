"""Validation and data-quality classification for Behavioral events."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import List, Mapping

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

from common.silver_behavioral_keys import (
    event_identity_source_expression,
    event_key_expression,
    has_complete_kafka_identity,
    quality_issue_key_expression,
    quality_record_key_expression,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationResult:
    """Explicit three-way validation split.

    ``processable_df`` is the union of strict-valid and warning records. It is
    the only validation output that should continue into cleaning and Kimball
    modelling.
    """

    valid_df: DataFrame
    warning_df: DataFrame
    rejected_df: DataFrame
    processable_df: DataFrame
    quality_issues_df: DataFrame


SOURCE_TABLE = "behavioral_events"

# Columns used by downstream builders. Missing optional columns are supplied
# as typed nulls so schema drift fails in one controlled place, not deep in a
# select list later.
OPTIONAL_COLUMN_TYPES: Mapping[str, str] = {
    "event_id": "string",
    "user_id": "string",
    "device_type": "string",
    "ip_address": "string",
    "utm_source": "string",
    "product_id": "string",
    "quantity": "int",
    "cart_total_items": "int",
    "cart_items": "array<struct<product_id:string,price:double,quantity:int>>",
    "cart_value": "double",
    "shipping_method": "string",
    "order_id": "string",
    "fulfillment_speed": "string",
    "url_path": "string",
    "duration_sec": "int",
    "http_status": "int",
    "payment_type": "string",
    "success": "boolean",
    "error_code": "string",
    "query": "string",
    "results_count": "int",
    "clicked_position": "int",
    "rating": "int",
    "text_length": "int",
    "wishlist_name": "string",
    "kafka_topic": "string",
    "kafka_partition": "int",
    "kafka_offset": "bigint",
    "kafka_timestamp": "timestamp",
    "bronze_ingestion_timestamp": "timestamp",
    "_source_file": "string",
    "processing_date": "date",
    "pipeline_run_id": "string",
}

STRUCTURAL_COLUMNS = ("session_id", "event_type")

KNOWN_DEVICE_TYPES = {
    "mobile",
    "desktop",
    "tablet",
    "web",
    "android",
    "ios",
    "unknown",
}

KNOWN_EVENT_TYPES = {
    "page_view",
    "product_view",
    "home_view",
    "category_view",
    "click",
    "search_product",
    "search",
    "add_to_cart",
    "remove_from_cart",
    "view_cart",
    "update_cart",
    "wishlist_add",
    "wishlist_remove",
    "checkout_start",
    "checkout_attempt",
    "payment_attempt",
    "purchase",
    "order_placed",
    "rating",
    "review",
    "product_rating",
    "error",
    "page_error",
}

# Bronze currently treats these as hard failures, while the Silver contract
# deliberately keeps anonymous users and unknown devices. Downgrade only these
# known upstream flags; all other Bronze errors remain rejection reasons.
DOWNGRADED_BRONZE_ERRORS = {
    "missing_event_id",
    "missing_user_id",
    "missing_device_type",
    "missing_utm_source",
    "invalid_ip_address",
}


def _empty_string_array() -> Column:
    # Build this lazily: Spark Columns require an active Spark context.
    return F.expr("cast(array() as array<string>)")


def _trimmed(column_name: str) -> Column:
    return F.trim(F.col(column_name).cast("string"))


def _nonblank(column_name: str) -> Column:
    return F.col(column_name).isNotNull() & (_trimmed(column_name) != "")


def align_behavioral_source_schema(df: DataFrame) -> DataFrame:
    aligned = df

    if "kafka_topic" not in aligned.columns and "source_topic" in aligned.columns:
        aligned = aligned.withColumn("kafka_topic", F.col("source_topic"))

    # Older/alternate Behavioral schemas use ``device`` while the project
    # contract uses ``device_type``. Keep both readable without losing the
    # original source field.
    if "device" in aligned.columns:
        if "device_type" in aligned.columns:
            aligned = aligned.withColumn(
                "device_type",
                F.coalesce(F.col("device_type"), F.col("device")),
            )
        else:
            aligned = aligned.withColumn("device_type", F.col("device"))

    for column_name in STRUCTURAL_COLUMNS:
        if column_name not in aligned.columns:
            aligned = aligned.withColumn(column_name, F.lit(None).cast("string"))

    if "event_timestamp" not in aligned.columns:
        if "timestamp" not in aligned.columns:
            aligned = aligned.withColumn("timestamp", F.lit(None).cast("string"))
        aligned = aligned.withColumn(
            "event_timestamp",
            F.coalesce(
                F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS"),
                F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"),
                F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"),
                F.to_timestamp("timestamp"),
            ),
        )
    else:
        aligned = aligned.withColumn(
            "event_timestamp", F.col("event_timestamp").cast("timestamp")
        )

    if "timestamp" not in aligned.columns:
        aligned = aligned.withColumn(
            "timestamp", F.col("event_timestamp").cast("string")
        )

    for column_name, sql_type in OPTIONAL_COLUMN_TYPES.items():
        if column_name not in aligned.columns:
            aligned = aligned.withColumn(column_name, F.lit(None).cast(sql_type))

    if "validation_errors" not in aligned.columns:
        aligned = aligned.withColumn("validation_errors", _empty_string_array())

    return (
        aligned.withColumn("event_key", event_key_expression(aligned)).withColumn(
            "event_identity_source",
            event_identity_source_expression(aligned),
        )
    )


def _build_original_record(df: DataFrame) -> Column:
    fields = []
    for field in df.schema.fields:
        if field.name in {"_validation_row_number", "_duplicate_count"}:
            continue
        value = F.col(field.name)
        if isinstance(field.dataType, BinaryType):
            value = F.base64(value)
        fields.append(value.alias(field.name))
    return F.to_json(F.struct(*fields))


def _bronze_errors(df: DataFrame) -> Column:
    raw = F.coalesce(
        F.col("validation_errors").cast("array<string>"),
        _empty_string_array(),
    )
    return F.filter(
        raw,
        lambda error: ~error.isin(*sorted(DOWNGRADED_BRONZE_ERRORS)),
    )


def _bronze_warnings(df: DataFrame) -> Column:
    raw = F.coalesce(
        F.col("validation_errors").cast("array<string>"),
        _empty_string_array(),
    )
    downgraded = F.filter(
        raw,
        lambda error: error.isin(*sorted(DOWNGRADED_BRONZE_ERRORS)),
    )
    return F.transform(
        downgraded,
        lambda error: F.concat(F.lit("bronze:"), error),
    )


def _critical_rule_columns(df: DataFrame) -> List[Column]:
    reliable_identity = _nonblank("event_id") | has_complete_kafka_identity(df)
    return [
        F.when(~reliable_identity, F.lit("event_identity:missing")),
        F.when(~_nonblank("session_id"), F.lit("session_id:required_value_missing")),
        F.when(~_nonblank("event_type"), F.lit("event_type:required_value_missing")),
        F.when(F.col("event_timestamp").isNull(), F.lit("event_timestamp:invalid")),
        F.when(F.col("_validation_row_number") > 1, F.lit("event_key:duplicate_event")),
    ]


def _valid_ip_address() -> Column:
    ipv4_pattern = (
        r"^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\."
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\."
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\."
        r"(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
    )
    # This intentionally accepts canonical and compressed IPv6 text without
    # attempting network resolution. It is a warning-only validation rule.
    ipv6_pattern = r"(?i)^(?:[0-9a-f]{1,4}:){1,7}[0-9a-f]{0,4}$"
    return F.col("ip_address").rlike(ipv4_pattern) | F.col("ip_address").rlike(
        ipv6_pattern
    )


def _has_complete_kafka_identity_columns() -> Column:
    # The aligned schema always contains these columns, so this avoids passing
    # a DataFrame through every warning helper.
    return (
        _nonblank("kafka_topic")
        & F.col("kafka_partition").isNotNull()
        & F.col("kafka_offset").isNotNull()
    )


def _warning_rule_columns() -> List[Column]:
    normalized_device = F.regexp_replace(
        F.lower(_trimmed("device_type")), r"[\s-]+", "_"
    )
    normalized_event = F.regexp_replace(
        F.lower(_trimmed("event_type")), r"[\s-]+", "_"
    )
    return [
        F.when(~_nonblank("user_id"), F.lit("user_id:anonymous_or_missing")),
        F.when(~_nonblank("utm_source"), F.lit("utm_source:missing")),
        F.when(
            ~_nonblank("device_type")
            | ~normalized_device.isin(*sorted(KNOWN_DEVICE_TYPES)),
            F.lit("device_type:unknown"),
        ),
        F.when(
            _nonblank("event_type")
            & ~normalized_event.isin(*sorted(KNOWN_EVENT_TYPES)),
            F.lit("event_type:unknown"),
        ),
        F.when(
            _nonblank("ip_address") & ~_valid_ip_address(),
            F.lit("ip_address:invalid_format"),
        ),
        F.when(
            ~_nonblank("event_id") & _has_complete_kafka_identity_columns(),
            F.lit("event_id:missing_used_kafka_identity"),
        ),
        F.when(F.col("cart_value") < 0, F.lit("cart_value:negative_value")),
        F.when(F.col("quantity") <= 0, F.lit("quantity:must_be_positive")),
        F.when(F.col("duration_sec") < 0, F.lit("duration_sec:negative_value")),
        F.when(
            (F.col("rating") < 1) | (F.col("rating") > 5),
            F.lit("rating:out_of_range"),
        ),
        F.when(F.col("results_count") < 0, F.lit("results_count:negative_value")),
        F.when(
            F.col("clicked_position") < 0,
            F.lit("clicked_position:negative_value"),
        ),
        F.when(F.col("text_length") < 0, F.lit("text_length:negative_value")),
        F.when(
            (F.col("http_status") < 100) | (F.col("http_status") > 599),
            F.lit("http_status:out_of_range"),
        ),
    ]


def _build_quality_issues(validated_df: DataFrame) -> DataFrame:
    """Return one idempotent row for each individual error or warning."""

    with_record = validated_df.withColumn(
        "original_record", _build_original_record(validated_df)
    ).withColumn("source_file", F.col("_source_file").cast("string"))
    with_record = with_record.withColumn(
        "record_key", quality_record_key_expression()
    )

    error_rows = (
        with_record.filter(F.size("validation_errors") > 0)
        .withColumn("issue_status", F.lit("REJECTED"))
        .withColumn("issue_code", F.explode("validation_errors"))
    )
    warning_rows = (
        with_record.filter(F.size("validation_warnings") > 0)
        .withColumn("issue_status", F.lit("WARNING"))
        .withColumn("issue_code", F.explode("validation_warnings"))
    )
    issues = error_rows.unionByName(warning_rows)
    now = F.current_timestamp()

    shaped = issues.select(
        F.lit(SOURCE_TABLE).alias("source_table"),
        "record_key",
        "issue_status",
        F.col("issue_code").cast("string").alias("issue_code"),
        F.col("validation_errors").cast("array<string>").alias(
            "validation_errors"
        ),
        F.col("validation_warnings").cast("array<string>").alias(
            "validation_warnings"
        ),
        "original_record",
        "source_file",
        F.col("kafka_topic").cast("string").alias("kafka_topic"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset").cast("bigint").alias("kafka_offset"),
        F.col("kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
        F.col("bronze_ingestion_timestamp").cast("timestamp").alias(
            "bronze_ingestion_timestamp"
        ),
        F.col("processing_date").cast("date").alias("processing_date"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        now.alias("first_detected_at"),
        now.alias("last_detected_at"),
    )
    return shaped.withColumn(
        "quality_issue_key", quality_issue_key_expression()
    ).select(
        "quality_issue_key",
        "source_table",
        "record_key",
        "issue_status",
        "issue_code",
        "validation_errors",
        "validation_warnings",
        "original_record",
        "source_file",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingestion_timestamp",
        "processing_date",
        "pipeline_run_id",
        "first_detected_at",
        "last_detected_at",
    )


def _drop_internal_columns(df: DataFrame, *, drop_errors: bool) -> DataFrame:
    columns = ["_validation_row_number", "_duplicate_count"]
    if drop_errors:
        columns.append("validation_errors")
    return df.drop(*columns)


def validate_behavioral_data(df: DataFrame) -> ValidationResult:
    aligned = align_behavioral_source_schema(df)

    # Stable ordering makes the surviving row deterministic when the same
    # event appears multiple times in one Bronze batch.
    ordering = [
        F.col("bronze_ingestion_timestamp").asc_nulls_last(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_topic").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
    ]
    duplicate_window = Window.partitionBy("event_key").orderBy(*ordering)
    count_window = Window.partitionBy("event_key")

    prepared = aligned.withColumn(
        "_validation_row_number", F.row_number().over(duplicate_window)
    ).withColumn("_duplicate_count", F.count(F.lit(1)).over(count_window))

    errors = F.array_distinct(
        F.concat(
            _bronze_errors(prepared),
            F.array_compact(F.array(*_critical_rule_columns(prepared))),
        )
    )
    warnings = F.array_distinct(
        F.concat(
            _bronze_warnings(prepared),
            F.array_compact(F.array(*_warning_rule_columns())),
        )
    )

    validated = prepared.withColumn("validation_errors", errors).withColumn(
        "validation_warnings", warnings
    )

    rejected_internal = validated.filter(F.size("validation_errors") > 0)
    processable_internal = validated.filter(F.size("validation_errors") == 0)
    valid_internal = processable_internal.filter(F.size("validation_warnings") == 0)
    warning_internal = processable_internal.filter(F.size("validation_warnings") > 0)

    valid = _drop_internal_columns(valid_internal, drop_errors=True)
    warning = _drop_internal_columns(warning_internal, drop_errors=True)
    processable = _drop_internal_columns(processable_internal, drop_errors=True)
    rejected = _drop_internal_columns(rejected_internal, drop_errors=False)
    quality = _build_quality_issues(validated)

    return ValidationResult(
        valid_df=valid,
        warning_df=warning,
        rejected_df=rejected,
        processable_df=processable,
        quality_issues_df=quality,
    )
