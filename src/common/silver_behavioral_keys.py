"""Deterministic key expressions for Behavioral facts, dimensions, and DQ state."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def nonblank(column_name: str) -> Column:
    return F.col(column_name).isNotNull() & (
        F.trim(F.col(column_name).cast("string")) != ""
    )


def has_complete_kafka_identity(df: DataFrame) -> Column:
    required = ("kafka_topic", "kafka_partition", "kafka_offset")
    if any(name not in df.columns for name in required):
        return F.lit(False)
    return (
        nonblank("kafka_topic")
        & F.col("kafka_partition").isNotNull()
        & F.col("kafka_offset").isNotNull()
    )


def event_key_expression(df: DataFrame) -> Column:
    """Create a stable, namespaced SHA-256 event key.

    Priority is producer/Bronze ``event_id``, then the complete Kafka
    coordinate. A stable-field fallback is retained for quarantine and
    migration visibility, while validation rejects normal fact records that
    have neither reliable identity.
    """

    event_id_present = (
        nonblank("event_id") if "event_id" in df.columns else F.lit(False)
    )
    kafka_present = has_complete_kafka_identity(df)

    id_material = F.concat(
        F.lit("event_id|"),
        F.trim(F.col("event_id").cast("string")),
    )
    kafka_material = F.concat_ws(
        "|",
        F.lit("kafka"),
        F.trim(F.col("kafka_topic").cast("string")),
        F.col("kafka_partition").cast("string"),
        F.col("kafka_offset").cast("string"),
    )

    fallback_columns = [
        name
        for name in (
            "session_id",
            "event_type",
            "timestamp",
            "event_timestamp",
            "user_id",
            "product_id",
            "order_id",
        )
        if name in df.columns
    ]
    fallback_parts = [F.lit("fallback")] + [
        F.coalesce(F.col(name).cast("string"), F.lit(""))
        for name in fallback_columns
    ]
    fallback_material = F.concat_ws("|", *fallback_parts)

    material = (
        F.when(event_id_present, id_material)
        .when(kafka_present, kafka_material)
        .otherwise(fallback_material)
    )
    return F.sha2(material, 256)


def event_identity_source_expression(df: DataFrame) -> Column:
    event_id_present = (
        nonblank("event_id") if "event_id" in df.columns else F.lit(False)
    )
    return (
        F.when(event_id_present, F.lit("event_id"))
        .when(has_complete_kafka_identity(df), F.lit("kafka_coordinate"))
        .otherwise(F.lit("fallback_fields"))
    )


def natural_key_hash(
    prefix: str,
    column_name: str,
    *,
    case_sensitive: bool = True,
) -> Column:
    value = F.trim(F.col(column_name).cast("string"))
    if not case_sensitive:
        value = F.lower(value)
    return F.when(
        nonblank(column_name),
        F.sha2(
            F.concat(F.lit(f"{prefix}|"), value),
            256,
        ),
    ).otherwise(F.lit(None).cast("string"))


def quality_record_key_expression() -> Column:
    """Build a retry-stable identity for a DQ record.

    A rejected event may not have a trustworthy fact key. Including source
    coordinates, source file, and the serialized original record avoids
    collapsing unrelated malformed rows while remaining stable on retries.
    """

    return F.sha2(
        F.concat_ws(
            "|",
            F.lit("quality_record"),
            F.coalesce(F.col("event_key").cast("string"), F.lit("")),
            F.coalesce(F.col("kafka_topic").cast("string"), F.lit("")),
            F.coalesce(F.col("kafka_partition").cast("string"), F.lit("")),
            F.coalesce(F.col("kafka_offset").cast("string"), F.lit("")),
            F.coalesce(F.col("source_file").cast("string"), F.lit("")),
            F.coalesce(F.col("original_record").cast("string"), F.lit("")),
        ),
        256,
    )


def quality_issue_key_expression() -> Column:
    """Build one deterministic key per record and individual DQ issue."""

    return F.sha2(
        F.concat_ws(
            "|",
            F.lit("quality_issue"),
            F.coalesce(F.col("source_table"), F.lit("")),
            F.coalesce(F.col("record_key"), F.lit("")),
            F.coalesce(F.col("issue_status"), F.lit("")),
            F.coalesce(F.col("issue_code"), F.lit("")),
        ),
        256,
    )
