"""Idempotent writers for Behavioral quality issues and quarantine rows."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_iceberg_writer import MergeStrategy, merge_dataframe
from common.silver_behavioral_schema import TABLE_QUALITY, TABLE_QUARANTINE


logger = logging.getLogger(__name__)

QUALITY_COLUMNS = (
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


def write_behavioral_quality_issues(
    spark: SparkSession,
    quality_issues_df: DataFrame,
    config: BehavioralRuntimeConfig,
) -> int:
    table = config.qualified_table(TABLE_QUALITY, config.quality_namespace)
    prepared = quality_issues_df.select(*QUALITY_COLUMNS)
    written = merge_dataframe(
        spark,
        table,
        prepared,
        merge_keys=("quality_issue_key",),
        strategy=MergeStrategy.UPSERT_PRESERVE_BOUNDS,
        protected_columns=("quality_issue_key",),
        min_columns=("first_detected_at",),
        max_columns=("last_detected_at",),
    )
    logger.info("Quality issue state merged: %s rows", written)
    return written


def build_quarantine_rows(rejected_df: DataFrame) -> DataFrame:
    source_device = "device" if "device" in rejected_df.columns else "device_type"
    now = F.current_timestamp()
    ordering = [
        F.col("bronze_ingestion_timestamp").asc_nulls_last(),
        F.col("kafka_timestamp").asc_nulls_last(),
        F.col("kafka_topic").asc_nulls_last(),
        F.col("kafka_partition").asc_nulls_last(),
        F.col("kafka_offset").asc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
    ]
    window = Window.partitionBy("event_key").orderBy(*ordering)
    representative_rows = (
        rejected_df
        .withColumn("_quarantine_row_number", F.row_number().over(window))
        .filter(F.col("_quarantine_row_number") == 1)
        .drop("_quarantine_row_number")
    )
    return (
        representative_rows
        .select(
            "event_key",
            F.col("event_id").cast("string").alias("event_id"),
            "event_identity_source",
            F.col("validation_errors").cast("array<string>").alias(
                "validation_errors"
            ),
            F.col("validation_warnings").cast("array<string>").alias(
                "validation_warnings"
            ),
            F.col("user_id").cast("string").alias("raw_user_id"),
            F.col("session_id").cast("string").alias("raw_session_id"),
            F.col("event_type").cast("string").alias("raw_event_type"),
            F.col("timestamp").cast("string").alias("raw_timestamp"),
            F.col(source_device).cast("string").alias("raw_device"),
            F.col("kafka_topic").cast("string").alias("kafka_topic"),
            F.col("kafka_partition").cast("int").alias("kafka_partition"),
            F.col("kafka_offset").cast("bigint").alias("kafka_offset"),
            F.col("kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
            F.col("bronze_ingestion_timestamp").cast("timestamp").alias(
                "bronze_ingestion_timestamp"
            ),
            F.col("_source_file").cast("string").alias("source_file"),
            F.col("processing_date").cast("date").alias("processing_date"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
            now.alias("first_quarantined_at"),
            now.alias("last_quarantined_at"),
        )
    )


def write_behavioral_quarantine(
    spark: SparkSession,
    rejected_df: DataFrame,
    config: BehavioralRuntimeConfig,
) -> int:
    table = config.qualified_table(TABLE_QUARANTINE)
    rows = build_quarantine_rows(rejected_df)
    written = merge_dataframe(
        spark,
        table,
        rows,
        merge_keys=("event_key",),
        strategy=MergeStrategy.UPSERT_PRESERVE_BOUNDS,
        protected_columns=("event_key",),
        min_columns=("first_quarantined_at",),
        max_columns=("last_quarantined_at",),
    )
    logger.info("Quarantine state merged: %s rows", written)
    return written
