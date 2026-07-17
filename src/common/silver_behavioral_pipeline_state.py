"""Retry-safe current state for each Behavioral processing date."""

from __future__ import annotations

from datetime import date
import hashlib
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.silver_behavioral_config import BehavioralRuntimeConfig
from common.silver_behavioral_iceberg_writer import MergeStrategy, merge_dataframe
from common.silver_behavioral_schema import TABLE_PIPELINE_STATE


PIPELINE_NAME = "silver_behavioral"


def run_key_for(execution_date: date) -> str:
    material = f"{PIPELINE_NAME}|{execution_date.isoformat()}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def build_pipeline_state_row(
    spark: SparkSession,
    execution_date: date,
    status: str,
    *,
    raw_count: Optional[int] = None,
    valid_count: Optional[int] = None,
    rejected_count: Optional[int] = None,
    warning_count: Optional[int] = None,
    processable_count: Optional[int] = None,
    fact_rows_merged: Optional[int] = None,
    error_message: Optional[str] = None,
):
    now = F.current_timestamp()
    completed = now if status in {"SUCCEEDED", "FAILED", "EMPTY"} else F.lit(None).cast("timestamp")
    return (
        spark.range(1)
        .select(
            F.lit(run_key_for(execution_date)).alias("run_key"),
            F.lit(PIPELINE_NAME).alias("pipeline_name"),
            F.lit(execution_date).cast("date").alias("execution_date"),
            F.lit(status).alias("status"),
            now.alias("first_started_at"),
            now.alias("last_started_at"),
            completed.alias("completed_at"),
            F.lit(raw_count).cast("bigint").alias("raw_count"),
            F.lit(valid_count).cast("bigint").alias("valid_count"),
            F.lit(warning_count).cast("bigint").alias("warning_count"),
            F.lit(processable_count).cast("bigint").alias("processable_count"),
            F.lit(rejected_count).cast("bigint").alias("rejected_count"),
            F.lit(fact_rows_merged).cast("bigint").alias("fact_rows_merged"),
            F.lit(error_message).cast("string").alias("error_message"),
            now.alias("updated_at"),
        )
    )


def write_pipeline_state(
    spark: SparkSession,
    config: BehavioralRuntimeConfig,
    execution_date: date,
    status: str,
    **metrics,
) -> int:
    table = config.qualified_table(TABLE_PIPELINE_STATE)
    row = build_pipeline_state_row(
        spark,
        execution_date,
        status,
        **metrics,
    )
    return merge_dataframe(
        spark,
        table,
        row,
        merge_keys=("run_key",),
        strategy=MergeStrategy.UPSERT_PRESERVE_BOUNDS,
        protected_columns=("run_key", "pipeline_name", "execution_date"),
        min_columns=("first_started_at",),
        max_columns=("last_started_at",),
    )
