"""Read one partition of Bronze Behavioral Parquet with lineage metadata."""

from __future__ import annotations

from datetime import date
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.silver_behavioral_config import BehavioralRuntimeConfig


logger = logging.getLogger(__name__)


def build_partition_path(base_path: str, execution_date: date) -> str:
    return (
        f"{base_path.rstrip('/')}/year={execution_date.year}"
        f"/month={execution_date.month}/day={execution_date.day}"
    )


def read_bronze_behavioral_partition(
    spark: SparkSession,
    execution_date: date,
    pipeline_run_id: str,
    config: Optional[BehavioralRuntimeConfig] = None,
) -> DataFrame:
    cfg = config or BehavioralRuntimeConfig.from_env()
    partition_path = build_partition_path(cfg.bronze_path, execution_date)
    logger.info("Reading Bronze Behavioral partition: %s", partition_path)

    dataframe = (
        spark.read.format("parquet")
        .option("basePath", cfg.bronze_path)
        .load(partition_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("processing_date", F.lit(execution_date).cast("date"))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
    )

    # Bronze currently emits kafka_topic.  source_topic is kept as a fallback
    # for backward-compatible partitions created by older jobs.
    if "kafka_topic" not in dataframe.columns and "source_topic" in dataframe.columns:
        dataframe = dataframe.withColumn("kafka_topic", F.col("source_topic"))

    logger.info("Bronze Behavioral columns: %s", dataframe.columns)
    return dataframe
