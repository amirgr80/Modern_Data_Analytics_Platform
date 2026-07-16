import logging
import os
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


QUALITY_COLUMNS = [
    "source_table",
    "record_id",
    "issue_status",
    "validation_errors",
    "validation_warnings",
    "repair_description",
    "original_record",
    "_source_file",
    "_kafka_topic",
    "_kafka_partition",
    "_kafka_offset",
    "_kafka_timestamp",
    "bronze_ingestion_timestamp",
    "detected_at",
]


def validate_quality_dataframe(
    quality_issues_df: DataFrame,
) -> None:
    """
    Ensure that the DataFrame produced by Validation contains
    all columns required by the Iceberg quality table.
    """

    missing_columns = [
        column_name
        for column_name in QUALITY_COLUMNS
        if column_name not in quality_issues_df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Quality DataFrame is missing required columns: "
            f"{missing_columns}"
        )


def prepare_quality_dataframe(
    quality_issues_df: DataFrame,
) -> DataFrame:
    """
    Select and cast columns to the exact schema expected
    by the Iceberg quality table.

    This prevents append errors caused by minor type differences.
    """

    validate_quality_dataframe(quality_issues_df)

    return quality_issues_df.select(
        F.col("source_table").cast("string").alias(
            "source_table"
        ),
        F.col("record_id").cast("string").alias(
            "record_id"
        ),
        F.col("issue_status").cast("string").alias(
            "issue_status"
        ),
        F.col("validation_errors")
        .cast("array<string>")
        .alias("validation_errors"),
        F.col("validation_warnings")
        .cast("array<string>")
        .alias("validation_warnings"),
        F.col("repair_description").cast("string").alias(
            "repair_description"
        ),
        F.col("original_record").cast("string").alias(
            "original_record"
        ),
        #writes the source address of parq file
        F.col("_source_file").cast("string").alias(
            "_source_file"
        ),
        F.col("_kafka_topic").cast("string").alias(
            "_kafka_topic"
        ),
        F.col("_kafka_partition").cast("int").alias(
            "_kafka_partition"
        ),
        F.col("_kafka_offset").cast("bigint").alias(
            "_kafka_offset"
        ),
        F.col("_kafka_timestamp").cast("timestamp").alias(
            "_kafka_timestamp"
        ),
        F.col("bronze_ingestion_timestamp")
        .cast("timestamp")
        .alias("bronze_ingestion_timestamp"),
        F.col("detected_at").cast("timestamp").alias(
            "detected_at"
        ),
    )


def create_quality_table_if_not_exists(
    quality_issues_df: DataFrame,
    catalog_name: str,
    namespace: str,
    table_name: str,
) -> str:

    # Create the Iceberg namespace and quality table when they do not already exist.


    spark = quality_issues_df.sparkSession

    full_table_name = (
        f"{catalog_name}.{namespace}.{table_name}"
    )

    spark.sql(
        f"""
        CREATE NAMESPACE IF NOT EXISTS
        {catalog_name}.{namespace}
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            source_table STRING,
            record_id STRING,
            issue_status STRING,
            validation_errors ARRAY<STRING>,
            validation_warnings ARRAY<STRING>,
            repair_description STRING,
            original_record STRING,
            _source_file STRING,
            _kafka_topic STRING,
            _kafka_partition INT,
            _kafka_offset BIGINT,
            _kafka_timestamp TIMESTAMP,
            bronze_ingestion_timestamp TIMESTAMP,
            detected_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (
            source_table,
            days(detected_at)
        )
        """
    )

    return full_table_name


def write_transactional_quality_issues(
    quality_issues_df: DataFrame,
    catalog_name: Optional[str] = None,
    namespace: Optional[str] = None,
    table_name: Optional[str] = None,
) -> None:
    """
    Append transactional validation issues to Iceberg.

    Records with these statuses are stored:

        REJECTED
            The record cannot continue to Cleaning.

        WARNING
            The record can continue, but has a quality issue.

        REPAIRED
            The record was repaired and can continue.

    Default destination:

        lakekeeper.silver_quality.transactional_validation_issues
    """

    catalog_name = catalog_name or os.getenv(
        "ICEBERG_CATALOG_NAME",
        "lakekeeper",
    )

    namespace = namespace or os.getenv(
        "SILVER_QUALITY_NAMESPACE",
        "silver_quality",
    )

    table_name = table_name or os.getenv(
        "TRANSACTIONAL_QUALITY_TABLE",
        "transactional_validation_issues",
    )

    if quality_issues_df.isEmpty():
        logger.info(
            "No transactional validation issues found. "
            "Nothing will be written."
        )
        return

    prepared_df = prepare_quality_dataframe(
        quality_issues_df
    )

    full_table_name = create_quality_table_if_not_exists(
        quality_issues_df=prepared_df,
        catalog_name=catalog_name,
        namespace=namespace,
        table_name=table_name,
    )

    (
        prepared_df
        .writeTo(full_table_name)
        .append()
    )

    logger.info(
        "Transactional quality issues written to '%s'.",
        full_table_name,
    )