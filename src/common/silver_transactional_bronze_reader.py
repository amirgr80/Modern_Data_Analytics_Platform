import logging
import sys
from typing import List, Optional, Sequence, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name


sys.path.append("/opt/spark/conf")

from silver_transactional_config import (
    BRONZE_TRANSACTIONAL_BASE_PATH,
    SUPPORTED_TRANSACTIONAL_TABLES,
)


logger = logging.getLogger(__name__)


def validate_table_name(table_name: str) -> None:
    """
    Validate that the requested transactional table
    is supported by the Silver pipeline.
    """

    if table_name not in SUPPORTED_TRANSACTIONAL_TABLES:
        raise ValueError(
            "Unsupported transactional table '{}'. "
            "Supported tables: {}".format(
                table_name,
                sorted(SUPPORTED_TRANSACTIONAL_TABLES),
            )
        )


def build_bronze_table_path(
    table_name: str,
    partition_dates: Optional[Sequence[str]] = None,
) -> Union[str, List[str]]:
    """
    Build the Bronze MinIO path for one transactional table.

    Without partition_dates:
        s3a://bronze/transactional/orders

    With partition_dates:
        [
            s3a://bronze/transactional/orders/20260714,
            s3a://bronze/transactional/orders/20260715
        ]
    """

    validate_table_name(table_name)

    table_path = "{}/{}".format(
        BRONZE_TRANSACTIONAL_BASE_PATH.rstrip("/"),
        table_name,
    )

    if not partition_dates:
        return table_path

    paths = []  # type: List[str]

    for partition_date in partition_dates:
        normalized_date = partition_date.strip()

        if (
            len(normalized_date) != 8
            or not normalized_date.isdigit()
        ):
            raise ValueError(
                "Bronze partition date must use yyyyMMdd format. "
                "Received: '{}'.".format(partition_date)
            )

        paths.append(
            "{}/{}".format(
                table_path,
                normalized_date,
            )
        )

    return paths


def read_bronze_transactional_table(
    spark: SparkSession,
    table_name: str,
    partition_dates: Optional[Sequence[str]] = None,
) -> DataFrame:
    """
    Read Bronze Parquet files from MinIO.

    This function only reads the Bronze data and adds
    the physical source file path. Validation and cleaning
    are handled in separate Silver modules.
    """

    bronze_paths = build_bronze_table_path(
        table_name=table_name,
        partition_dates=partition_dates,
    )

    logger.info(
        "Reading Bronze table '%s' from paths: %s",
        table_name,
        bronze_paths,
    )

    reader = (
        spark.read
        .format("parquet")
        .option("recursiveFileLookup", "true")
    )

    dataframe = (
        reader
        .load(bronze_paths)
        .withColumn(
            "_source_file",
            input_file_name(),
        )
    )

    logger.info(
        "Bronze table '%s' loaded successfully.",
        table_name,
    )

    logger.info(
        "Loaded columns for table '%s': %s",
        table_name,
        dataframe.columns,
    )

    return dataframe