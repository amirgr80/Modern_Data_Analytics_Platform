import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from configs.silver_transactional_config import (
    BRONZE_TRANSACTIONAL_BASE_PATH,
    SUPPORTED_TRANSACTIONAL_TABLES,
)


logger = logging.getLogger(__name__)


def validate_table_name(table_name: str) -> None:
    if table_name not in SUPPORTED_TRANSACTIONAL_TABLES:
        raise ValueError(
            f"Unsupported transactional table '{table_name}'. "
            f"Supported tables: "
            f"{sorted(SUPPORTED_TRANSACTIONAL_TABLES)}"
        )


def build_bronze_table_path(
    table_name: str,
    partition_dates: Sequence[str] | None = None,
) -> str | list[str]:
    validate_table_name(table_name)

    table_path = (
        f"{BRONZE_TRANSACTIONAL_BASE_PATH}/{table_name}"
    )

    if not partition_dates:
        return table_path

    paths: list[str] = []

    for partition_date in partition_dates:
        normalized_date = partition_date.strip()

        if (
            len(normalized_date) != 8
            or not normalized_date.isdigit()
        ):
            raise ValueError(
                "Bronze partition date must use yyyyMMdd format. "
                f"Received: '{partition_date}'."
            )

        paths.append(
            f"{table_path}/{normalized_date}"
        )

    return paths


def read_bronze_transactional_table(
    spark: SparkSession,
    table_name: str,
    partition_dates: Sequence[str] | None = None,
) -> DataFrame:
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

    dataframe = reader.load(bronze_paths)

    logger.info(
        "Bronze table '%s' loaded with columns: %s",
        table_name,
        dataframe.columns,
    )

    return dataframe