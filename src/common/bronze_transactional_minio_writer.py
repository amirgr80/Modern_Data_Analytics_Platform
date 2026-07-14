import logging
import os
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.streaming import StreamingQuery


logger = logging.getLogger(__name__)


def get_required_env(variable_name: str) -> str:
    value = os.getenv(variable_name)

    if value is None or not value.strip():
        raise ValueError(
            f"Required environment variable '{variable_name}' is not set."
        )

    return value.strip()


def create_microbatch_writer(
    table_name: str,
    output_base_path: str,
) -> Callable[[DataFrame, int], None]:
    """
    Create a foreachBatch writer for one transactional table.

    Each micro-batch is split by partition_date and written to:

        bronze/transactional/<table_name>/<yyyyMMdd>/
    """

    def write_microbatch(
        batch_df: DataFrame,
        batch_id: int,
    ) -> None:

        if batch_df.isEmpty():
            logger.info(
                "Micro-batch %s for table '%s' is empty.",
                batch_id,
                table_name,
            )
            return

        logger.info(
            "Batch %s row count: %s",
            batch_id,
            batch_df.count(),
        )

        logger.info(
            "Batch %s columns: %s",
            batch_id,
            batch_df.columns,
        )

        batch_df.select(
            "partition_date"
        ).show(
            10,
            False,
        )

        logger.info(
            "Writing micro-batch %s for table '%s'.",
            batch_id,
            table_name,
        )

        batch_df = batch_df.cache()

        try:
            partition_dates = (
                batch_df
                .select("partition_date")
                .where(
                    col("partition_date").isNotNull()
                )
                .distinct()
                .collect()
            )

            for row in partition_dates:

                partition_date = row["partition_date"]

                output_path = (
                    f"{output_base_path}/"
                    f"{table_name}/"
                    f"{partition_date}"
                )

                logger.info(
                    "Writing table '%s' to '%s'.",
                    table_name,
                    output_path,
                )

                (
                    batch_df
                    .filter(
                        col("partition_date") == partition_date
                    )
                    .write
                    .mode("append")
                    .format("parquet")
                    .option(
                        "compression",
                        "snappy",
                    )
                    .save(output_path)
                )

        finally:
            batch_df.unpersist()

    return write_microbatch


def write_transactional_stream(
    dataframe: DataFrame,
    table_name: str,
) -> StreamingQuery:
    """
    Start one streaming writer for a transactional topic.
    """

    bucket_name = "bronze"

    trigger_interval = os.getenv(
        "BRONZE_TRIGGER_INTERVAL",
        "1 minute",
    )

    output_base_path = (
        f"s3a://{bucket_name}/transactional"
    )

    checkpoint_path = f"s3a://tr-checkpoints/transactional/{table_name}"

    microbatch_writer = create_microbatch_writer(
        table_name=table_name,
        output_base_path=output_base_path,
    )

    logger.info(
        "Starting MinIO writer for table '%s'.",
        table_name,
    )

    return (
        dataframe
        .writeStream
        .foreachBatch(
            microbatch_writer
        )
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .trigger(
            processingTime=trigger_interval,
        )
        .queryName(
            f"bronze_transactional_{table_name}"
        )
        .start()
    )
