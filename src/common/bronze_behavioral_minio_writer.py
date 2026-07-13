import os

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_bronze_stream_to_parquet(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
    coalesce_partitions: int = None,
    max_records_per_file: int = None,
) -> StreamingQuery:
    """
    Write a Bronze streaming DataFrame as Parquet files.

    This writer is designed for MinIO/S3A paths.

    Example output path:
        s3a://bronze/behavioral/events

    Example checkpoint path:
        s3a://be-checkpoints/behavioral/events

    Partition columns:
        year, month, day

    These columns must already exist in the input DataFrame.

    Writing every micro-batch directly with a short trigger interval tends
    to produce many small Parquet files (thousands per day at low volume),
    which slows down downstream Silver reads. To keep files closer to a
    healthy size:
    - each micro-batch is coalesced down to a small, fixed number of
      output partitions before writing (coalesce, not repartition, since
      it avoids an extra shuffle), and
    - maxRecordsPerFile bounds how large a single file can grow, as a
      backstop for high-volume batches.
    Both are tunable via env vars since the right values depend on actual
    throughput, which should be confirmed with a load test.
    """

    required_partition_columns = {"year", "month", "day"}
    missing_columns = required_partition_columns.difference(set(df.columns))

    if missing_columns:
        raise ValueError(
            "Cannot write Bronze Parquet stream. "
            f"Missing partition columns: {sorted(missing_columns)}"
        )

    coalesce_partitions = coalesce_partitions or int(
        os.getenv("BRONZE_WRITE_COALESCE_PARTITIONS", "2")
    )
    max_records_per_file = max_records_per_file or int(
        os.getenv("BRONZE_WRITE_MAX_RECORDS_PER_FILE", "500000")
    )

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        (
            batch_df
            .coalesce(coalesce_partitions)
            .write
            .mode("append")
            .option("compression", "snappy")
            .option("maxRecordsPerFile", max_records_per_file)
            .partitionBy("year", "month", "day")
            .parquet(output_path)
        )

    return (
        df.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
