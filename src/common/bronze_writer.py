from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_bronze_stream_to_parquet(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
) -> StreamingQuery:
    """
    Write a Bronze streaming DataFrame as Parquet files.

    This writer is designed for MinIO/S3A paths.

    Example output path:
        s3a://bronze/behavioral/events

    Example checkpoint path:
        s3a://bronze/checkpoints/behavioral/events

    Partition columns:
        year, month, day

    These columns must already exist in the input DataFrame.
    """

    required_partition_columns = {"year", "month", "day"}
    missing_columns = required_partition_columns.difference(set(df.columns))

    if missing_columns:
        raise ValueError(
            "Cannot write Bronze Parquet stream. "
            f"Missing partition columns: {sorted(missing_columns)}"
        )

    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("year", "month", "day")
        .trigger(processingTime=trigger_interval)
        .start()
    )