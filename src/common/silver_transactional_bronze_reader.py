from pyspark.sql import DataFrame, SparkSession


def build_bronze_table_path(
    base_path: str,
    table_name: str,
) -> str:
    # build minio path
    return f"{base_path.rstrip('/')}/{table_name}"


def read_bronze_table(
    spark: SparkSession,
    base_path: str,
    table_name: str,
) -> DataFrame:

    #Read Parquet partitions

    table_path = build_bronze_table_path(
        base_path=base_path,
        table_name=table_name,
    )

    return (
        spark.read
        .format("parquet")
        # find the file inside buckets in minio => recursive
        .option("recursiveFileLookup", "true")
        .parquet(table_path)
    )