import logging
import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import input_file_name

logger = logging.getLogger(__name__)

BRONZE_TRANSACTIONAL_BASE_PATH = os.getenv('BRONZE_TRANSACTIONAL_BASE_PATH', 's3a://bronze/transactional')
SUPPORTED_TRANSACTIONAL_TABLES = ['categories','users','products','orders','order_items','product_price_history']

def get_available_partitions(spark: SparkSession, table_path: str) -> list:
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path = spark._jvm.org.apache.hadoop.fs.Path(table_path)
        if not fs.exists(path):
            return []
        statuses = fs.listStatus(path)
        partitions = []
        for status in statuses:
            if status.isDirectory():
                name = status.getPath().getName()
                if name.isdigit() and len(name) == 8:
                    partitions.append(name)
        return sorted(partitions)
    except Exception as e:
        logger.warning(f'Could not list partitions: {e}')
        return []

def read_bronze_transactional_table(
    spark: SparkSession,
    table_name: str,
    partition_dates: list = None,
) -> DataFrame:
    base_path = f'{BRONZE_TRANSACTIONAL_BASE_PATH}/{table_name}'
    
    if partition_dates is None:
        partition_dates = get_available_partitions(spark, base_path)
    
    if partition_dates:
        paths = [f'{base_path}/{d}' for d in partition_dates]
        logger.info(f'Reading from partitioned paths: {paths}')
    else:
        paths = base_path
        logger.info(f'Reading from base path: {paths}')
    
    df = spark.read.format('parquet').option('recursiveFileLookup', 'true').load(paths)
    return df.withColumn('_source_file', input_file_name())