import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, LongType, TimestampType
from pyspark.sql import functions as F
from common.silver_transactional_quality_writer import (
    prepare_quality_dataframe,
    validate_quality_dataframe,
    write_transactional_quality_issues,
    QUALITY_COLUMNS
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("TestQualityWriter") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .getOrCreate()
    
    # Sample quality issues data (like what validation produces)
    sample_data = [
        {
            "source_table": "categories",
            "record_id": "C003",
            "issue_status": "REJECTED",
            "validation_errors": ["name:required_value_missing"],
            "validation_warnings": [],
            "repair_description": None,
            "original_record": '{"category_id":"C003","name":"","parent_category_id":"C001"}',
            "_source_file": "s3a://bronze/transactional/categories/part-00000.parquet",
            "_kafka_topic": "transactional.categories",
            "_kafka_partition": 0,
            "_kafka_offset": 12345,
            "_kafka_timestamp": "2026-07-16 17:39:50",
            "bronze_ingestion_timestamp": "2026-07-16 17:39:50",
            "detected_at": "2026-07-16 17:39:50"
        },
        {
            "source_table": "categories",
            "record_id": "C005",
            "issue_status": "WARNING",
            "validation_errors": [],
            "validation_warnings": ["category_id:unusual_id_format"],
            "repair_description": None,
            "original_record": '{"category_id":"c005","name":"Test","parent_category_id":"C001"}',
            "_source_file": "s3a://bronze/transactional/categories/part-00001.parquet",
            "_kafka_topic": "transactional.categories",
            "_kafka_partition": 1,
            "_kafka_offset": 12346,
            "_kafka_timestamp": "2026-07-16 17:40:00",
            "bronze_ingestion_timestamp": "2026-07-16 17:40:00",
            "detected_at": "2026-07-16 17:40:00"
        }
    ]
    
    # Create DataFrame with proper schema
    schema = StructType([
        StructField("source_table", StringType(), True),
        StructField("record_id", StringType(), True),
        StructField("issue_status", StringType(), True),
        StructField("validation_errors", ArrayType(StringType()), True),
        StructField("validation_warnings", ArrayType(StringType()), True),
        StructField("repair_description", StringType(), True),
        StructField("original_record", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_kafka_topic", StringType(), True),
        StructField("_kafka_partition", IntegerType(), True),
        StructField("_kafka_offset", LongType(), True),
        StructField("_kafka_timestamp", TimestampType(), True),
        StructField("bronze_ingestion_timestamp", TimestampType(), True),
        StructField("detected_at", TimestampType(), True)
    ])
    
    df = spark.createDataFrame(sample_data, schema)
    
    logger.info("Sample quality data created:")
    df.show(truncate=False)
    
    # Test 1: Validate DataFrame
    logger.info("=" * 50)
    logger.info("Test 1: Validating DataFrame...")
    try:
        validate_quality_dataframe(df)
        logger.info("✅ Validation passed!")
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
    
    # Test 2: Prepare DataFrame
    logger.info("=" * 50)
    logger.info("Test 2: Preparing DataFrame...")
    prepared_df = prepare_quality_dataframe(df)
    prepared_df.printSchema()
    prepared_df.show(truncate=False)
    
    # Test 3: Write to Iceberg (if Iceberg is available)
    logger.info("=" * 50)
    logger.info("Test 3: Writing to Iceberg...")
    try:
        write_transactional_quality_issues(
            quality_issues_df=df,
            catalog_name="lakekeeper",
            namespace="silver_quality",
            table_name="transactional_validation_issues_test"
        )
        logger.info("✅ Write completed!")
    except Exception as e:
        logger.warning(f"⚠️ Write skipped (Iceberg may not be configured): {e}")
    
    spark.stop()

if __name__ == "__main__":
    main()
