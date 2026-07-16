import logging
import os
from pyspark.sql import SparkSession
from common.silver_transactional_bronze_reader import read_bronze_data
from common.silver_transactional_validation import validate_transactional_data
from common.silver_transactional_cleaning import clean_transactional_data
from common.silver_transactional_quality_writer import write_transactional_quality_issues
from common.silver_transactional_spark_session import create_iceberg_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Create Spark Session with Iceberg
    spark = create_iceberg_spark_session("SilverTransactionalJob")
    
    # List of tables to process
    tables = ['categories', 'users', 'products', 'orders', 'order_items', 'product_price_history']
    
    # Minio bucket for silver
    silver_bucket = os.getenv("MINIO_BUCKET_SILVER", "silver")
    bronze_bucket = os.getenv("MINIO_BUCKET", "bronze")
    
    for table in tables:
        logger.info(f"=" * 50)
        logger.info(f"Processing table: {table}")
        
        try:
            # 1. Read from Bronze (Minio)
            bronze_path = f"s3a://{bronze_bucket}/transactional/{table}/"
            logger.info(f"Reading from: {bronze_path}")
            df = spark.read.parquet(bronze_path)
            logger.info(f"Read {df.count()} records from bronze for {table}")
            
            # 2. Validation
            logger.info(f"Validating table: {table}")
            result = validate_transactional_data(df, table)
            logger.info(f"Valid: {result.valid_df.count()}, Rejected: {result.rejected_df.count()}, Issues: {result.quality_issues_df.count()}")
            
            # 3. Write quality issues
            if result.quality_issues_df.count() > 0:
                logger.info(f"Writing quality issues for {table}")
                write_transactional_quality_issues(result.quality_issues_df)
            
            # 4. Clean data
            logger.info(f"Cleaning table: {table}")
            cleaned_df = clean_transactional_data(result.valid_df, table)
            
            # 5. Write to Silver (Minio)
            silver_path = f"s3a://{silver_bucket}/transactional/{table}/"
            logger.info(f"Writing cleaned data to: {silver_path}")
            cleaned_df.write.mode('overwrite').parquet(silver_path)
            logger.info(f"✅ Successfully processed table: {table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process table {table}: {e}")
            continue
    
    logger.info("=" * 50)
    logger.info("Silver Transactional Job completed!")
    spark.stop()

if __name__ == "__main__":
    main()
