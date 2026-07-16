import logging
import os
from pyspark.sql import SparkSession
from common.silver_transactional_bronze_reader import read_bronze_transactional_table
from common.silver_transactional_validation import validate_transactional_data
from common.silver_transactional_cleaning import clean_transactional_data
from common.silver_transactional_quality_writer import write_transactional_quality_issues

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Create Spark Session
    spark = SparkSession.builder \
        .appName("SilverTransactionalJob") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin")) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # List of tables to process
    tables = ['categories', 'users', 'products', 'orders', 'order_items', 'product_price_history']
    
    # Minio buckets
    silver_bucket = os.getenv("MINIO_BUCKET_SILVER", "silver")
    
    for table in tables:
        logger.info("=" * 60)
        logger.info(f"Processing table: {table}")
        
        try:
            # 1. Read from Bronze (Minio)
            logger.info(f"Reading Bronze data for: {table}")
            df = read_bronze_transactional_table(spark, table)
            
            if df.isEmpty():
                logger.warning(f"No data found for table: {table}")
                continue
                
            logger.info(f"Read {df.count()} records from Bronze for {table}")
            
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
            
            cleaned_df.write \
                .mode('overwrite') \
                .format('parquet') \
                .save(silver_path)
            
            logger.info(f"✅ Successfully processed table: {table}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process table {table}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            continue
    
    logger.info("=" * 60)
    logger.info("Silver Transactional Job completed!")
    spark.stop()

if __name__ == "__main__":
    main()