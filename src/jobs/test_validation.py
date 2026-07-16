import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from common.silver_transactional_validation import validate_transactional_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder.appName("TestSilverValidation").getOrCreate()
    
    sample_data = [
        ("C001", "Electronics", None),
        ("C002", "Books", "C001"),
        ("C003", "", "C001"),  
        (None, "Invalid", "C001"),  
        ("C004", "Clothing", "C999"), 
    ]
    
    schema = StructType([
        StructField("category_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("parent_category_id", StringType(), True),
    ])
    
    df = spark.createDataFrame(sample_data, schema)
    
    logger.info("Sample data created:")
    df.show(truncate=False)
    
    result = validate_transactional_data(df, "categories")
    
    logger.info("=" * 50)
    logger.info(f"Valid count: {result.valid_df.count()}")
    result.valid_df.show(truncate=False)
    
    logger.info("=" * 50)
    logger.info(f"Rejected count: {result.rejected_df.count()}")
    result.rejected_df.show(truncate=False)
    
    logger.info("=" * 50)
    logger.info(f"Quality issues count: {result.quality_issues_df.count()}")
    result.quality_issues_df.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()