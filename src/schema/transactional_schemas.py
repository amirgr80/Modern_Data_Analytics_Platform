from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

TRANSACTIONAL_SCHEMAS = {
    'transactions': StructType([
        StructField('transaction_id', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('amount', IntegerType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('status', StringType(), True)
    ])
}
