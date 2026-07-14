from pyspark.sql.functions import col, when

def transform_bronze_transactional(kafka_df, schema, table_name):
    return kafka_df.select(
        col('transaction_id'),
        col('user_id'),
        col('amount'),
        col('timestamp'),
        when(col('status').isNull(), 'UNKNOWN').otherwise(col('status')).alias('status')
    )
