from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

@dp.table(
    name='finguard.silver.transactions',
    comment='Parsed and cleaned transaction data'
)

@dp.expect_or_drop('valid_transaction_id', 'transaction_id IS NOT NULL')
@dp.expect_or_drop('valid_customer_id', 'customer_id IS NOT NULL')
@dp.expect_or_drop('valid_card_number', 'card_number IS NOT NULL')
@dp.expect_or_drop('valid_merchant_id', 'merchant_id IS NOT NULL')
@dp.expect('valid_amount', 'amount > 0')


def transaction_bronze() -> DataFrame:
    bronzeDF = spark.readStream.table('finguard.bronze.transactions')
    
    schema = StructType([
        StructField('transaction_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('card_number', StringType(), True),
        StructField('merchant_id', StringType(), True),
        StructField('merchant_name', StringType(), True),
        StructField('merchant_category', StringType(), True),
        StructField('amount', DoubleType(), True),
        StructField('currency', StringType(), True),
        StructField('transaction_type', StringType(), True),
        StructField('payment_channel', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('city', StringType(), True),
        StructField('country', StringType(), True),
        StructField('transaction_timestamp', TimestampType(), True),
        StructField('is_international', BooleanType(), True),
        StructField('status', StringType(), True),
    ])

    transformedDF = bronzeDF.select(
        from_json(col('value'), schema).alias('data'),
        col('topic').alias('kafka_topic'),
        col('partition').alias('kafka_partition'),
        col('offset').alias('kafka_offset'),
        col('timestamp').alias('kafka_timestamp'),
        col('ingestion_timestamp').alias('bronze_ingestion_timestamp')
    ).select(
        col('data.*'),
        col('kafka_topic'),
        col('kafka_partition'),
        col('kafka_offset'),
        col('kafka_timestamp'),
        col('bronze_ingestion_timestamp'),
        current_timestamp().alias('silver_ingestion_timestamp')
    )

    return transformedDF