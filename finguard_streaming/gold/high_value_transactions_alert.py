from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name='finguard.gold.high_value_transactions_alert',
    comment='High value transactions alert'
)
def high_value_transactions_alert() -> DataFrame:
    transactionDF = spark.readStream.table('finguard.silver.transactions')
    customerDF = spark.read.table('finguard.silver.customers')
    
    joinedDF = transactionDF.join(customerDF, transactionDF.customer_id == customerDF.customer_id, 'left').filter(F.col("amount") > F.col('transaction_limit')).select(
        F.concat_ws("-", F.lit('ALERT'), F.col('transaction_id')).alias('alert_id'),
        F.lit('High_Value_Alert').alias('alert_type'),
        F.current_timestamp().alias('alert_timestamp'),

        transactionDF.transaction_id,
        transactionDF.customer_id,
        customerDF.email.alias('customer_email'),
        F.concat_ws(' ', F.col('first_name'), F.col('last_name')).alias('customer_name'),
        transactionDF.amount.alias('transaction_amount'),
        customerDF.transaction_limit,
        transactionDF.currency,
        transactionDF.merchant_name,
        transactionDF.merchant_category,
        transactionDF.transaction_type,
        transactionDF.payment_channel,
        transactionDF.city,
        transactionDF.country,
        transactionDF.is_international,
        transactionDF.transaction_timestamp,
        transactionDF.status
    )
    
    return joinedDF