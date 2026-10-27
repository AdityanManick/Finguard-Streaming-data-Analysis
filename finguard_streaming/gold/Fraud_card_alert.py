from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name='finguard.gold.fraud_card_alert',
    comment='High value transactions alert'
)
def fraud_card_alert() -> DataFrame:
    transactionDF = spark.readStream.table('finguard.silver.transactions')
    fraudwatchlistDF = spark.readStream.table('finguard.silver.fraud_watchlist')

    customerDF = spark.read.table('finguard.silver.customers')
    
    transaction_with_watermark = transactionDF.withWatermark('transaction_timestamp', '5 minutes')
    fraudwatchlist_with_watermark = fraudwatchlistDF.withWatermark('effective_from', '5 minutes')
    
    fraud_detucted = (
        transaction_with_watermark
        .join(fraudwatchlist_with_watermark,
                  transaction_with_watermark.card_number == fraudwatchlistDF.entity_id, 'inner'
                  ).join(
                      customerDF, transaction_with_watermark.customer_id == customerDF.customer_id, 'left'
                  ).select(
                      F.concat_ws('-',F.lit('Fraud'),F.col('transaction_id'),F.col('watchlist_id')).alias('alert_id'),
                      F.lit('FRAUD_WATCHLIST_MATCH').alias('alert_type'),
                      F.current_timestamp().alias('alert_timestamp'),

                      transaction_with_watermark.transaction_id,
                      transaction_with_watermark.customer_id,
                      customerDF.email.alias('customer_email'),
                      F.concat_ws(' ',customerDF.first_name, customerDF.last_name).alias('customer_name'),
                      transaction_with_watermark.card_number,
                      transaction_with_watermark.amount,
                      transaction_with_watermark.currency,
                      transaction_with_watermark.merchant_id,
                      transaction_with_watermark.merchant_name,
                      transaction_with_watermark.merchant_category,
                      transaction_with_watermark.transaction_type,
                      transaction_with_watermark.payment_channel,
                      transaction_with_watermark.device_id,
                      transaction_with_watermark.city.alias('transaction_city'),
                      transaction_with_watermark.country.alias('transaction_country'),
                      transaction_with_watermark.transaction_timestamp,
                      transaction_with_watermark.is_international,
                      transaction_with_watermark.status.alias('transaction_status'),

                      F.col('watchlist_id'),
                      F.col('watch_type'),
                      F.col('risk_level'),
                      F.col('action'),
                      F.col('reason_code'),
                      F.col('reason_description'),
                      F.col('effective_from').alias('watchlist_effective_from'),
                      F.col('reported_by'),
                      F.col('reported_source'),
                      fraudwatchlist_with_watermark.city.alias('watchlist_city'),
                      fraudwatchlist_with_watermark.country.alias('watchlist_country')
                  )
    )

    return fraud_detucted
