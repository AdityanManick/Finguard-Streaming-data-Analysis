from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name='finguard.gold.transaction_count_by_minute_sliding',
    comment='Number of transactions in a 5 minutes with sliding of 1 minute'
)
def transaction_count_by_minute_sliding() -> DataFrame:
    transactionDF = spark.readStream.table('finguard.silver.transactions')

    transaction_with_watermark = transactionDF.withWatermark('transaction_timestamp', '5 minutes')

    transaction_countDF = (transaction_with_watermark.groupBy(F.window('transaction_timestamp', '5 minutes', '1 minute')).
                         agg(F.count('*').alias('transaction_count')).select(
                             F.col('window.start').alias('window_start'),
                             F.col('window.end').alias('window_end'),
                             F.col('transaction_count')
                         )
    )
    return transaction_countDF