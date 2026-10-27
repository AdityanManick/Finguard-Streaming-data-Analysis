from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name='finguard.silver.customers'
    ,comment='Parsed and cleaned customer data'
)

@dp.expect('valid_customer_id','customer_id IS NOT NULL')

def customers_silver() ->DataFrame:
    bronzeDF = spark.readStream.table('finguard.bronze.customers')
    
    transformDF = bronzeDF.withColumn('account_open_date', F.to_date('account_open_date', 'yyyy-MM-dd')).withColumn('silver_ingestion_timestamp', F.current_timestamp())

    return transformDF
