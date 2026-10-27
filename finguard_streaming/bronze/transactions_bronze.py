from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

@dp.table(
    name='finguard.bronze.transactions',
    comment='Transaction raw stream data ingested by kafka'
)

def transaction_bronze() ->DataFrame:
    kafka_connection_dtls = dbutils.secrets.get('finguard_scope','kafka_connection_dtls')
    kafka_config = json.loads(kafka_connection_dtls)
    bootstrap_server = kafka_config['bootstrap_servers']
    api_key = kafka_config['api_key']
    api_secret = kafka_config['api_secret']
    topic = kafka_config['topic']

    jaas_config=f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}"password="{api_secret}";'

    streamingDF = (spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers',bootstrap_server)
                .option('subscribe',topic)
                .option('kafka.security.protocol','SASL_SSL')
                .option('kafka.sasl.mechanism','PLAIN')
                .option('kafka.sasl.jaas.config',jaas_config)
                .option('startingOffsets','earliest')
                .load()
    )

    parsedstreamDF = streamingDF.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType"),
    F.current_timestamp().alias('ingestion_timestamp')
    )

    return parsedstreamDF

