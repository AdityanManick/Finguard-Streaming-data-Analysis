#Overview
Financial institutions need to spot suspicious transactions as they happen, not hours later in a batch report. Finguard simulates that scenario end to end: a Confluent data generator produces a continuous stream of transaction events, which flow through Bronze, Silver and Gold Delta layers. In the Gold layer, windowed aggregations with watermarks identify high-alert transactions and publish them to a fraud watchlist.

#Key highlights

* Simulated real-time transaction stream produced with a Confluent data generator and Apache Kafka
* Incremental, file-based ingestion with Databricks Auto Loader [confirm how Kafka data reaches Auto Loader, e.g. Kafka -> cloud storage -> Auto Loader]
* Medallion architecture (Bronze, Silver, Gold) on Delta Lake
* Spark Structured Streaming with watermarks (late-data handling) and time windows (aggregation over fixed intervals)
* A high-alert fraud watchlist as the Gold-layer output

##What I Learned

1. How watermarks bound state and handle late-arriving events in streaming aggregations
2. Designing incremental Bronze, Silver and Gold layers on Delta Lake

##Future Improvements

1. Add automated tests and CI/CD
2. Add monitoring for data freshness and stream health
