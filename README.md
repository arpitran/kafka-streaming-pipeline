# Kafka streaming pipeline

Kafka -> Spark Structured Streaming pipeline on NYC taxi trips: keyed producer, deliberate duplicates, watermark + dedupe, checkpointed restarts.

## What it shows
- **Producer** (`stream/producer.py`): replays 100k January 2024 trips into topic `trips` (3 partitions, key = trip id) and re-sends ~2% as duplicates.
- **Consumer** (`stream/consume.py`): Spark Structured Streaming reads the topic, applies a 1-hour watermark and `dropDuplicates`, and writes hourly per-zone counts to parquet with a checkpoint.
- **Data prep** (`ingest.py`): one-off PySpark job that builds the cleaned "silver" trips the producer replays.

## Verified
- Duplicates are not double counted.
- Kill the consumer mid-run, restart it: output is identical to an uninterrupted run (4,174 rows, 98,664 trips both times).
- The 1,336 trips not in the output sit in the last open watermark window; append mode has not emitted it yet.

## Run (uv)
    brew install openjdk@17 kafka
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
    curl -o raw/yellow_tripdata_2024-01.parquet --create-dirs https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    uv sync
    uv run ingest.py                     # builds lake/silver

    brew services start kafka
    kafka-topics --create --topic trips --partitions 3 --bootstrap-server localhost:9092
    uv run stream/producer.py
    uv run stream/consume.py

## Next
- Consumer groups and rebalancing demo
- Kafka Streams / Connect
- Exactly-once with transactions
