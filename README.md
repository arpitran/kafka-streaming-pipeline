# Lakehouse + GraphQL side project

A PySpark lakehouse (bronze/silver/gold) with a Kafka + Spark Structured Streaming pipeline, and a GraphQL API on top.

## Plan
1. **ingest.py** (PySpark): NYC taxi trips (public parquet) -> bronze (raw) -> silver (typed, deduped, quality-checked) -> gold (daily/zone aggregates). Partitioned parquet.
2. **Airflow DAG** (`dags/lakehouse.py`): runs the three stages, with row-count and null-rate checks.
3. **api/** (Strawberry GraphQL): `trips`, `zoneStats` over gold tables (DuckDB reads the parquet).
4. **Federation**: split into two subgraphs (`zones`, `stats`) behind Apollo Router. This is the resume-relevant "federated graph" piece.
5. Dataproc: run ingest.py on a GCP Dataproc Serverless batch, note cost and timings in this README.

## Streaming stage (Kafka)
6. **stream/producer.py**: replays silver trips into a Kafka topic `trips` (key = trip id, with deliberate duplicates and late events).
7. **stream/consume.py** (Spark Structured Streaming): reads `trips`, applies watermark + `dropDuplicates` for idempotency, writes windowed counts to `lake/gold/zone_stream`. Checkpointed, so a killed job replays without double counting.
8. Document: replay procedure, late-event handling, freshness SLO.

## Setup (uv)
    brew install openjdk@17 kafka
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
    curl -o raw/yellow_tripdata_2024-01.parquet --create-dirs https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    uv sync
    uv run ingest.py                     # batch: bronze -> silver -> gold

    # streaming (broker: brew services start kafka)
    kafka-topics --create --topic trips --partitions 3 --bootstrap-server localhost:9092
    uv run stream/producer.py
    uv run stream/consume.py
