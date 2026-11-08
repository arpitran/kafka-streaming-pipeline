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

## Run it (Mac, about 10 minutes)
Copy each command into Terminal, one at a time, and press Enter. Wait for it to finish before the next.

**1. Install the tools (once).** You need [Homebrew](https://brew.sh) and [uv](https://docs.astral.sh/uv/getting-started/installation/). For Kafka, pick one: [Docker Desktop](https://www.docker.com/products/docker-desktop/) (option A in step 4) or Homebrew Kafka (option B).

    brew install openjdk@17
    brew install kafka    # only for option B

**2. Get the code and the data.**

    git clone https://github.com/arpitran/kafka-streaming-pipeline.git
    cd kafka-streaming-pipeline
    curl -o raw/yellow_tripdata_2024-01.parquet --create-dirs https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
    uv sync

The `curl` line downloads a 50 MB file of New York taxi rides.

**3. Prepare the rides.** Run the export line in every new Terminal window before steps 3 and 6, because it tells Spark where Java lives.

    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
    uv run ingest.py

It takes about a minute and creates a `lake/` folder. It prints a lot of text; that is normal. No red `Error` at the end means success.

**4. Start Kafka and make the topic** (a topic is a named mailbox for messages). Use one option, not both: each one listens on port 9092.

Option A, Docker:

    docker compose up -d
    sleep 10
    docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic trips --partitions 3 --if-not-exists --bootstrap-server localhost:9092

Option B, Homebrew:

    brew services start kafka
    sleep 10
    kafka-topics --create --topic trips --partitions 3 --if-not-exists --bootstrap-server localhost:9092

You should see `Created topic trips.`

**5. Send the rides into Kafka.**

    uv run stream/producer.py

It prints nothing and ends after a few seconds. That means it worked.

**6. Read the rides with Spark.** This one never stops by itself.

    uv run stream/consume.py

Wait 90 seconds, then press `Ctrl-C` to stop it.

**7. Check the answer.**

    uv run stream/check.py

You should see `4174 rows, 98664 trips`.

**When you are done:** option A: `docker compose stop` or `docker compose down` (data stays in a Docker volume; `docker compose down -v` deletes it). Option B: `brew services stop kafka`.

If something fails, the usual cause is a missing `export JAVA_HOME=...` line in that Terminal window, or Kafka not running yet.

## Development
Make shortcuts and how to add a Python library: [`docs/development.md`](docs/development.md).

## Learn Kafka
Step-by-step lessons, from basics to expert, live in [`lessons/`](lessons/README.md).

## Next
- Consumer groups and rebalancing demo
- Kafka Streams / Connect
- Exactly-once with transactions
