# AGENTS.md

Context for AI coding agents working in this repo. Humans: start with [README.md](README.md).

## What this repo is

Two things in one repo:

1. **A finished pipeline** (`stream/`, `ingest.py`): NYC taxi trips replayed into Kafka, read by Spark Structured Streaming with a watermark and dedupe. The README "Run it" section walks through it.
2. **A hands-on Kafka course** (`lessons/`, `apps/`): the owner is learning Kafka from basics to expert, one lesson at a time. The lessons are written so others can learn from them too. This is the active work.

## Layout

| Path | What it is |
|---|---|
| `ingest.py` | One-off PySpark job: `raw/` taxi parquet to cleaned trips in `lake/` |
| `stream/producer.py` | Replays trips into topic `trips` (key = trip id, ~2% deliberate duplicates) |
| `stream/consume.py` | Spark readStream from Kafka, 1-hour watermark, `dropDuplicates`, writes parquet with a checkpoint |
| `stream/check.py` | Verifies output: expect `4174 rows, 98664 trips` |
| `apps/order_producer.py` | Lesson app: fake food orders to topic `orders`, key = user |
| `apps/order_consumer.py` | Lesson app: reads `orders` as consumer group `order-tracker` |
| `lessons/` | The course. `README.md` is the index and roadmap; one file per lesson |
| `docs/development.md` | Make commands, adding a Python library |
| `docker-compose.yaml` | Two single-node KRaft brokers as profiles: `apache` (default) and `confluent` |
| `Makefile` | Shortcuts: `make up`, `make topics`, `make group`, ... Run `make` to list |

`raw/`, `lake/` and `.venv/` are git-ignored and generated locally.

## Stack

- Python via **uv** only: `uv run <script>`, `uv add <lib>`. Never `pip install`.
- Kafka **4.0, KRaft only** (no ZooKeeper). Images: `apache/kafka:4.0.0` and `confluentinc/cp-kafka:8.0.0`.
- Client libraries: `kafka-python` (used by `apps/` and `stream/producer.py`) and `confluent-kafka` (installed, for later lessons: transactions, Schema Registry). Not yet used in code.
- `pyspark==4.3.0` is pinned on purpose: it must match the `spark-sql-kafka` connector version in `stream/consume.py`.
- Spark needs Java 17: `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`.

## Kafka setup facts (verified, easy to get wrong)

- Only one broker runs at a time: both bind `localhost:9092`. Homebrew Kafka (README option B) also uses 9092, so stop it first: `brew services stop kafka`.
- CLI names differ by image: Apache `docker exec kafka /opt/kafka/bin/kafka-topics.sh`, Confluent `docker exec kafka-confluent kafka-topics`. The Makefile handles this via `BROKER=confluent`.
- Data lives in named volumes at `/var/lib/kafka/data`. It survives `docker compose down`; `down -v` or `make clean` deletes it. Do not mount a new path: it would be root-owned and Kafka (user `appuser`) fails with `AccessDeniedException`.
- The Apache image drops its built-in config once any `KAFKA_*` env var is set, so both services share the full env block (`x-kafka-env` anchor).
- The broker needs a few seconds after start. Use `make up`, which waits until the broker answers, instead of `sleep`.
- Topic `orders` has 3 partitions. With the 4 users in `order_producer.py`, partition 2 gets no keys, so a second consumer can sit idle on it. This is expected and used in Lesson 1.

## Lessons: how to continue the course

- Progress is the Status column in `lessons/README.md`. Pick up at the first `Planned` lesson.
- Teach hands-on: explain the idea briefly, have the owner run an experiment, predict the result, then explain what the real output shows.
- Write each lesson as `lessons/NN-short-name.md`, following `lessons/01-the-log.md`: goal, the idea, experiments with commands and **real output from an actual run** (never invented output), check questions with answers in a `<details>` block, common mistakes. Then mark it `Done` in the index.
- Lesson 1 left a check question for the owner (4 partitions, 6 consumers); the answer is in the file.

## Conventions

- Writing: no em dashes anywhere (docs, comments, commit messages). Use a hyphen or rewrite the sentence.
- Commits: one-line message, no body, no AI attribution trailers (no `Co-Authored-By`). Commit or push only when the owner asks. The repo is public on GitHub, so never commit secrets or personal data.
- Keep code small and readable: these files are teaching material.
- When docs change, keep `docs/development.md` in sync with the `Makefile`.
