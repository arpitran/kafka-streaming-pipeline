# Learn Kafka, from basics to expert

Hands-on lessons that run against the local Kafka in this repo. Each lesson has the idea, an experiment you run, the real output from a run, and check questions with answers.

## Before you start

1. Start Kafka (option A in the main [README](../README.md), step 4):

       docker compose up -d

2. Make the `orders` topic the lessons use:

       docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --if-not-exists --bootstrap-server localhost:9092

3. The two apps the lessons use:
   - [`apps/order_producer.py`](../apps/order_producer.py) - sends fake food orders, keyed by user.
   - [`apps/order_consumer.py`](../apps/order_consumer.py) - reads them as part of consumer group `order-tracker`.

### Using Confluent's Kafka image instead

Same broker, Confluent's packaging (`confluentinc/cp-kafka:8.0.0`, Kafka 4.0). Use it to match a Confluent tutorial. Only one broker can run at a time, because both use port 9092.

    docker compose stop
    docker compose --profile confluent up -d
    docker exec -it kafka-confluent kafka-topics --create --topic orders --partitions 3 --if-not-exists --bootstrap-server localhost:9092

In the lessons, replace `docker exec kafka /opt/kafka/bin/<tool>.sh` with `docker exec kafka-confluent <tool>`, for example `kafka-consumer-groups`. To go back: `docker compose stop` and then `docker compose up -d`. The two brokers keep separate data.

Open 3 or 4 Terminal windows in the repo folder. The lessons say which window runs what.

## Lessons

| # | Lesson | Status |
|---|---|---|
| 1 | [The log: topics, partitions, keys, consumer groups, offsets](01-the-log.md) | Done |
| 2 | Producers: `acks`, batching, retries, idempotence | Planned |
| 3 | Replication and durability: 3 brokers, ISR, `min.insync.replicas`, leader election | Planned |
| 4 | Consumers in depth: manual commits, rebalancing, lag | Planned |
| 5 | Delivery guarantees: at-least-once, at-most-once, exactly-once with transactions | Planned |
| 6 | Schemas: Avro or Protobuf, Schema Registry, compatibility rules | Planned |
| 7 | Kafka Connect and change data capture: the outbox pattern | Planned |
| 8 | Stream processing: Spark Structured Streaming, windows, watermarks (uses [`stream/`](../stream/)) | Planned |
| 9 | Operations: retention, compaction, partition sizing, monitoring | Planned |
| 10 | Production: managed Kafka (Confluent Cloud), security, quotas | Planned |

## Reading

- *Kafka: The Definitive Guide* (Narkhede, Shapira, Palino). The 1st edition (2017) still uses ZooKeeper; Kafka 4.0 runs on KRaft only. The concepts carry over.
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
